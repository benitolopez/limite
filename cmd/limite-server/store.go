// store.go implements the sharded in-memory key-value store and its binary
// persistence format. This file is one half of the Limite Persistence Layer; the
// other half (persistence.go) orchestrates the interaction between this store
// and the append-only log.
//
// The Store is intentionally decoupled from the filesystem. All persistence
// methods operate on generic io.Writer and io.Reader interfaces, allowing
// data to be streamed to files, buffers, or network sockets without the store
// knowing (or caring) about the destination. This separation enables testing
// with in-memory buffers and makes the snapshot logic reusable for replication.
//
// Sharding Strategy
// =================
//
// The store partitions data across 256 independent shards, each with its own
// mutex. This design reduces lock contention compared to a single global lock:
// two concurrent writes to different keys will typically hit different shards
// and proceed in parallel. The shard count of 256 is a sweet spot—enough to
// virtually eliminate contention at typical workloads, but small enough to
// iterate quickly during snapshots.
//
// Keys are assigned to shards using FNV-1a hashing modulo 256. FNV-1a was
// chosen for its speed and reasonable distribution properties; cryptographic
// strength is not needed here.
//
// The Binary Format (LIM1)
// ========================
//
// Snapshots use a custom binary format optimized for raw loading speed. Unlike
// generic formats (JSON, Gob), LIM1 avoids reflection and can restore millions
// of keys per second by inserting directly into the destination shard.
//
// File Structure:
//
//	+--------+-----------+-----------+---------+     +-----+-----------+
//	| Header | Shard 0   | Shard 1   | Shard 2 | ... | EOF | Checksum  |
//	+--------+-----------+-----------+---------+     +-----+-----------+
//	 4 bytes   variable    variable    variable       1 B    8 bytes
//
// Header: A 4-byte magic string "LIM1" for format identification.
//
// Shard Blocks: Each non-empty shard is written as a block:
//
//	+--------+----------+-------+-------+-------+--------+-------+-------+-------+
//	| OpCode | Shard ID | Count | KLen  | Key   | Expiry | VLen  | Value | ...   |
//	+--------+----------+-------+-------+-------+--------+-------+-------+-------+
//	  1 byte   1 byte    4 bytes 4 bytes  var    8 bytes  4 bytes  var
//
//	OpCode:   0xFE indicates a shard block follows.
//	Shard ID: The index (0-255) for direct array placement on load.
//	Count:    Number of key-value pairs in this block.
//	KLen/VLen: Little-endian uint32 length prefixes.
//	Key/Value: Raw bytes.
//	Expiry:   Unix milliseconds timestamp (0 = no expiry).
//
// EOF Marker: A single byte 0xFF signals the end of binary data. This is
// critical for Hybrid AOF files where text commands follow the binary section.
//
// Checksum: A 64-bit CRC (ISO polynomial) computed over all preceding bytes
// (Header + Shard Blocks + EOF). This detects corruption from partial writes
// or disk errors.
//
// Zero-Rehash Loading
// ===================
//
// Normally, inserting a key requires hashing it to find the correct shard.
// However, since LIM1 explicitly stores the shard ID with each block, the
// loader can bypass hashing entirely and insert directly into the destination
// shard. This "zero-rehash" optimization significantly speeds up startup times
// for large datasets.
//
// Clone-then-Write Snapshots
// ==========================
//
// To avoid "Stop-the-World" pauses during persistence, SaveSnapshotToWriter
// uses a "clone-then-write" strategy. It iterates through shards sequentially,
// acquiring a read lock only long enough to copy data into a RAM buffer. The
// lock is released before the slow I/O operation begins. This keeps the server
// responsive during snapshots—write latency impact is typically under 10ms per
// shard.

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"hash/fnv"
	"io"
	"sync"
	"time"
)

const persistenceMagic = "LIM1"

// shardCount determines how many independent maps we maintain.
// 256 is a sweet spot: enough to reduce contention significantly,
// but small enough to iterate quickly during snapshots.
const shardCount = 256

// Opcodes for the binary snapshot format.
// These markers allow us to parse the binary stream without relying on
// file size or EOF, which is critical for Hybrid AOF (where text follows binary).
const (
	OpCodeShardData = 0xFE
	OpCodeEOF       = 0xFF
)

// Shard represents a single slice of the data store.
// It has its own lock, meaning locking this shard does NOT block others.
type Shard struct {
	mu      sync.RWMutex
	data    map[string][]byte
	expires map[string]int64 // key -> Unix milliseconds expiration time (0 = no expiry)
}

// Store holds the array of shards.
// It acts as the router, directing keys to the correct shard.
type Store struct {
	shards [shardCount]*Shard
}

// NewStore creates and initializes the sharded store.
func NewStore() *Store {
	s := &Store{}
	for i := 0; i < shardCount; i++ {
		s.shards[i] = &Shard{
			data:    make(map[string][]byte),
			expires: make(map[string]int64),
		}
	}

	return s
}

// getShardIndex computes the FNV-1a hash of the key and modulo it
// by the shard count to find the correct bucket.
func (s *Store) getShardIndex(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % shardCount)
}

// getShard returns the specific pointer to the Shard responsible for the key.
func (s *Store) getShard(key string) *Shard {
	return s.shards[s.getShardIndex(key)]
}

// isExpired checks if a key has expired. Safe to call with RLock held.
func (sh *Shard) isExpired(key string, now int64) bool {
	exp, ok := sh.expires[key]
	return ok && exp > 0 && exp <= now
}

// deleteIfExpired checks if a key has expired and deletes it if so.
// Must be called with exclusive (write) lock held.
// Returns true if the key was expired and deleted.
func (sh *Shard) deleteIfExpired(key string, now int64) bool {
	if sh.isExpired(key, now) {
		delete(sh.data, key)
		delete(sh.expires, key)
		return true
	}
	return false
}

// Set adds a key-value pair to the correct shard.
// Setting a key clears any existing expiration.
func (s *Store) Set(key string, value []byte) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.data[key] = value
	delete(shard.expires, key) // Clear any existing expiry
}

// Get retrieves a value from the correct shard.
// Returns nil, false if the key doesn't exist or has expired.
func (s *Store) Get(key string) ([]byte, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// Check if expired (lazy expiration check)
	if shard.isExpired(key, time.Now().UnixMilli()) {
		return nil, false
	}

	val, ok := shard.data[key]
	return val, ok
}

// Delete removes a key from the correct shard.
// Returns true if the key existed and was deleted.
func (s *Store) Delete(key string) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Clean up expired key first (so we don't report it as deleted)
	now := time.Now().UnixMilli()
	if shard.isExpired(key, now) {
		delete(shard.data, key)
		delete(shard.expires, key)
		return false // Key was already logically deleted
	}

	_, ok := shard.data[key]
	if ok {
		delete(shard.data, key)
		delete(shard.expires, key)
	}
	return ok
}

// View executes a read-only callback while holding the shard's read lock.
// The callback receives the raw bytes (or nil if key doesn't exist or expired).
// This enables zero-copy reads for performance-critical paths.
func (s *Store) View(key string, fn func(data []byte) error) error {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// Check if expired (lazy expiration check)
	if shard.isExpired(key, time.Now().UnixMilli()) {
		return fn(nil)
	}

	return fn(shard.data[key]) // nil if not exists
}

// Mutate atomically reads, modifies, and updates a value using a callback.
// If the key has expired, the callback receives nil (as if key doesn't exist).
func (s *Store) Mutate(key string, fn func([]byte) ([]byte, bool)) {
	//
	// DESIGN
	// ------
	//
	// This method implements a Read-Modify-Write (RMW) primitive to prevent the
	// "Lost Update" problem. Without this, concurrent operations on the same key
	// would need to acquire the lock, read the value, release the lock, compute
	// the new value, acquire the lock again, and write. In that gap, another
	// goroutine could have modified the value, and our write would silently
	// overwrite its changes.
	//
	// By passing a callback function, the caller performs all logic while holding
	// the exclusive lock. The callback receives the current value (or nil if the
	// key doesn't exist) and returns the new value along with a boolean indicating
	// whether the store should be updated. This allows the caller to abort the
	// write (e.g., on a type mismatch error) without modifying the data.
	//

	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Delete expired key (lazy cleanup with write lock)
	shard.deleteIfExpired(key, time.Now().UnixMilli())

	currentValue := shard.data[key]
	newValue, changed := fn(currentValue)

	if changed {
		shard.data[key] = newValue
		delete(shard.expires, key) // New value clears expiry
	}
}

// SetExpiry sets the expiration time for a key.
// Returns false if the key does not exist.
// Pass 0 to remove expiration (like PERSIST).
func (s *Store) SetExpiry(key string, timestampMs int64) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if key exists (and clean up if expired)
	shard.deleteIfExpired(key, time.Now().UnixMilli())

	if _, exists := shard.data[key]; !exists {
		return false
	}

	if timestampMs <= 0 {
		delete(shard.expires, key) // Remove expiry (PERSIST)
	} else {
		shard.expires[key] = timestampMs
	}
	return true
}

// GetExpiry returns the expiration time and existence status for a key.
// Returns:
//   - (expiry, true) if key exists with expiry
//   - (-1, true) if key exists but has no expiry
//   - (0, false) if key doesn't exist
func (s *Store) GetExpiry(key string) (int64, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// Check if expired
	if shard.isExpired(key, time.Now().UnixMilli()) {
		return 0, false
	}

	// Check if key exists
	if _, exists := shard.data[key]; !exists {
		return 0, false
	}

	// Return expiry or -1 if no expiry set
	if exp, hasExpiry := shard.expires[key]; hasExpiry {
		return exp, true
	}
	return -1, true
}

// Exists checks if a key exists and is not expired.
func (s *Store) Exists(key string) bool {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if shard.isExpired(key, time.Now().UnixMilli()) {
		return false
	}

	_, exists := shard.data[key]
	return exists
}

// Active expiration constants (Redis-style adaptive algorithm)
const (
	expiryKeysPerLoop     = 20 // Sample size per shard
	expiryAcceptableStale = 10 // Continue if >10% of sampled keys are expired
	expiryMaxIterations   = 16 // Check time limit every N iterations
	expiryTimeLimitMs     = 25 // Max milliseconds per cycle
)

// DeleteExpiredKeys performs active expiration using Redis-style adaptive sampling.
// It samples keys from each shard and removes expired ones, repeating if the
// stale percentage is high. Returns the number of keys deleted.
func (s *Store) DeleteExpiredKeys() int {
	//
	// DESIGN
	// ------
	//
	// This implements Redis's active expiration algorithm from expire.c. The naive
	// approach would scan all keys looking for expired ones, but that's O(n) and
	// would block the server. Instead, we use probabilistic sampling:
	//
	// For each shard:
	//   1. Sample up to 20 random keys from the expires map
	//   2. Delete any that have expired
	//   3. If >10% of sampled keys were expired, repeat (the shard is "hot")
	//   4. If <=10% expired, move to the next shard (this one is clean enough)
	//
	// Go's map iteration order is randomized, giving us free random sampling.
	// This adaptive approach focuses CPU time on shards with many expired keys
	// while quickly skipping clean shards.
	//
	// We also enforce a 25ms time budget per cycle. At 100ms intervals (10 Hz),
	// this caps active expiration at 25% of one CPU core. The time check happens
	// every 16 iterations to avoid the overhead of calling time.Now() too often.
	//
	start := time.Now()
	deleted := 0
	now := time.Now().UnixMilli()

	for _, shard := range s.shards {
		iteration := 0
		for {
			iteration++
			sampled, expired := s.sampleAndExpire(shard, now)
			deleted += expired

			// Stop if no keys to sample or stale percentage is acceptable
			if sampled == 0 || (expired*100/sampled) <= expiryAcceptableStale {
				break
			}

			// Check time limit every N iterations to bound CPU usage
			if iteration%expiryMaxIterations == 0 {
				if time.Since(start).Milliseconds() > expiryTimeLimitMs {
					return deleted
				}
			}
		}
	}
	return deleted
}

// sampleAndExpire samples up to expiryKeysPerLoop keys from a shard's expires map
// and deletes any that have expired. Returns (sampled, expired) counts.
func (s *Store) sampleAndExpire(shard *Shard, now int64) (int, int) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	sampled := 0
	expired := 0

	// Go map iteration is random, which provides natural sampling
	for key, exp := range shard.expires {
		if sampled >= expiryKeysPerLoop {
			break
		}
		sampled++

		if exp > 0 && exp <= now {
			delete(shard.data, key)
			delete(shard.expires, key)
			expired++
		}
	}

	return sampled, expired
}

// SaveSnapshotToWriter serializes the entire in-memory state to an io.Writer
// in the LIM1 binary format. This is the core persistence primitive used by
// both standalone snapshots and the Hybrid AOF compaction process.
//
// The caller receives no indication of progress; for large datasets, consider
// wrapping the writer with a progress reporter if user feedback is needed.
func (s *Store) SaveSnapshotToWriter(w io.Writer) error {
	//
	// DESIGN
	// ------
	//
	// This method implements the "Clone-then-Write" strategy to minimize the
	// impact on server responsiveness during persistence. The naive approach
	// would lock the entire store for the duration of the disk write, causing
	// latency spikes for all clients. Instead, we exploit the sharded architecture:
	//
	// For each shard (0 to 255):
	//   - Acquire a read lock on just that shard.
	//   - Copy all key-value pairs to a RAM buffer (fast, memory-bound).
	//   - Release the lock immediately.
	//   - Write the RAM buffer to disk (slow, I/O-bound, but no locks held).
	//
	// This means the server remains fully responsive on 255 out of 256 shards
	// at any given moment. The brief read-lock on the active shard allows
	// concurrent reads but blocks writes to that specific shard—typically for
	// just a few milliseconds.
	//
	// The output stream is wrapped in a MultiWriter that simultaneously feeds
	// data to both the destination and a CRC64 hasher. This avoids a second
	// pass over the data to calculate the checksum.
	//
	crcTable := crc64.MakeTable(crc64.ISO)
	checksumCalculator := crc64.New(crcTable)

	// We wrap the writer so every byte written is also hashed.
	multiWriter := io.MultiWriter(w, checksumCalculator)
	bw := bufio.NewWriter(multiWriter)

	if _, err := bw.WriteString(persistenceMagic); err != nil {
		return err
	}

	// We reuse these buffers across shards to reduce GC pressure.
	shardBuf := new(bytes.Buffer)
	lenBuf := make([]byte, 4)
	expiryBuf := make([]byte, 8)

	for i := 0; i < shardCount; i++ {
		shard := s.shards[i]

		// Critical Section: Fast Memory Copy
		shard.mu.RLock()
		count := len(shard.data)
		if count == 0 {
			shard.mu.RUnlock()
			continue
		}

		shardBuf.Reset()

		// Write Block Header: OpCode + ShardID
		shardBuf.WriteByte(OpCodeShardData)
		shardBuf.WriteByte(byte(i))

		binary.LittleEndian.PutUint32(lenBuf, uint32(count))
		shardBuf.Write(lenBuf)

		for k, v := range shard.data {
			// Key
			binary.LittleEndian.PutUint32(lenBuf, uint32(len(k)))
			shardBuf.Write(lenBuf)
			shardBuf.WriteString(k)
			// Expiry (0 if no expiry)
			expiry := shard.expires[k] // 0 if not in map
			binary.LittleEndian.PutUint64(expiryBuf, uint64(expiry))
			shardBuf.Write(expiryBuf)
			// Value
			binary.LittleEndian.PutUint32(lenBuf, uint32(len(v)))
			shardBuf.Write(lenBuf)
			shardBuf.Write(v)
		}
		shard.mu.RUnlock()

		// IO Section: Write to Output
		if _, err := shardBuf.WriteTo(bw); err != nil {
			return err
		}
	}

	// Write EOF Marker
	// This tells the reader "The binary data ends here".
	if err := bw.WriteByte(OpCodeEOF); err != nil {
		return err
	}

	// Flush the buffer to ensure all data (including EOF) is in the hasher.
	if err := bw.Flush(); err != nil {
		return err
	}

	// Write Checksum
	// We write this directly to the underlying writer because we don't want
	// to hash the checksum itself.
	if err := binary.Write(w, binary.LittleEndian, checksumCalculator.Sum64()); err != nil {
		return err
	}

	return nil
}

// LoadSnapshotFromReader restores the in-memory state from a buffered reader
// containing LIM1 binary data. This method is designed specifically for Hybrid
// AOF loading: it consumes exactly the binary preamble (header + shard blocks +
// EOF marker + checksum) and stops, leaving the reader positioned at the first
// byte of any subsequent text data.
//
// The caller must provide a buffered reader; this method uses Peek and ReadByte
// operations that require buffering support.
func (s *Store) LoadSnapshotFromReader(r *bufio.Reader) error {
	//
	// DESIGN
	// ------
	//
	// This loader implements the "Zero-Rehash" optimization. Normally, inserting
	// a key requires computing hash(key) % 256 to find the destination shard.
	// However, the LIM1 format explicitly stores the shard ID with each block.
	// We trust this ID and insert directly into s.shards[shardID], bypassing the
	// hash function entirely.
	//
	// This is safe because the snapshot was written by SaveSnapshotToWriter,
	// which placed each key in the correct shard. The only risk is file corruption,
	// which we detect via the CRC64 checksum at the end.
	//
	// The reader is consumed byte-by-byte, with each byte also fed to the CRC
	// hasher. When we hit the EOF marker (0xFF), we stop reading shard blocks,
	// consume the 8-byte checksum, and verify. If successful, the reader is now
	// positioned exactly at the start of any text tail—ready for RESP parsing.
	//
	header := make([]byte, len(persistenceMagic))
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}
	if string(header) != persistenceMagic {
		return errors.New("invalid snapshot header")
	}

	crcTable := crc64.MakeTable(crc64.ISO)
	hasher := crc64.New(crcTable)
	hasher.Write(header)

	lenBuf := make([]byte, 4)
	expiryBuf := make([]byte, 8)
	keyScratchBuf := make([]byte, 256) // Reuse buffer to reduce allocs
	now := time.Now().UnixMilli()

	for {
		opcodeByte, err := r.ReadByte()
		if err != nil {
			return err
		}
		hasher.Write([]byte{opcodeByte})

		// Stop condition: End of Binary Section
		if opcodeByte == OpCodeEOF {
			break
		}

		if opcodeByte != OpCodeShardData {
			return fmt.Errorf("snapshot stream corruption: unexpected opcode %x", opcodeByte)
		}

		// Read Shard ID
		shardIDByte, err := r.ReadByte()
		if err != nil {
			return err
		}
		hasher.Write([]byte{shardIDByte})

		shard := s.shards[int(shardIDByte)]

		// Read Key Count
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return err
		}
		hasher.Write(lenBuf)
		count := binary.LittleEndian.Uint32(lenBuf)

		// Read Keys
		for i := uint32(0); i < count; i++ {
			// Key Length
			if _, err := io.ReadFull(r, lenBuf); err != nil {
				return err
			}
			hasher.Write(lenBuf)
			kLen := binary.LittleEndian.Uint32(lenBuf)

			// Key Data
			if uint32(cap(keyScratchBuf)) < kLen {
				keyScratchBuf = make([]byte, kLen)
			}
			keySlice := keyScratchBuf[:kLen]

			if _, err := io.ReadFull(r, keySlice); err != nil {
				return err
			}
			hasher.Write(keySlice)
			key := string(keySlice)

			// Expiry (8 bytes)
			if _, err := io.ReadFull(r, expiryBuf); err != nil {
				return err
			}
			hasher.Write(expiryBuf)
			expiry := int64(binary.LittleEndian.Uint64(expiryBuf))

			// Value Length
			if _, err := io.ReadFull(r, lenBuf); err != nil {
				return err
			}
			hasher.Write(lenBuf)
			vLen := binary.LittleEndian.Uint32(lenBuf)

			// Value Data
			valBuf := make([]byte, vLen)
			if _, err := io.ReadFull(r, valBuf); err != nil {
				return err
			}
			hasher.Write(valBuf)

			// Skip expired keys (don't insert into store)
			if expiry > 0 && expiry <= now {
				continue
			}

			// Direct Insertion (Zero-Rehash)
			shard.data[key] = valBuf
			if expiry > 0 {
				shard.expires[key] = expiry
			}
		}
	}

	// Verify Checksum
	// We read the 8-byte checksum from the stream and compare it against
	// what we calculated during the read.
	storedChecksumBytes := make([]byte, 8)
	if _, err := io.ReadFull(r, storedChecksumBytes); err != nil {
		return err
	}

	storedChecksum := binary.LittleEndian.Uint64(storedChecksumBytes)
	calculatedChecksum := hasher.Sum64()

	if storedChecksum != calculatedChecksum {
		return errors.New("snapshot corruption: checksum mismatch")
	}

	return nil
}
