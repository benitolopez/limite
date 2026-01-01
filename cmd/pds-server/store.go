// store.go implements the sharded in-memory key-value store and its binary
// persistence format. This file is one half of the PDS Persistence Layer; the
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
// The Binary Format (PDS1)
// ========================
//
// Snapshots use a custom binary format optimized for raw loading speed. Unlike
// generic formats (JSON, Gob), PDS1 avoids reflection and can restore millions
// of keys per second by inserting directly into the destination shard.
//
// File Structure:
//
//	+--------+-----------+-----------+---------+     +-----+-----------+
//	| Header | Shard 0   | Shard 1   | Shard 2 | ... | EOF | Checksum  |
//	+--------+-----------+-----------+---------+     +-----+-----------+
//	 4 bytes   variable    variable    variable       1 B    8 bytes
//
// Header: A 4-byte magic string "PDS1" for format identification.
//
// Shard Blocks: Each non-empty shard is written as a block:
//
//	+--------+----------+-------+-------+-------+-------+-------+-------+
//	| OpCode | Shard ID | Count | KLen  | Key   | VLen  | Value | ...   |
//	+--------+----------+-------+-------+-------+-------+-------+-------+
//	  1 byte   1 byte    4 bytes 4 bytes  var    4 bytes  var
//
//	OpCode:   0xFE indicates a shard block follows.
//	Shard ID: The index (0-255) for direct array placement on load.
//	Count:    Number of key-value pairs in this block.
//	KLen/VLen: Little-endian uint32 length prefixes.
//	Key/Value: Raw bytes.
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
// However, since PDS1 explicitly stores the shard ID with each block, the
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
)

const persistenceMagic = "PDS1"

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
	mu   sync.RWMutex
	data map[string][]byte
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
			data: make(map[string][]byte),
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

// Set adds a key-value pair to the correct shard.
func (s *Store) Set(key string, value []byte) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.data[key] = value
}

// Get retrieves a value from the correct shard.
func (s *Store) Get(key string) ([]byte, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	val, ok := shard.data[key]
	return val, ok
}

// Delete removes a key from the correct shard.
func (s *Store) Delete(key string) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	_, ok := shard.data[key]
	if ok {
		delete(shard.data, key)
	}
	return ok
}

// View executes a read-only callback while holding the shard's read lock.
// The callback receives the raw bytes (or nil if key doesn't exist).
// This enables zero-copy reads for performance-critical paths.
func (s *Store) View(key string, fn func(data []byte) error) error {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	return fn(shard.data[key]) // nil if not exists
}

// Mutate atomically reads, modifies, and updates a value using a callback.
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

	currentValue := shard.data[key]
	newValue, changed := fn(currentValue)

	if changed {
		shard.data[key] = newValue
	}
}

// SaveSnapshotToWriter serializes the entire in-memory state to an io.Writer
// in the PDS1 binary format. This is the core persistence primitive used by
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
// containing PDS1 binary data. This method is designed specifically for Hybrid
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
	// However, the PDS1 format explicitly stores the shard ID with each block.
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
	keyScratchBuf := make([]byte, 256) // Reuse buffer to reduce allocs

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

			// Direct Insertion (Zero-Rehash)
			shard.data[key] = valBuf
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
