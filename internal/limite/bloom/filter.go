// Package bloom implements a high-performance Hybrid Scalable Blocked Bloom Filter.
//
// A Bloom filter is a probabilistic data structure that allows checking if an
// element is *definitely not* in a set or *probably* in a set. It is highly
// space-efficient but does not support deletion.
//
// This implementation deviates from standard Bloom filters to solve two specific
// problems found in high-throughput database servers: Cache Thrashing and
// Fixed Capacity.
//
// It combines two architectural patterns:
//
//  1. Blocked Bloom Filters (Cache Efficiency):
//     Standard Bloom filters scatter k hash bits randomly across the entire bitset.
//     For a large filter (e.g., 500MB), this results in k CPU cache misses per
//     operation, causing a significant latency bottleneck.
//     This implementation partitions the bitset into small "Blocks" that fit
//     exactly into a CPU L1 Cache Line (64 bytes). An element is hashed to a
//     single block, and all k bits are set within that block. This reduces the
//     memory bandwidth requirement from k random fetches to 1 fetch.
//     [1] F. Putze, P. Sanders, J. Singler. "Cache-, Hash- and Space-Efficient Bloom Filters".
//
//  2. Scalable Bloom Filters (Dynamic Growth):
//     Standard Bloom filters have a fixed capacity. Once exceeded, the false
//     positive rate (FPR) rises rapidly. This implementation uses a chain of
//     filters. When one filter layer fills up, a new layer is allocated with
//     greater capacity and tighter error bounds.
//     [2] P. Almeida, C. Baquero, N. Preguica, D. Hutchison. "Scalable Bloom Filters".
//
// The Algorithm
// =============
//
// To achieve maximum throughput while maintaining statistical safety, the hashing
// strategy uses 128 bits of entropy derived from a single 64-bit xxHash:
//
//  1. Primary Hash (Location): The incoming item is hashed using xxHash. The
//     result is used to select the specific Block (Cache Line) in the active layer.
//
//  2. Decorrelation (Entropy): To prevent "clumping" (where items mapping to the
//     same block always set the same bits), the primary hash is scrambled using
//     the SplitMix64 algorithm. This generates a secondary, statistically
//     independent hash used for bit selection.
//
//  3. Bit Setting (Content): Inside the 512-bit block, we use the Kirsch-Mitzenmacher
//     optimization to simulate k=8 independent hash functions using only the
//     double-hash derived in step 2.
//
// Memory Model (Zero-Copy)
// ========================
//
// This package is designed for persistence. It does not deserialize binary data
// into Go struct fields (which would cause heap allocations and GC pressure).
// Instead, it "mounts" the raw byte slice provided by the storage engine.
// Accessors read and write directly to the underlying byte array using Little
// Endian encoding.
//
// Data Layout
// ===========
//
// The filter is stored as a contiguous byte stream containing a Global Header
// followed by a variable number of Layers.
//
//	+-----------------------+---------------------------+---------------------------+
//	| Global Metadata       | Layer 1                   | Layer 2 ...               |
//	| (24 Bytes)            | (Header + Data)           | (Header + Data)           |
//	+-----------------------+---------------------------+---------------------------+
//
// Global Metadata:
//
//	+--------+--------+--------+
//	| Magic  | Total  | Num    |
//	| Number | Items  | Layers |
//	+--------+--------+--------+
//	  8B       8B       8B
//
// Layer Structure:
//
//	+--------+--------+--------+--------+---------------------------------------+
//	| Size   | Cap    | Count  | Error  | Block 0 | Block 1 | ... | Block N     |
//	| (Bytes)| (Max)  | (Cur)  | (Rate) | (64 B)  | (64 B)  |     | (64 B)      |
//	+--------+--------+--------+--------+---------------------------------------+
//	  8B       8B       8B       8B       <---------- Variable Size ---------->
//
// Block Structure (64 Bytes / 512 Bits):
//
//	+-----------------------------------------------------------------------+
//	| [Word 0] [Word 1] [Word 2] [Word 3] [Word 4] [Word 5] [Word 6] [Word 7]|
//	+-----------------------------------------------------------------------+
//	  64 bits  64 bits  ...
package bloom

import (
	"errors"

	"github.com/cespare/xxhash/v2"
)

const (
	// Configuration Defaults
	DefaultCapacity  = 1000
	DefaultErrorRate = 0.01
	GrowthFactor     = 2
	TighteningRatio  = 0.5

	// MaxLayers is a safety break to prevent OOM attacks or corruption.
	// Since capacity doubles every layer, 64 layers covers > 18 quintillion items.
	// We allow 1024 to be virtually infinite while protecting the allocator.
	MaxLayers = 1024
)

// Config holds the initialization parameters for a new Bloom Filter.
// These values are used when bootstrapping the first layer of a fresh filter.
// For existing filters, the stored layer parameters take precedence.
type Config struct {
	// InitialCapacity is the number of items the first layer is sized for.
	// Subsequent layers double this capacity (GrowthFactor = 2).
	InitialCapacity uint64

	// ErrorRate is the target false positive rate for the first layer.
	// Subsequent layers halve this rate (TighteningRatio = 0.5).
	ErrorRate float64
}

// DefaultConfig returns the default configuration for new Bloom Filters.
func DefaultConfig() Config {
	return Config{
		InitialCapacity: DefaultCapacity,
		ErrorRate:       DefaultErrorRate,
	}
}

// ScalableFilter is the runtime driver for the Bloom Filter.
//
// This struct is a transient "View" into the storage. It holds no data itself,
// only pointers and offsets into the `backing` slice. This allows it to be
// extremely lightweight (stack allocated) while managing potentially gigabytes
// of data.
type ScalableFilter struct {
	// backing is the raw storage provided by the database Store.
	// All writes to this slice are effectively writes to the persistence layer.
	backing []byte

	// layers is a pooled slice containing the in-memory index of where each
	// filter layer begins and ends. Using a pointer allows the slice to be
	// returned to the pool via Release(), reducing GC pressure at high throughput.
	// This cache allows O(1) access to specific layers during lookups.
	layers *[]layerOffset

	// config holds the initialization parameters for bootstrapping new filters.
	// This is only used when creating the first layer; existing filters use
	// their stored layer parameters for growth calculations.
	config Config
}

// Metadata returns a safe view into the global header of the filter.
func (sf *ScalableFilter) Metadata() Metadata {
	return Metadata(sf.backing[:MetadataSize])
}

// NewScalableFilter initializes or loads a Scalable Bloom Filter from raw bytes.
// It acts as a "File System Mount" operation that supports two modes depending
// on whether data is nil (fresh creation) or contains existing bytes (load).
//
// The cfg parameter specifies initialization settings for new filters. For
// existing filters (data != nil), the config is stored but only used if the
// filter needs to bootstrap (which shouldn't happen for valid existing data).
//
// IMPORTANT: The caller MUST call Release() when done with the filter to return
// pooled resources. Failure to do so will cause memory leaks under high load.
func NewScalableFilter(data []byte, cfg Config) (*ScalableFilter, error) {
	//
	// DESIGN
	// ------
	//
	// For fresh initialization (data == nil), we allocate the "Superblock"
	// (Global Header) and set the Magic number to mark this memory as a valid
	// Bloom Filter. No layers are created yet; they are allocated lazily on
	// the first Add() call.
	//
	// For loading existing data, we "mount" the byte slice by first validating
	// the Magic number to detect corruption, then scanning the linear byte
	// slice to rebuild the layers index. This ensures that subsequent
	// operations are O(1) rather than O(L) where L is the number of layers.
	//
	// The layers slice is acquired from a sync.Pool to minimize allocations
	// on the hot path. This is consistent with the HyperLogLog accumulator
	// pooling pattern used elsewhere in the codebase.
	//

	// Validate and apply defaults to config.
	if cfg.InitialCapacity == 0 {
		cfg.InitialCapacity = DefaultCapacity
	}
	if cfg.ErrorRate <= 0 || cfg.ErrorRate >= 1 {
		cfg.ErrorRate = DefaultErrorRate
	}

	// Acquire a pooled layers slice upfront.
	layers := GetLayers()

	// Fresh filter creation: bootstrap the persistent structure.
	if data == nil {
		// Allocate exactly enough for the Global Header.
		// Future writes will append to this slice.
		data = make([]byte, MetadataSize)

		sf := &ScalableFilter{backing: data, layers: layers, config: cfg}

		// Initialize the safety signature and counters using the safe view.
		meta := sf.Metadata()
		meta.SetMagic(Magic)
		meta.SetTotalItems(0)
		meta.SetNumLayers(0)

		return sf, nil
	}

	// Loading an existing filter: ensure we have at least the header bytes.
	if len(data) < MetadataSize {
		PutLayers(layers) // Return to pool on error
		return nil, errors.New("bloom: data too short to be a bloom filter")
	}

	sf := &ScalableFilter{backing: data, layers: layers, config: cfg}

	// Integrity Check: Verify this is actually a Bloom Filter.
	if sf.Metadata().Magic() != Magic {
		PutLayers(layers) // Return to pool on error
		return nil, errors.New("bloom: invalid magic number")
	}

	// Indexing: Scan the byte slice to rebuild the layer index.
	// We delegate this to reloadLayers because it logic is identical to what
	// is needed after growing the filter (reallocating the backing slice).
	if err := sf.reloadLayers(); err != nil {
		PutLayers(layers) // Return to pool on error
		return nil, err
	}

	return sf, nil
}

// Release returns pooled resources to their respective pools.
// This MUST be called when done with the filter to prevent memory leaks.
// It is safe to call Release multiple times; subsequent calls are no-ops.
func (sf *ScalableFilter) Release() {
	if sf.layers != nil {
		PutLayers(sf.layers)
		sf.layers = nil
	}
	sf.backing = nil
}

// Check tests if an item is likely in the set. It computes the hash internally
// using xxhash, consistent with the HyperLogLog implementation.
func (sf *ScalableFilter) Check(item []byte) bool {
	return sf.checkWithHash(xxhash.Sum64(item))
}

// checkWithHash is an internal helper that tests set membership using a
// pre-calculated hash. It iterates through layers from newest to oldest,
// since items are most likely to be in recent layers.
func (sf *ScalableFilter) checkWithHash(itemHash uint64) bool {
	layers := *sf.layers
	for i := len(layers) - 1; i >= 0; i-- {
		layer := layers[i]

		numBlocks := uint64(len(layer.data))
		if numBlocks == 0 {
			continue
		}

		// Select the block using raw hash distribution, then derive the
		// internal hash for bit operations. This decorrelation ensures that
		// landing in Block X doesn't imply a specific bit pattern.
		blockIdx := itemHash % numBlocks
		internalHash := mix(itemHash)

		if layer.data[blockIdx].Check(internalHash) {
			return true
		}
	}

	return false
}

// Add inserts an item into the filter chain. It computes the hash internally
// using xxhash, consistent with the HyperLogLog implementation. It returns the
// backing slice (which may have grown/moved in memory), a boolean indicating
// whether the item was newly added, and any error encountered during the operation.
func (sf *ScalableFilter) Add(item []byte) ([]byte, bool, error) {
	// Compute hash once internally. The helper avoids re-hashing during the
	// idempotency check, which would be wasteful for large items.
	itemHash := xxhash.Sum64(item)

	// Check if the item already exists. If so, adding would not change any
	// bits, so we skip incrementing counts to keep saturation accurate.
	if sf.checkWithHash(itemHash) {
		return sf.backing, false, nil
	}

	meta := sf.Metadata()
	numLayers := int(meta.NumLayers())

	// Bootstrap the first layer with configured settings if the filter is empty.
	if numLayers == 0 {
		if err := sf.addLayer(sf.config.InitialCapacity, sf.config.ErrorRate); err != nil {
			return nil, false, err
		}
		numLayers++
	} else {
		// Check if the active (last) layer is saturated. If so, grow the
		// filter by adding a new layer with doubled capacity and halved
		// error rate. This scaling ensures the global error rate stays bounded.
		layers := *sf.layers
		lastLayer := layers[numLayers-1]
		if lastLayer.header.Count() >= lastLayer.header.Capacity() {
			if numLayers >= MaxLayers {
				return nil, false, errors.New("bloom: max layers reached")
			}
			newCap := lastLayer.header.Capacity() * GrowthFactor
			newErr := lastLayer.header.ErrorRate() * TighteningRatio
			if err := sf.addLayer(newCap, newErr); err != nil {
				return nil, false, err
			}
			numLayers++
		}
	}

	// Insert into the active (last) layer. We use the raw hash to select the
	// block (cache line) and derive a decorrelated internal hash for the bit
	// operations within that block.
	layers := *sf.layers
	lastLayer := layers[numLayers-1]
	numBlocks := uint64(len(lastLayer.data))

	if numBlocks == 0 {
		return sf.backing, false, errors.New("bloom: active layer has zero size")
	}

	blockIdx := itemHash % numBlocks
	internalHash := mix(itemHash)

	if lastLayer.data[blockIdx].Add(internalHash) {
		// Update both the layer counter and the global counter.
		lastLayer.header.SetCount(lastLayer.header.Count() + 1)
		meta.SetTotalItems(meta.TotalItems() + 1)
		return sf.backing, true, nil
	}

	// Unreachable if Check() works correctly, but safe fallback.
	return sf.backing, false, nil
}
