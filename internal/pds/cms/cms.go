// Package cms implements the Count-Min Sketch data structure with Conservative Update.
//
// The Count-Min Sketch (CMS) is a probabilistic data structure used to estimate
// the frequency of events in a data stream. Unlike exact counters, CMS uses
// sub-linear space at the cost of some accuracy - it may overestimate frequencies
// but never underestimates them.
//
// This implementation uses the Conservative Update technique, which significantly
// reduces over-counting compared to standard CMS. Instead of incrementing all
// counters for an item, Conservative Update only increments counters that would
// become the new minimum, reducing the impact of hash collisions from heavy hitters.
//
// Binary Format
// =============
//
// The CMS is stored as a contiguous byte slice with the following layout:
//
//	+-------+-------+-------+-------+------------------+
//	| Magic | Width | Depth | Count | Counters...      |
//	+-------+-------+-------+-------+------------------+
//	  4B      4B      4B      8B      Width*Depth*4B
//
// Header (20 bytes):
//   - Magic (4 bytes): "CMS1" in Little Endian (0x31534D43)
//   - Width (4 bytes): Number of columns (hash table size)
//   - Depth (4 bytes): Number of rows (number of hash functions)
//   - Count (8 bytes): Total number of items added (sum of all deltas)
//
// Body:
//   - Counters: Width * Depth uint32 values in Little Endian, stored row-major.
//
// The Counter at row i, column j is located at offset:
//
//	HeaderSize + (i * Width + j) * 4
//
// Conservative Update Algorithm
// =============================
//
// Standard CMS increments all d counters for an item. This causes heavy hitters
// (frequently occurring items) to pollute counters shared with rare items.
//
// Conservative Update improves accuracy by first finding the minimum counter
// value across all d rows for the item. Instead of incrementing all counters
// by delta, it computes a target value (min + delta) and only raises counters
// that are below this target. Counters already at or above the target are left
// untouched. This ensures counters are only raised to the new minimum "floor",
// preventing unnecessary inflation from hash collisions with heavy hitters.
//
// Research shows this technique can reduce error by 50% or more for Zipfian
// distributions, which are common in real-world data (web traffic, word
// frequencies, etc.).
package cms

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/cespare/xxhash/v2"
)

const (
	// Magic is the 4-byte identifier for CMS data.
	// "CMS1" in Little Endian: 0x31 0x53 0x4D 0x43
	Magic = 0x31534D43

	// HeaderSize is the size of the CMS header in bytes.
	// 4 (Magic) + 4 (Width) + 4 (Depth) + 8 (Count) = 20 bytes
	HeaderSize = 20
)

var (
	// ErrInvalidData is returned when the data is too short to be valid CMS.
	ErrInvalidData = errors.New("cms: data too short")

	// ErrInvalidMagic is returned when the magic bytes don't match.
	ErrInvalidMagic = errors.New("cms: invalid magic identifier")
)

// CMS represents a Count-Min Sketch backed by a raw byte slice.
// The backing slice is the source of truth - all operations read from
// and write to it directly (zero-copy).
type CMS struct {
	backing []byte
}

// New creates a fresh Count-Min Sketch with the specified dimensions.
// The width determines the number of columns (hash table size per row).
// The depth determines the number of rows (hash functions).
//
// Memory usage: HeaderSize + (width * depth * 4) bytes
//
// Typical values:
//   - width=1000, depth=5 for ~20KB, ~0.27% error rate
//   - width=10000, depth=7 for ~280KB, ~0.027% error rate
func New(width, depth uint32) *CMS {
	// Calculate total size: Header + (Width * Depth * 4 bytes per counter)
	size := uint64(HeaderSize) + (uint64(width) * uint64(depth) * 4)
	data := make([]byte, size)

	// Initialize header
	binary.LittleEndian.PutUint32(data[0:4], Magic)
	binary.LittleEndian.PutUint32(data[4:8], width)
	binary.LittleEndian.PutUint32(data[8:12], depth)
	binary.LittleEndian.PutUint64(data[12:20], 0) // Count starts at 0

	return &CMS{backing: data}
}

// NewFromBytes loads an existing CMS from a byte slice.
// This is a zero-copy operation - the CMS wraps the provided slice directly.
// The caller must ensure the slice is not modified externally while the CMS is in use.
func NewFromBytes(data []byte) (*CMS, error) {
	if len(data) < HeaderSize {
		return nil, ErrInvalidData
	}

	if binary.LittleEndian.Uint32(data[0:4]) != Magic {
		return nil, ErrInvalidMagic
	}

	// Validate data size matches header dimensions
	width := binary.LittleEndian.Uint32(data[4:8])
	depth := binary.LittleEndian.Uint32(data[8:12])
	expectedSize := uint64(HeaderSize) + (uint64(width) * uint64(depth) * 4)

	if uint64(len(data)) < expectedSize {
		return nil, ErrInvalidData
	}

	return &CMS{backing: data}, nil
}

// HasValidMagic checks if data starts with the CMS magic bytes without allocation.
// This is useful for handlers to quickly check the data type before full parsing.
func HasValidMagic(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return binary.LittleEndian.Uint32(data[0:4]) == Magic
}

// Width returns the number of columns (hash table size per row).
func (c *CMS) Width() uint32 {
	return binary.LittleEndian.Uint32(c.backing[4:8])
}

// Depth returns the number of rows (hash functions).
func (c *CMS) Depth() uint32 {
	return binary.LittleEndian.Uint32(c.backing[8:12])
}

// Count returns the total number of items added (sum of all deltas).
func (c *CMS) Count() uint64 {
	return binary.LittleEndian.Uint64(c.backing[12:20])
}

// Bytes returns the underlying byte slice.
// This is used for persistence - the returned slice is the CMS's backing store.
func (c *CMS) Bytes() []byte {
	return c.backing
}

// getHashes returns the base hash components (h1, h2) for double hashing.
// We use xxhash for the primary hash and SplitMix64 for decorrelation.
func (c *CMS) getHashes(item []byte) (uint64, uint64) {
	h := xxhash.Sum64(item)

	// SplitMix64 decorrelation for the second hash component.
	// This produces a statistically independent second hash without
	// re-hashing the input bytes.
	h2 := h
	h2 ^= h2 >> 30
	h2 *= 0xbf58476d1ce4e5b9
	h2 ^= h2 >> 27
	h2 *= 0x94d049bb133111eb
	h2 ^= h2 >> 31

	return h, h2
}

// getIndex computes the column index for a specific row using double hashing.
// Formula: h_i(x) = (h1 + i*h2) % width
func (c *CMS) getIndex(row uint32, h1, h2 uint64) uint32 {
	width := uint64(c.Width())
	return uint32((h1 + uint64(row)*h2) % width)
}

// Incr performs a Conservative Update increment on the item.
// It returns true if the backing slice was modified.
func (c *CMS) Incr(item []byte, delta uint32) bool {
	//
	// DESIGN
	// ------
	//
	// Conservative Update differs from standard CMS by being selective about
	// which counters to increment. We first scan all d rows to find the minimum
	// counter value for this item. This minimum represents the "floor" - the
	// best estimate we have of the item's true frequency.
	//
	// We then compute a target value (min + delta) and only raise counters
	// that fall below this target. Counters already at or above the target
	// are left untouched, since incrementing them would only add noise from
	// hash collisions. This prevents heavy hitters from artificially inflating
	// counters that happen to collide with rare items.
	//

	if delta == 0 {
		return false
	}

	width := c.Width()
	depth := c.Depth()
	h1, h2 := c.getHashes(item)

	// Find the current minimum value across all rows. This two-pass approach
	// (find min, then update) is the key insight of Conservative Update.
	minVal := uint32(math.MaxUint32)

	for i := uint32(0); i < depth; i++ {
		idx := c.getIndex(i, h1, h2)
		offset := HeaderSize + (uint64(i)*uint64(width)+uint64(idx))*4

		val := binary.LittleEndian.Uint32(c.backing[offset:])
		if val < minVal {
			minVal = val
		}
	}

	// Compute target value with overflow protection.
	var targetVal uint32
	if uint64(minVal)+uint64(delta) > math.MaxUint32 {
		targetVal = math.MaxUint32
	} else {
		targetVal = minVal + delta
	}

	// Update counters conservatively: only raise those below the target.
	changed := false

	for i := uint32(0); i < depth; i++ {
		idx := c.getIndex(i, h1, h2)
		offset := HeaderSize + (uint64(i)*uint64(width)+uint64(idx))*4

		currentVal := binary.LittleEndian.Uint32(c.backing[offset:])

		if currentVal < targetVal {
			binary.LittleEndian.PutUint32(c.backing[offset:], targetVal)
			changed = true
		}
	}

	if changed {
		curTotal := c.Count()
		binary.LittleEndian.PutUint64(c.backing[12:20], curTotal+uint64(delta))
	}

	return changed
}

// Query returns the estimated frequency of the item.
// The estimate is the minimum counter value across all rows.
// This is always >= the true count (may overestimate, never underestimates).
func (c *CMS) Query(item []byte) uint32 {
	width := c.Width()
	depth := c.Depth()
	h1, h2 := c.getHashes(item)

	minVal := uint32(math.MaxUint32)

	for i := uint32(0); i < depth; i++ {
		idx := c.getIndex(i, h1, h2)
		offset := HeaderSize + (uint64(i)*uint64(width)+uint64(idx))*4

		val := binary.LittleEndian.Uint32(c.backing[offset:])
		if val < minVal {
			minVal = val
		}
	}

	return minVal
}
