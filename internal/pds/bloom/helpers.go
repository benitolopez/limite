package bloom

import (
	"math"
	"sync"
)

// layersPool reuses layer index slices to reduce GC pressure during high-throughput
// filter operations. We store *[]layerOffset (pointer) instead of []layerOffset
// (value) to avoid interface wrapping allocations (SA6002).
var layersPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate capacity for 4 layers (covers most common cases).
		// Scalable filters typically have 1-4 layers; beyond that is rare.
		s := make([]layerOffset, 0, 4)
		return &s
	},
}

// GetLayers returns a zeroed-out layer slice from the pool.
// The caller must return it via PutLayers when done.
func GetLayers() *[]layerOffset {
	ptr := layersPool.Get().(*[]layerOffset)
	*ptr = (*ptr)[:0] // Reset length, keep capacity
	return ptr
}

// PutLayers returns the pointer to the pool.
func PutLayers(ptr *[]layerOffset) {
	if ptr == nil {
		return
	}
	layersPool.Put(ptr)
}

// mix scrambles a 64-bit integer to remove correlation using the SplitMix64
// algorithm (public domain). This allows us to derive a second uncorrelated
// hash for bit-setting without reading the string bytes again.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// EstimateParameters calculates the optimal bitset size (in bytes) for a given
// capacity and error rate. The result is aligned to 64-byte blocks. The hash
// count k is fixed at 8 for this block-based implementation, following the
// approach in "Cache-, Hash- and Space-Efficient Bloom Filters" (Putze, Sanders,
// Singler, 2007). While the paper suggests dynamic k, for 512-bit blocks k=8
// is the optimal sweet spot.
func EstimateParameters(n uint64, p float64) (uint64, int) {
	// Sanitize inputs: n=0 creates invalid math, p must be strictly 0 < p < 1
	// to avoid Log() returning -Inf or non-negative values.
	if n == 0 {
		n = 1
	}
	if p <= 0 {
		p = 1e-9
	} else if p >= 1.0 {
		p = 0.99
	}

	// Standard Bloom Filter formula: m = -(n * ln(p)) / (ln(2)^2)
	ln2 := math.Log(2)
	m := -float64(n) * math.Log(p) / (ln2 * ln2)

	// Convert bits to bytes and align to 64-byte cache lines.
	bytes := uint64(math.Ceil(m / 8.0))

	const blockSize = 64
	if bytes < blockSize {
		bytes = blockSize
	} else if bytes%blockSize != 0 {
		bytes += blockSize - (bytes % blockSize)
	}

	return bytes, 8
}
