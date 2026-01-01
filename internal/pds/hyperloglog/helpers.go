package hyperloglog

import (
	"math"
	"math/bits"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// hllSigma is the implementation of the `sigma(x)` helper function as defined in [3].
//
// This function is used by the Ertl algorithm to add the contribution of registers
// that are equal to zero.
func hllSigma(x float64) float64 {
	if x == 1. {
		return math.Inf(1)
	}

	zPrime := 0.0
	y := 1.0
	z := x

	for {
		x *= x
		zPrime = z
		z += x * y
		y += y

		if zPrime == z {
			break
		}
	}

	return z
}

// hllTau is the implementation of the `tau(x)` helper function as defined in [3].
//
// This function is used by the Ertl algorithm to correct for the bias introduced
// by registers that have not been modified.
func hllTau(x float64) float64 {
	if x == 0. || x == 1. {
		return 0.
	}

	zPrime := 0.0
	y := 1.0
	z := 1 - x

	for {
		x = math.Sqrt(x)
		zPrime = z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y

		if zPrime == z {
			break
		}
	}

	return z / 3
}

// HasValidMagic checks if data starts with the HLL magic bytes without allocation.
func HasValidMagic(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == 'H' && data[1] == 'Y' && data[2] == 'L' && data[3] == 'L'
}

// hashToIndexAndRank computes the HLL register index and rank for a given item.
// It is a pure function that encapsulates the hashing and bit-splitting logic,
// which is the first step of the Add operation.
func hashToIndexAndRank(data []byte) (index uint64, rank uint8) {
	//
	// DESIGN
	// ------
	//
	// The core idea of the HLL algorithm is to use the statistical properties
	// of uniformly distributed hash values to estimate cardinality.
	//
	// When an item is added, its 64-bit hash is split into two parts. The `p`
	// least significant bits (14 bits in our case) are used to select one of
	// the `m` (16,384) registers. The remaining `q` most significant bits
	// (50 bits) are used to find the position of the least significant `1` bit,
	// which determines the rank `ρ` (rho). The rank is defined as that position + 1.
	//
	// Each register's purpose is to store the maximum rank ever observed for an
	// item hashing to that register. A higher maximum rank is statistically less
	// likely and indicates that more unique items have likely been seen. This
	// function implements that "observe and update if greater" logic.
	//

	// Hash the input data to a 64-bit unsigned integer.
	hashValue := xxhash.Sum64(data)

	// Determine the register index by taking the `p` least significant bits of the hash.
	// We do this with a bitwise AND against a pre-calculated mask.
	index = hashValue & pMask

	// Calculate the rank (ρ).
	//
	// The rank is determined by the pattern of zeros in the 50 most significant
	// bits of the original hash. To calculate this efficiently, we first
	// isolate these bits by right-shifting `hashValue` by `hllP` (14).
	//
	// This resulting `rankHash` is then modified with a guard bit. We set the
	// bit at position `hllQ` (the 51st bit) to 1. This is a safety measure from
	// that guarantees the `rankHash` is never zero and that the maximum number
	// of trailing zeros we can count is `hllQ` (50).
	//
	// Finally, we use the `bits.TrailingZeros64` function, which finds the
	// position of the least significant 1-bit. The algorithm defines the rank
	// as this position + 1, so the maximum possible rank is 51.
	rankHash := hashValue >> p
	rankHash |= uint64(1) << q
	rank = uint8(bits.TrailingZeros64(rankHash)) + 1

	return index, rank
}

// accumulatorPool reuses 16KB buffers to reduce GC pressure during multi-key merges.
// We store *[]byte (pointer) instead of []byte (value) to avoid
// interface wrapping allocations (SA6002).
var accumulatorPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, M)
		return &b // Return pointer to slice header
	},
}

// GetAccumulator returns a zeroed-out 16KB buffer from the pool.
// The caller must return it via PutAccumulator when done.
func GetAccumulator() *[]byte {
	ptr := accumulatorPool.Get().(*[]byte)
	buf := *ptr

	// Fast zeroing.
	// We use the built-in clear() which compiles down to memclr calls.
	clear(buf)

	return ptr
}

// PutAccumulator returns the pointer to the pool.
func PutAccumulator(ptr *[]byte) {
	accumulatorPool.Put(ptr)
}

// CompressToSerialized converts the raw 16KB accumulator into a serialized
// dense HLL format ready for storage. With 8-bit registers, this is simply
// a header prepend and memory copy (no actual compression is performed).
//
// The reuseBuffer parameter allows the caller to provide an existing buffer
// to avoid memory allocation in the hot path. If nil or too small, a new
// buffer is allocated.
func CompressToSerialized(rawRegisters, reuseBuffer []byte) []byte {
	const size = headerSize + denseSize

	var output []byte
	if cap(reuseBuffer) >= size {
		output = reuseBuffer[:size]
	} else {
		output = make([]byte, size)
	}

	// 1. Write Header
	_ = output[15] // Bounds check hint
	output[0], output[1], output[2], output[3] = 'H', 'Y', 'L', 'L'
	output[4] = byte(dense)
	// Clear reserved bytes (indices 5 to 14)
	for i := 5; i < 15; i++ {
		output[i] = 0
	}
	// Set Dirty Bit
	output[15] = 0x80

	// 2. Copy Data
	// With 8-bit registers, compression is just a memory copy.
	copy(output[headerSize:], rawRegisters)

	return output
}

// UpdateDenseFromAccumulator merges a raw 16KB accumulator into an existing
// 16KB dense HLL buffer IN-PLACE.
//
// It returns true if the data was modified (requiring a cache invalidation).
func UpdateDenseFromAccumulator(denseData, rawRegisters []byte) bool {
	//
	// DESIGN
	// ------
	//
	// This is the "Hot Path" for merging into an existing key. It avoids the
	// cost of deserialization and memory allocation entirely by iterating
	// directly over the dense data and applying the MAX logic in-place.
	//
	// With 8-bit registers, each comparison is a simple byte operation. The
	// loop is unrolled (stride 8) to improve cache utilization and allow the
	// compiler to vectorize the comparisons where possible.
	//

	dense := denseData
	raw := rawRegisters
	changed := false

	// BCE Hint: Remove bounds checks inside the loop
	_ = dense[denseSize-1]
	_ = raw[M-1]

	// Process 8 registers at a time for better cache utilization.
	for i := 0; i < M; i += 8 {
		if raw[i] > dense[i] {
			dense[i] = raw[i]
			changed = true
		}
		if raw[i+1] > dense[i+1] {
			dense[i+1] = raw[i+1]
			changed = true
		}
		if raw[i+2] > dense[i+2] {
			dense[i+2] = raw[i+2]
			changed = true
		}
		if raw[i+3] > dense[i+3] {
			dense[i+3] = raw[i+3]
			changed = true
		}
		if raw[i+4] > dense[i+4] {
			dense[i+4] = raw[i+4]
			changed = true
		}
		if raw[i+5] > dense[i+5] {
			dense[i+5] = raw[i+5]
			changed = true
		}
		if raw[i+6] > dense[i+6] {
			dense[i+6] = raw[i+6]
			changed = true
		}
		if raw[i+7] > dense[i+7] {
			dense[i+7] = raw[i+7]
			changed = true
		}
	}

	return changed
}
