// Package hyperloglog implements the HyperLogLog algorithm for cardinality estimation.
//
// The HyperLogLog (HLL) algorithm is a probabilistic data structure used to
// estimate the number of distinct elements in a multiset. It achieves this
// using a fixed amount of memory, regardless of the actual cardinality. This
// makes it invaluable for applications like counting unique visitors, distinct
// IP addresses, or unique search queries in massive data streams.
//
// This implementation is based on the following ideas:
//
//   - The use of a 64-bit hash function as proposed in [1], enabling cardinality
//     estimation beyond 10^9 elements at the cost of just 1 additional bit per
//     register compared to 32-bit hashes.
//   - The use of 16,384 registers (p=14) for a standard error of ~0.81%.
//   - The use of 8-bit registers (one byte per register), resulting in 16KB per
//     key in dense mode. This differs from the Redis approach which packs 6-bit
//     values into 12KB. See "Register Size Trade-off" below for the rationale.
//   - The cardinality estimation algorithm from Ertl [3], which provides better
//     accuracy than the original HyperLogLog formula.
//
// [1] Heule, Nunkesser, Hall: HyperLogLog in Practice: Algorithmic
//
//	Engineering of a State of The Art Cardinality Estimation Algorithm.
//
// [2] P. Flajolet, Éric Fusy, O. Gandouet, and F. Meunier. Hyperloglog: The
//
//	analysis of a near-optimal cardinality estimation algorithm.
//
// [3] O. Ertl. New cardinality estimation algorithms for HyperLogLog sketches.
//
// The Algorithm
// =============
//
// The HLL algorithm exploits a statistical property of uniformly distributed
// hash values. When hashing random inputs, the probability of a hash starting
// with k leading zeros is 1/2^k. By observing the maximum number of leading
// zeros across many hashes, it is possible to estimate how many unique items
// have been processed.
//
// To reduce variance, the hash space is partitioned into m "registers" (buckets).
// Each input element is hashed to a 64-bit value. This value is then split:
//
//  1. The lower p bits (14 bits here) select one of m=2^p registers. With p=14,
//     there are 16,384 registers.
//  2. The remaining q=64-p bits (50 bits) are used to compute the "rank": the
//     position of the first 1-bit, starting from the least significant bit,
//     plus one. The maximum rank is therefore q+1 = 51.
//
// Each register stores the maximum rank ever observed for elements hashing to
// that bucket. The final cardinality estimate is computed using a harmonic mean
// of 2^(-rank) across all registers, with bias corrections.
//
// Data Representations
// ====================
//
// This implementation uses two representations:
//
//  1. A "dense" representation where every register is stored as a single byte.
//     This uses 16,384 bytes (16KB) of memory per HLL.
//
//  2. A "sparse" representation that stores only non-zero registers as (index,
//     value) pairs in a sorted list. This is highly memory-efficient for low-
//     cardinality sets. For example, an HLL tracking 100 unique elements uses
//     approximately 300 bytes instead of 16KB.
//
// The sparse representation is automatically promoted to dense when the number
// of non-zero registers exceeds a configurable threshold.
//
// HLL Header
// ==========
//
// Both representations share a 16-byte header:
//
//	+------+---+-----+----------+
//	| HYLL | E | N/U | Cardin.  |
//	+------+---+-----+----------+
//
// The first 4 bytes are the magic string "HYLL" for type identification.
// "E" is one byte encoding: 0 for dense, 1 for sparse.
// "N/U" are three unused bytes reserved for future use.
// "Cardin." is the 64-bit cached cardinality in little-endian format.
//
// The most significant bit of the cached cardinality (bit 63) serves as a
// "dirty flag". When set, it indicates the cache is stale and must be
// recomputed. This optimization allows fast responses for repeated COUNT
// operations when the data has not changed.
//
// Dense Representation
// ====================
//
// In dense mode, registers are stored contiguously as a byte array:
//
//	+--------+--------+--------+------//      //--+
//	| reg[0] | reg[1] | reg[2] | reg[3] ....     |
//	+--------+--------+--------+------//      //--+
//
// Each register occupies exactly one byte. While the maximum rank (51) would
// fit in 6 bits, the 8-bit layout was chosen for performance reasons (see
// "Register Size Trade-off" below). Direct byte access eliminates the need
// for bit-packing and unpacking operations.
//
// Sparse Representation
// =====================
//
// In sparse mode, only non-zero registers are stored. Each entry consists of
// a 2-byte index (uint16, little-endian) and a 1-byte value:
//
//	+-------+-------+-------+-------+     +-------+-------+
//	| Idx_0 | Val_0 | Idx_1 | Val_1 | ... | Idx_N | Val_N |
//	+-------+-------+-------+-------+     +-------+-------+
//
// The entries are kept sorted by index to enable binary search during Add
// operations. A 4-byte count prefix (uint32, little-endian) is written before
// the entries during serialization.
//
// For low cardinalities, this representation is significantly more compact:
//
//	Cardinality    ~Bytes Used (Sparse)
//	-----------    --------------------
//	100            ~300
//	500            ~1500
//	1000           ~3000
//	3000           ~9000
//
// The dense representation uses 16,384 bytes, so sparse mode provides memory
// savings up to cardinalities of approximately 5,000-6,000 unique elements.
//
// Register Size Trade-off (8-bit vs 6-bit)
// ========================================
//
// The maximum rank for a 64-bit hash with p=14 is 51, which fits in 6 bits.
// Redis packs 16,384 six-bit values into 12,288 bytes (12KB) using bit
// manipulation. This approach was initially implemented here as well.
//
// However, the 6-bit packed representation caused significant performance
// issues with the HLL.MERGE operation. Merging two HLLs requires:
//  1. Unpacking 6-bit values from the source
//  2. Comparing with existing values
//  3. Repacking into the destination
//
// This bit manipulation is CPU-intensive and prevents compiler vectorization.
// Benchmarks using redis-benchmark on a VM (Ubuntu Server 24.04.3 LTS, 16GB RAM)
// showed that switching to 8-bit registers improved MERGE throughput by ~37%:
//
//	Representation    MERGE ops/sec    Pipeline (x10) ops/sec
//	--------------    -------------    ----------------------
//	6-bit (12KB)      ~100,000         ~142,000
//	8-bit (16KB)      ~138,000 (+37%)  ~193,000 (+36%)
//	Redis             ~150,000         ~201,000
//
// The 8-bit implementation reaches 92-96% of Redis MERGE throughput while
// being significantly simpler to implement and maintain. The trade-off is
// 4KB additional memory per key in dense mode, which is acceptable given
// the performance gains for merge-heavy workloads.
package hyperloglog

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
)

const (
	p            = 14                      // p: the precision parameter (m = 2^p)
	q            = 50                      // q: bits for rank calculation (64 - p)
	registerBits = 8                       // registerBits: bits per register (we use 8, see above)
	M            = 1 << p                  // m: the number of registers (2^p = 2^14 = 16384)
	registerMax  = (1 << registerBits) - 1 // 2^registerBits - 1, which for 8 bits is 255
	denseSize    = M                       // physical size in bytes (16384, one byte per register)

	// pMask is a mask used to extract the 14 LSBs for the register index.
	pMask = (1 << p) - 1 // 2^14 - 1 = 16383 (0x3FFF)

	// alpha is the "alpha" constant for 64-bit hashes from the Ertl paper.
	alpha = 0.721347520444481703680 // 0.5 / log(2)

	// DefaultSparseThreshold is the number of non-zero registers that will
	// trigger a conversion from sparse to dense.
	DefaultSparseThreshold = 750 // 3000 bytes / 4 bytes per sparseRegister struct
)

type encoding uint8

const (
	dense  encoding = 0
	sparse encoding = 1
	raw    encoding = 2
)

type HLL struct {
	header          hllHeader
	mu              sync.RWMutex
	denseData       []byte           // The 16KB slice for dense mode.
	sparseData      []sparseRegister // A sorted slice of non-zero registers for sparse mode.
	sparseThreshold int              // Threshold for sparse to dense conversion.
}

func New() *HLL {
	return NewWithThreshold(DefaultSparseThreshold)
}

// NewWithThreshold creates a new HLL with a custom sparse-to-dense threshold.
// The threshold controls how many sparse registers are allowed before
// converting to the dense representation. Higher values use less memory
// for low-cardinality sets but have slower Add operations.
func NewWithThreshold(sparseThreshold int) *HLL {
	return &HLL{
		// Initialize the header to the correct starting state.
		header: hllHeader{
			encoding:     sparse,
			cacheInvalid: true,
		},

		// The sparseData slice starts empty. We allocate a small capacity
		// to avoid reallocations for the first few adds.
		sparseData:      make([]sparseRegister, 0, 8),
		sparseThreshold: sparseThreshold,
	}
}

// NewRaw creates a temporary HLL wrapper around a raw byte slice accumulator.
// This is used by the server to calculate the cardinality of a merged result.
// The provided slice is used directly as the internal denseData.
func NewRaw(registers []byte) *HLL {
	return &HLL{
		header: hllHeader{
			encoding:     raw,
			cacheInvalid: true,
		},
		denseData: registers,
	}
}

// Add incorporates a new item into the HyperLogLog estimate.
// This function is thread-safe.
//
// It acts as a dispatcher, checking the current encoding of the HLL and
// calling the appropriate implementation (sparse or dense) to perform the
// add operation.
func (h *HLL) Add(data []byte) bool {
	h.mu.Lock()
	defer h.mu.Unlock() // Guarantees the lock is released.

	// Check the current encoding and dispatch to the correct implementation.
	// The HLL begins as sparse and is promoted to dense when it grows.
	if h.header.encoding == sparse {
		return h.sparseAdd(data)
	} else {
		return h.denseAdd(data)
	}
}

// Count returns the estimated cardinality of the set.
// This function is thread-safe and highly optimized for concurrent reads.
// It uses a cached value for instant responses and implements a double-checked
// locking pattern to handle cache recalculation efficiently under load.
//
// This implements the improved cardinality estimation algorithm
// from Ertl [3], which provides better accuracy and bias correction
// than the classic HyperLogLog harmonic mean formula.
func (h *HLL) Count() uint64 {
	//
	// DESIGN
	// ------
	//
	// Calculating the cardinality is computationally expensive, requiring a full
	// scan of all 16,384 registers. To make `Count` a fast operation in the common
	// case, the result is cached in the `hllHeader`. This method implements a
	// "double-checked locking" pattern to safely access this cache in a highly
	// concurrent environment without introducing performance bottlenecks.
	//
	// The fast path, for a valid cache, uses a read lock (`RLock`). This allows
	// many concurrent readers to access the cached value with minimal contention.
	//
	// If the cache is invalid, the goroutine must promote its lock to an exclusive
	// write lock (`Lock`). This ensures that only one operation will perform the
	// expensive recalculation.
	//

	// Check for a valid cached value.
	h.mu.RLock()
	if !h.header.cacheInvalid {
		cachedValue := h.header.cachedCardinality
		h.mu.RUnlock()
		return cachedValue
	}
	h.mu.RUnlock()

	// If the cache is invalid, we must acquire an exclusive write lock to ensure
	// that only one operation performs the expensive recalculation.
	h.mu.Lock()
	defer h.mu.Unlock()

	// Re-check the cache validity after acquiring the lock. This is necessary
	// to handle the case where another operation has already recalculated and
	// repopulated the cache while this one was waiting for the lock. This
	// "double-check" prevents redundant work.
	if !h.header.cacheInvalid {
		return h.header.cachedCardinality
	}

	// The Ertl algorithm operates on a histogram of the register values,
	// not the raw values themselves. We build this histogram by iterating
	// through all logical registers and counting the occurrences of each rank.
	var regHisto []int
	switch h.header.encoding {
	case sparse:
		regHisto = h.getSparseHisto()
	case raw:
		// Internal Only: This handles the temporary accumulator used during
		// multi-key merges (HLL.COUNT key1 key2). The registers are stored
		// as plain bytes in denseData, requiring no unpacking.
		regHisto = h.getRawHisto()
	default:
		regHisto = h.getDenseHisto()
	}

	// This is the core of the Ertl estimation formula, as defined in [3].
	// It calculates a raw estimate `Z` based on the register histogram, using
	// the `hllTau` and `hllSigma` helper functions to account for the bias
	// of zeroed and non-updated registers.
	z := float64(M) * hllTau(float64(M-regHisto[q+1])/float64(M))
	for j := q; j >= 1; j-- {
		z += float64(regHisto[j])
		z *= 0.5
	}
	z += float64(M) * hllSigma(float64(regHisto[0])/float64(M))

	// The final cardinality estimate `E` is calculated from the raw estimate `Z`
	// using the standard HLL formula with its alpha correction constant.
	E := alpha * float64(M*M) / z

	// The result is rounded to the nearest integer.
	cardinality := uint64(math.Round(E))

	// Update the cache before returning.
	h.header.cachedCardinality = cardinality
	h.header.cacheInvalid = false
	return cardinality
}

// getRawHisto builds a histogram of register values from a raw (unpacked)
// byte slice.
//
// This helper is exclusively used by the internal 'hllRaw' encoding during
// multi-key merge operations. In this mode, the 'denseData' field is
// repurposed to hold the temporary 16KB accumulator (one byte per register).
func (h *HLL) getRawHisto() []int {
	regHisto := make([]int, 64)
	data := h.denseData

	// OPTIMIZATION: Block Processing
	//
	// We iterate over the accumulator in blocks of 8 registers (8 bytes).
	// This mirrors the Redis optimization (hllRawRegHisto) which checks
	// 64-bit words for zero-ness.
	//
	// Since HLL registers are statistically likely to be zero (especially
	// in the "tails" of the distribution or in non-saturated HLLs),
	// checking 8 registers at once allows us to skip individual increments
	// for the vast majority of the array, providing a significant CPU speedup.
	for i := 0; i < len(data); i += 8 {
		// Fast Path: Check if the entire 8-byte block is zero by casting
		// it to a uint64. This is much faster than 8 separate byte checks.
		if binary.LittleEndian.Uint64(data[i:]) == 0 {
			regHisto[0] += 8
			continue
		}

		// Slow Path: Count individual values.
		regHisto[data[i]]++
		regHisto[data[i+1]]++
		regHisto[data[i+2]]++
		regHisto[data[i+3]]++
		regHisto[data[i+4]]++
		regHisto[data[i+5]]++
		regHisto[data[i+6]]++
		regHisto[data[i+7]]++
	}

	return regHisto
}

// Serialize converts the in-memory HLL struct into a contiguous byte slice.
// This function serves as the bridge between the "Smart Data" (structs with logic)
// and the "Dumb Store" (raw byte storage).
//
// It acquires a read-lock to ensure that the internal state (especially the
// sparse list or dense buffer) is not mutated by a concurrent Add() operation
// during the serialization process.
func (h *HLL) Serialize() []byte {
	//
	// DESIGN
	// ------
	//
	// The serialized format varies depending on the current encoding of the HLL.
	// Both formats start with the standard 16-byte header, which contains the
	// magic string, encoding flag, and cached cardinality.
	//
	// 1. DENSE REPRESENTATION
	//    For dense HLLs, the data is a fixed-size byte array (one byte per register).
	//    We simply append the raw 16,384 bytes of registers immediately after the header.
	//
	//    Layout:
	//    +----------------+---------------------------+
	//    | Header (16 B)  | Dense Registers (16 KB)   |
	//    +----------------+---------------------------+
	//
	// 2. SPARSE REPRESENTATION
	//    For sparse HLLs, the registers are stored in a Go slice of structs, which
	//    cannot be directly cast to bytes. We must flatten this list. To ensure
	//    the data can be reconstructed, we prefix the list with a 4-byte count.
	//    Each register is then encoded as 3 bytes: 2 bytes for the index (uint16)
	//    and 1 byte for the rank value (uint8).
	//
	//    Layout:
	//    +----------------+--------------+-------+-------+     +-------+-------+
	//    | Header (16 B)  | Count (4 B)  | Idx_0 | Val_0 | ... | Idx_N | Val_N |
	//    +----------------+--------------+-------+-------+     +-------+-------+
	//
	// All multi-byte integers (Count, Index) are encoded using Little Endian
	// to ensure portability across different CPU architectures.
	//

	h.mu.RLock()
	defer h.mu.RUnlock()

	// 1. Serialize Header (16 bytes)
	headerBytes := h.header.serialize()

	// 2. Append Data based on encoding
	if h.header.encoding == dense {
		// Dense: Header + 16KB Data
		result := make([]byte, 0, len(headerBytes)+len(h.denseData))
		result = append(result, headerBytes...)
		result = append(result, h.denseData...)
		return result
	} else {
		// Sparse: Header + Count(4 bytes) + Registers(3 bytes each)
		count := uint32(len(h.sparseData))
		result := make([]byte, 0, len(headerBytes)+4+(int(count)*3))

		result = append(result, headerBytes...)

		// Write count of sparse registers
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, count)
		result = append(result, lenBuf...)

		// Write registers (Index uint16 + Value uint8)
		regBuf := make([]byte, 2)
		for _, reg := range h.sparseData {
			binary.LittleEndian.PutUint16(regBuf, reg.index)
			result = append(result, regBuf...)
			result = append(result, reg.value)
		}
		return result
	}
}

// GetCachedCount peeks into the raw byte representation of an HLL to retrieve
// the cached cardinality without full deserialization.
//
// This function is a critical optimization helper. It allows the command handler
// to perform a "fast path" check for the cardinality. If the cache is valid,
// the server can respond immediately using a Read Lock, avoiding the overhead
// of memory allocation, parsing, and exclusive locking required for recomputation.
func GetCachedCount(data []byte) (uint64, bool) {
	//
	// DESIGN
	// ------
	//
	// The HLL header format reserves bytes 8-15 for the 64-bit cached cardinality.
	// To save space, we overload the Most Significant Bit (MSB) of this value
	// to act as a "dirty" flag.
	//
	// Since the cardinality is stored in Little Endian format, the MSB of the
	// 64-bit integer is located in the last byte (byte 15) at bit 7 (0x80).
	//
	// The logic is:
	// 1. Check if the dirty bit is set (1). If so, the cache is stale (false).
	// 2. If the bit is clear (0), the cache is valid. We read the uint64 and
	//    mask out the MSB to ensure we return the clean cardinality value.
	//

	if len(data) < headerSize {
		return 0, false
	}

	// Check Dirty Bit (MSB of byte 15)
	if (data[15] & 0x80) != 0 {
		return 0, false
	}

	raw := binary.LittleEndian.Uint64(data[8:16])

	// Mask out the MSB just in case, though it should be 0
	return raw & ^(uint64(1) << 63), true
}

// Deserialize reconstructs an in-memory HLL struct from its serialized byte
// representation. This function is the counterpart to Serialize and is
// responsible for parsing the "dumb" bytes back into a "smart" object that
// supports operations.
//
// It performs strict validation on the input to ensure that corrupted or
// truncated data from the store does not cause runtime panics.
func Deserialize(data []byte) (*HLL, error) {
	//
	// DESIGN
	// ------
	//
	// 1. HEADER PARSING
	//    We first extract the standard 16-byte header to determine the
	//    encoding (Sparse vs Dense) and the cached cardinality state.
	//
	// 2. DENSE DECODING
	//    For dense HLLs, the remaining bytes in the slice represent the
	//    raw register array. We allocate a new memory block and perform a
	//    copy. This copy is essential for concurrency: it ensures that
	//    modifications to this new HLL instance do not affect the underlying
	//    slice in the Store until Serialize is called.
	//
	// 3. SPARSE DECODING
	//    For sparse HLLs, we must reconstruct the slice of sparseRegister
	//    structs. We read the 4-byte count prefix to allocate the slice
	//    with the correct capacity, avoiding dynamic resizing during the
	//    loop. Then, we iterate through the packed 3-byte entries to
	//    populate the index and value fields.
	//

	if len(data) < headerSize {
		return nil, errors.New("data too short")
	}

	// Parse Header
	header, err := deserializeHeader(data)
	if err != nil {
		return nil, err
	}

	h := &HLL{
		header:          *header,
		sparseThreshold: DefaultSparseThreshold,
	}

	// Parse Data
	if h.header.encoding == dense {
		h.denseData = make([]byte, len(data)-headerSize)
		copy(h.denseData, data[headerSize:])
	} else {
		offset := headerSize
		if len(data) < offset+4 {
			return nil, errors.New("invalid sparse data")
		}

		count := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		h.sparseData = make([]sparseRegister, count)
		for i := uint32(0); i < count; i++ {
			if offset+3 > len(data) {
				return nil, errors.New("sparse data corrupted")
			}
			idx := binary.LittleEndian.Uint16(data[offset : offset+2])
			val := data[offset+2]
			h.sparseData[i] = sparseRegister{index: idx, value: val}
			offset += 3
		}
	}
	return h, nil
}

// DeserializeUnsafe reconstructs an HLL struct without copying the underlying data.
// WARNING: The resulting HLL is read-only. Modifying it will mutate the input slice,
// which might be shared memory (e.g., from the Store).
// This is an optimization for read-heavy operations like HLL.COUNT and HLL.MERGE.
func DeserializeUnsafe(data []byte) (*HLL, error) {
	if len(data) < headerSize {
		return nil, errors.New("data too short")
	}

	header, err := deserializeHeader(data)
	if err != nil {
		return nil, err
	}

	h := &HLL{
		header:          *header,
		sparseThreshold: DefaultSparseThreshold,
	}

	if h.header.encoding == dense {
		// ZERO-COPY: Point directly to the slice window
		// This avoids allocating 16KB and copying memory.
		h.denseData = data[headerSize:]
	} else {
		// Sparse still requires parsing because it's a list of structs,
		// but sparse HLLs are small so the copy cost is negligible.
		offset := headerSize
		if len(data) < offset+4 {
			return nil, errors.New("invalid sparse data")
		}

		count := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		h.sparseData = make([]sparseRegister, count)
		for i := uint32(0); i < count; i++ {
			if offset+3 > len(data) {
				return nil, errors.New("sparse data corrupted")
			}
			idx := binary.LittleEndian.Uint16(data[offset : offset+2])
			val := data[offset+2]
			h.sparseData[i] = sparseRegister{index: idx, value: val}
			offset += 3
		}
	}
	return h, nil
}

// MergeInto merges the internal register state into an external raw accumulator.
// It implements the standard HLL union logic: M[i] = MAX(M[i], R[i]).
// This helper is used by the multi-key COUNT operation to aggregate partial results.
func (h *HLL) MergeInto(rawRegisters []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.header.encoding == sparse {
		for _, reg := range h.sparseData {
			if reg.value > rawRegisters[reg.index] {
				rawRegisters[reg.index] = reg.value
			}
		}
	} else {
		// Since we use 8-bit registers, we can simply iterate and compare bytes.
		// We use loop unrolling (stride 8) to help the compiler vectorize.

		dense := h.denseData
		raw := rawRegisters

		// Process 8 registers at a time for better cache utilization.
		for i := 0; i < M; i += 8 {
			if dense[i] > raw[i] {
				raw[i] = dense[i]
			}
			if dense[i+1] > raw[i+1] {
				raw[i+1] = dense[i+1]
			}
			if dense[i+2] > raw[i+2] {
				raw[i+2] = dense[i+2]
			}
			if dense[i+3] > raw[i+3] {
				raw[i+3] = dense[i+3]
			}
			if dense[i+4] > raw[i+4] {
				raw[i+4] = dense[i+4]
			}
			if dense[i+5] > raw[i+5] {
				raw[i+5] = dense[i+5]
			}
			if dense[i+6] > raw[i+6] {
				raw[i+6] = dense[i+6]
			}
			if dense[i+7] > raw[i+7] {
				raw[i+7] = dense[i+7]
			}
		}
	}
}
