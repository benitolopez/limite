package bloom

type Block [8]uint64

// Add sets the bits corresponding to the hashed key within this specific block.
// It returns true if at least one bit was flipped from 0 to 1 (indicating a
// state change that may need persistence), or false if all bits were already set.
func (b *Block) Add(hash uint64) bool {
	//
	// DESIGN
	// ------
	//
	// This method implements insertion logic for a Cache-Line Blocked Bloom Filter
	// with a fixed k=8. Standard Bloom Filters scatter bits across the entire
	// bitset, causing k cache misses per insertion. Here, we restrict all k bits
	// to reside within this single 64-byte block (512 bits), which aligns with
	// the L1 Cache Line size of modern CPUs. Once this block is loaded from
	// memory, all 8 bit-set operations occur entirely in CPU registers.
	//
	// To avoid computing k=8 distinct hash functions, we use the Kirsch-Mitzenmacher
	// optimization from "Less hashing, same performance: Building a better Bloom
	// filter". We split the 64-bit input hash into two 32-bit components (h1 and
	// h2) and simulate k independent hashes using g_i(x) = (h1 + i * h2) mod 512.
	// This has been mathematically proven to have negligible impact on the False
	// Positive Rate while being significantly faster.
	//
	// The loop is deliberately unrolled to enable instruction-level parallelism.
	// Modern superscalar CPUs can execute independent instructions in parallel,
	// and unrolling removes loop overhead while allowing the CPU to calculate
	// multiple pointers and masks simultaneously.
	//
	// The bitwise arithmetic uses pos = (h1 + i*h2) & 511 as a fast modulo 512,
	// wordIdx = pos >> 6 for division by 64 (selecting one of the 8 uint64s),
	// and bitIdx = pos & 63 for modulo 64 (selecting the bit inside the uint64).
	//

	h1 := uint32(hash)
	h2 := uint32(hash >> 32)

	// Track if any bit was actually changed from 0 to 1.
	// This is critical for the upper layers to know if the filter state
	// has mutated (requiring a disk write or count increment).
	changed := false

	var pos uint32
	var wordIdx uint32
	var mask uint64

	// --- Bit 0 (i=0) ---
	// Optimization: 0 * h2 is 0, so we just use h1.
	pos = h1 & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		b[wordIdx] |= mask
		changed = true
	}

	// --- Bit 1 (i=1) ---
	pos = (h1 + h2) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		b[wordIdx] |= mask
		changed = true
	}

	// --- Bit 2 (i=2) ---
	// Optimization: h2 << 1 is equivalent to h2 * 2 but explicit shifts
	// are sometimes preferred by the compiler for strength reduction.
	pos = (h1 + (h2 << 1)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		b[wordIdx] |= mask
		changed = true
	}

	// --- Bit 3 (i=3) ---
	pos = (h1 + (h2 * 3)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		b[wordIdx] |= mask
		changed = true
	}

	// --- Bit 4 (i=4) ---
	pos = (h1 + (h2 << 2)) & 511 // h2 * 4
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		b[wordIdx] |= mask
		changed = true
	}

	// --- Bit 5 (i=5) ---
	pos = (h1 + (h2 * 5)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		b[wordIdx] |= mask
		changed = true
	}

	// --- Bit 6 (i=6) ---
	pos = (h1 + (h2 * 6)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		b[wordIdx] |= mask
		changed = true
	}

	// --- Bit 7 (i=7) ---
	pos = (h1 + (h2 * 7)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		b[wordIdx] |= mask
		changed = true
	}

	return changed
}

// Check tests whether the element (represented by its hash) is present in the
// block. Returns true if the element is probably present (subject to False
// Positive Rate), or false if definitely not present (True Negative).
func (b *Block) Check(hash uint64) bool {
	//
	// DESIGN
	// ------
	//
	// Unlike Add() which must set all k=8 bits, Check() returns false immediately
	// upon encountering the first unset bit. Since Bloom Filters are primarily
	// used to filter out non-existent items (negative lookups), this ensures
	// that most queries return in significantly fewer than 8 checks.
	//
	// If all k bits are set to 1, we return true. This does not guarantee
	// existence; it only guarantees that the bit pattern exists. This is the
	// source of the False Positive Rate.
	//

	h1 := uint32(hash)
	h2 := uint32(hash >> 32)

	var pos uint32
	var wordIdx uint32
	var mask uint64

	// --- Bit 0 (i=0) ---
	// Optimization: 0 * h2 is 0, so we just use h1.
	pos = h1 & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		return false
	}

	// --- Bit 1 (i=1) ---
	pos = (h1 + h2) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		return false
	}

	// --- Bit 2 (i=2) ---
	// Optimization: h2 << 1 is equivalent to h2 * 2 but explicit shifts
	// are sometimes preferred by the compiler for strength reduction.
	pos = (h1 + (h2 << 1)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		return false
	}

	// --- Bit 3 (i=3) ---
	pos = (h1 + (h2 * 3)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		return false
	}

	// --- Bit 4 (i=4) ---
	pos = (h1 + (h2 << 2)) & 511 // h2 * 4
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		return false
	}

	// --- Bit 5 (i=5) ---
	pos = (h1 + (h2 * 5)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		return false
	}

	// --- Bit 6 (i=6) ---
	pos = (h1 + (h2 * 6)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	if b[wordIdx]&mask == 0 {
		return false
	}

	// --- Bit 7 (i=7) ---
	pos = (h1 + (h2 * 7)) & 511
	wordIdx = pos >> 6
	mask = uint64(1) << (pos & 63)
	return b[wordIdx]&mask != 0
}
