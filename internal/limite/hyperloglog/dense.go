package hyperloglog

// denseAdd incorporates a new item into a dense HyperLogLog estimate.
// It is not thread-safe; the caller must handle locking.
//
// This function uses the hashToIndexAndRank helper to determine the target
// register and rank. It then checks the register's current value and updates
// it only if the new rank is greater. If an update occurs, it invalidates
// the cached cardinality in the header.
func (h *HLL) denseAdd(data []byte) bool {
	// Calculate the register index and rank from the input data.
	registerIndex, rank := hashToIndexAndRank(data)

	// Read the current value from the register and update it only if the new
	// rank is greater.
	currentValue := h.denseData[registerIndex]
	if rank > currentValue {
		h.denseData[registerIndex] = rank

		// If we have updated a register, the previously cached cardinality
		// is now incorrect. We must mark the cache as invalid.
		h.header.cacheInvalid = true

		return true
	}

	return false
}

// getDenseHisto builds a histogram of register values from a dense
// representation. It iterates over all 16,384 registers, and
// counts the occurrences of each rank.
func (h *HLL) getDenseHisto() []int {
	regHisto := make([]int, 64)

	// Iterate through all registers and build a histogram.
	for i := uint64(0); i < M; i++ {
		value := h.denseData[i]
		regHisto[value]++
	}

	return regHisto
}
