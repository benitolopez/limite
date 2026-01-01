package hyperloglog

import (
	"sort"
)

type sparseRegister struct {
	index uint16
	value uint8
}

// convertToDense transforms the HLL from a sparse to a dense representation.
// This is a one-way operation that is triggered when a sparse HLL grows
// beyond a certain memory threshold.
//
// The function is not thread-safe by itself; the caller must hold an
// exclusive write lock.
func (h *HLL) convertToDense() {
	newDenseData := make([]byte, denseSize)

	// Copy each sparse register value into the corresponding byte position
	// in the dense array. With 8-bit registers, this is a direct assignment.
	for _, pair := range h.sparseData {
		newDenseData[uint64(pair.index)] = pair.value
	}

	h.denseData = newDenseData
	h.sparseData = nil // Free the memory from the old slice.
	h.header.encoding = dense
}

func (h *HLL) sparseAdd(data []byte) bool {
	// Calculate the register index and rank from the input data.
	registerIndex, rank := hashToIndexAndRank(data)

	index := uint16(registerIndex)

	i := sort.Search(len(h.sparseData), func(i int) bool {
		// This compares the index at the current position with
		// the target registerIndex we are looking for.
		return h.sparseData[i].index >= index
	})

	var updated bool

	if i < len(h.sparseData) && h.sparseData[i].index == index {
		currentValue := h.sparseData[i].value

		if rank > currentValue {
			h.sparseData[i].value = rank
			updated = true
		}
	} else {
		newPair := sparseRegister{index: index, value: rank}

		// Optimized insertion using the standard Go slice insert idiom:
		// 1. Grow the slice by one (reuses capacity if available)
		// 2. Shift elements right using copy (maps to optimized memmove)
		// 3. Assign the new value at the insertion point
		// This avoids allocating a temporary slice for the variadic append.
		h.sparseData = append(h.sparseData, sparseRegister{})
		copy(h.sparseData[i+1:], h.sparseData[i:])
		h.sparseData[i] = newPair

		updated = true
	}

	// If we have updated a register, the previously cached cardinality
	// is now incorrect. We must mark the cache as invalid.
	if updated {
		h.header.cacheInvalid = true

		if len(h.sparseData) > h.sparseThreshold {
			// If it has exceeded, immediately convert to the dense representation.
			h.convertToDense()
		}
	}

	return updated
}

// getSparseHisto builds a histogram of register values from a sparse
// representation. It iterates over the list of non-zero registers and
// infers the count of zeroed registers to construct a complete histogram.
func (h *HLL) getSparseHisto() []int {
	regHisto := make([]int, 64)

	// Iterate through all the non-zero registers.
	for _, pair := range h.sparseData {
		regHisto[pair.value]++
	}

	// Registers that are not in our sparse list have a rank of 0.
	numNonZero := len(h.sparseData)
	numZeros := M - numNonZero
	regHisto[0] = numZeros

	return regHisto
}
