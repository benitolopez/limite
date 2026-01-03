package hyperloglog

import (
	"fmt"
	"math"
	"sort"
	"testing"
)

// TestAdd verifies the Add operation works correctly in both sparse and dense modes.
// This comprehensive test covers all aspects of adding items to an HLL, including
// insertion, updates, sort order, cache invalidation, and sparse-to-dense conversion.
func TestAdd(t *testing.T) {
	// Helper function to count non-zero registers in dense mode
	countNonZeroRegisters := func(denseData []byte) int {
		count := 0
		for i := uint64(0); i < M; i++ {
			if denseData[i] > 0 {
				count++
			}
		}
		return count
	}

	// ========================================================================
	// SPARSE MODE TESTS
	// ========================================================================

	t.Run("sparse: insert into empty", func(t *testing.T) {
		h := New()
		if h.header.encoding != sparse {
			t.Fatal("New() HLL should be in sparse mode")
		}

		h.Add([]byte("test-item"))

		// Verify exactly one register was set
		if len(h.sparseData) != 1 {
			t.Errorf("expected exactly 1 non-zero register, got %d", len(h.sparseData))
		}

		// Verify the rank is valid
		if len(h.sparseData) > 0 {
			rank := h.sparseData[0].value
			if rank < 1 || rank > 51 {
				t.Errorf("expected rank in range [1, 51], got %d", rank)
			}
		}

		// Verify cache was invalidated
		if !h.header.cacheInvalid {
			t.Error("cache should be invalid after Add")
		}
	})

	t.Run("sparse: adding duplicate doesn't increase size", func(t *testing.T) {
		h := New()

		// Add the same item multiple times
		h.Add([]byte("duplicate-test"))
		firstSize := len(h.sparseData)
		firstRank := h.sparseData[0].value

		h.Add([]byte("duplicate-test"))

		// The size should remain the same (no new register created)
		if len(h.sparseData) != firstSize {
			t.Errorf("expected size to remain %d after duplicate add, got %d", firstSize, len(h.sparseData))
		}

		// The rank should be the same (same hash = same rank)
		if h.sparseData[0].value != firstRank {
			t.Errorf("expected rank to remain %d after duplicate add, got %d", firstRank, h.sparseData[0].value)
		}
	})

	t.Run("sparse: update existing register with higher rank", func(t *testing.T) {
		h := New()

		// Add an item and capture its hash
		testHLL := New()
		testHLL.Add([]byte("test-item-for-collision"))
		targetIndex := testHLL.sparseData[0].index
		targetRank := testHLL.sparseData[0].value

		// Set up our test HLL with a lower rank at the same index
		h.sparseData = []sparseRegister{{index: targetIndex, value: 1}}
		h.header.cacheInvalid = false

		// Add the same item (which should update the rank from 1 to targetRank)
		h.Add([]byte("test-item-for-collision"))

		// Verify the rank was updated
		if len(h.sparseData) != 1 {
			t.Fatalf("expected sparseData to still have 1 element, but got %d", len(h.sparseData))
		}

		if h.sparseData[0].value != targetRank {
			t.Errorf("expected rank to be updated to %d, got %d", targetRank, h.sparseData[0].value)
		}

		// Verify cache was invalidated
		if !h.header.cacheInvalid {
			t.Error("cache should be invalid after updating a register")
		}
	})

	t.Run("sparse: insert and maintain sort order", func(t *testing.T) {
		h := New()
		// Manually set up some sparse registers
		h.sparseData = []sparseRegister{
			{index: 100, value: 5},
			{index: 1000, value: 7},
			{index: 5000, value: 3},
		}

		// Add several different items
		testItems := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
		for _, item := range testItems {
			h.Add([]byte(item))
		}

		// Verify that the slice is still sorted by index
		isSorted := sort.SliceIsSorted(h.sparseData, func(i, j int) bool {
			return h.sparseData[i].index < h.sparseData[j].index
		})

		if !isSorted {
			t.Errorf("sparseData slice is not sorted by index after insertions")
			// Log the actual order for debugging
			for i, reg := range h.sparseData {
				t.Logf("  [%d] index=%d, value=%d", i, reg.index, reg.value)
			}
		}

		// Verify that all indices are unique
		seenIndices := make(map[uint16]bool)
		for _, reg := range h.sparseData {
			if seenIndices[reg.index] {
				t.Errorf("duplicate index %d found in sparseData", reg.index)
			}
			seenIndices[reg.index] = true
		}
	})

	// ========================================================================
	// DENSE MODE TESTS
	// ========================================================================

	t.Run("dense: add single item", func(t *testing.T) {
		h := New()
		// Force conversion to dense mode
		h.convertToDense()

		if h.header.encoding != dense {
			t.Fatal("HLL should be in dense mode")
		}

		h.Add([]byte("test-item"))

		// Count non-zero registers
		nonZeroCount := countNonZeroRegisters(h.denseData)
		if nonZeroCount != 1 {
			t.Errorf("expected exactly 1 non-zero register in dense mode, got %d", nonZeroCount)
		}

		// Verify cache was invalidated
		if !h.header.cacheInvalid {
			t.Error("cache should be invalid after Add")
		}
	})

	t.Run("dense: add duplicate item", func(t *testing.T) {
		h := New()
		h.convertToDense()

		// Add the same item multiple times
		h.Add([]byte("duplicate"))
		firstCount := countNonZeroRegisters(h.denseData)

		h.Add([]byte("duplicate"))
		h.Add([]byte("duplicate"))

		secondCount := countNonZeroRegisters(h.denseData)

		// Count should remain the same
		if firstCount != secondCount {
			t.Errorf("register count should remain %d after duplicates, got %d", firstCount, secondCount)
		}

		// Should have exactly 1 register set
		if secondCount != 1 {
			t.Errorf("expected 1 register after adding duplicates, got %d", secondCount)
		}
	})

	t.Run("dense: add multiple unique items", func(t *testing.T) {
		h := New()
		h.convertToDense()

		items := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
		for _, item := range items {
			h.Add([]byte(item))
		}

		nonZeroCount := countNonZeroRegisters(h.denseData)

		// Should have at least some registers set
		if nonZeroCount == 0 {
			t.Error("expected at least some non-zero registers")
		}

		// Should have at most len(items) registers (if no collisions)
		if nonZeroCount > len(items) {
			t.Errorf("expected at most %d registers, got %d", len(items), nonZeroCount)
		}
	})

	t.Run("dense: verify register update with higher rank", func(t *testing.T) {
		h := New()
		h.convertToDense()

		// Add an item and capture which register it sets
		testHLL := New()
		testHLL.Add([]byte("test-collision"))
		targetIndex := testHLL.sparseData[0].index
		targetRank := testHLL.sparseData[0].value

		// Manually set a lower rank in our dense HLL (ensure it's lower than the natural rank)
		lowerRank := uint8(1)
		if targetRank <= lowerRank {
			// If natural rank is 1, we can't test with a lower value, skip this scenario
			lowerRank = 0 // This will ensure the register gets updated
		}
		h.denseData[uint64(targetIndex)] = lowerRank
		h.header.cacheInvalid = false

		// Add the same item - should update to the higher rank
		h.Add([]byte("test-collision"))

		// Verify the rank was updated to at least the natural rank
		actualRank := h.denseData[uint64(targetIndex)]
		if actualRank != targetRank {
			t.Errorf("expected rank to be updated to %d, got %d", targetRank, actualRank)
		}

		// Verify cache was invalidated (should always happen when rank changes)
		if targetRank > lowerRank && !h.header.cacheInvalid {
			t.Error("cache should be invalid after updating a register")
		}
	})

	t.Run("dense: register doesn't decrease", func(t *testing.T) {
		h := New()
		h.convertToDense()

		// Add an item to get its natural rank
		h.Add([]byte("test-item"))

		// Find the non-zero register
		var registerIndex uint64
		var originalRank uint8
		for i := uint64(0); i < M; i++ {
			rank := h.denseData[i]
			if rank > 0 {
				registerIndex = i
				originalRank = rank
				break
			}
		}

		// Manually increase the rank
		higherRank := originalRank + 5
		if higherRank > registerMax {
			higherRank = registerMax
		}
		h.denseData[registerIndex] = higherRank

		// Add the same item again
		h.Add([]byte("test-item"))

		// Verify the rank didn't decrease
		finalRank := h.denseData[registerIndex]
		if finalRank < higherRank {
			t.Errorf("rank should not decrease: was %d, became %d", higherRank, finalRank)
		}
	})

	// ========================================================================
	// SPARSE-TO-DENSE CONVERSION TESTS
	// ========================================================================

	t.Run("conversion: trigger when threshold exceeded", func(t *testing.T) {
		h := New()

		// Keep adding items until we trigger the conversion to dense.
		// Due to hash collisions, we can't predict exactly how many items
		// we need to add, so we'll keep going until the conversion happens.
		maxIterations := DefaultSparseThreshold * 2 // Safety limit
		var itemsAdded int

		for itemsAdded = 0; itemsAdded < maxIterations; itemsAdded++ {
			// Check if we've already converted (this happens when sparseData exceeds threshold)
			if h.header.encoding == dense {
				break
			}

			item := fmt.Sprintf("unique-item-%d", itemsAdded)
			h.Add([]byte(item))

			// Verify we don't convert before reaching the threshold
			if h.header.encoding == dense && len(h.sparseData) == 0 {
				// Conversion happened - verify the previous state had enough items
				// Note: sparseData is nil after conversion, so we can't check it directly
				break
			}
		}

		// Verify conversion actually happened
		if h.header.encoding != dense {
			t.Errorf("expected encoding to be hllDense after %d items, but still sparse", itemsAdded)
			t.Logf("Final sparse item count: %d (Threshold is %d)", len(h.sparseData), DefaultSparseThreshold)
		}

		if h.denseData == nil {
			t.Error("denseData should not be nil after conversion")
		}

		if h.sparseData != nil {
			t.Error("sparseData should be nil after conversion")
		}

		t.Logf("Converted to dense after adding %d items", itemsAdded)
	})

	t.Run("conversion: no conversion if under threshold", func(t *testing.T) {
		h := New()

		// Manually populate with fewer items than the threshold
		itemsToAdd := DefaultSparseThreshold / 2
		for i := 0; i < itemsToAdd; i++ {
			item := fmt.Sprintf("item-%d", i)
			h.Add([]byte(item))
		}

		// Should still be in sparse mode
		if h.header.encoding != sparse {
			t.Errorf("expected encoding to remain hllSparse with %d items, but got dense", itemsToAdd)
		}

		if len(h.sparseData) == 0 {
			t.Error("sparseData should not be empty")
		}
	})

	t.Run("conversion: preserves data correctly", func(t *testing.T) {
		h := New()

		// Add some items in sparse mode
		testItems := []string{"item1", "item2", "item3"}
		for _, item := range testItems {
			h.Add([]byte(item))
		}

		// Capture sparse state
		sparseCount := len(h.sparseData)
		sparseRegisters := make(map[uint16]uint8)
		for _, reg := range h.sparseData {
			sparseRegisters[reg.index] = reg.value
		}

		// Force conversion to dense
		h.convertToDense()

		// Verify all sparse registers were copied correctly
		for index, value := range sparseRegisters {
			actualValue := h.denseData[uint64(index)]
			if actualValue != value {
				t.Errorf("register %d: expected value %d, got %d", index, value, actualValue)
			}
		}

		// Verify register count matches
		denseCount := countNonZeroRegisters(h.denseData)
		if denseCount != sparseCount {
			t.Errorf("expected %d non-zero registers after conversion, got %d", sparseCount, denseCount)
		}

		// Verify we can still add items after conversion
		h.Add([]byte("new-item-after-conversion"))

		newDenseCount := countNonZeroRegisters(h.denseData)
		if newDenseCount < denseCount {
			t.Error("adding items after conversion should not decrease register count")
		}
	})
}

// TestCount verifies the Count operation works correctly in both sparse and dense modes,
// tests cache behavior, and validates accuracy across different cardinalities.
func TestCount(t *testing.T) {
	t.Run("empty HLL returns zero", func(t *testing.T) {
		h := New()
		cardinality := h.Count()

		if cardinality != 0 {
			t.Errorf("expected empty HLL to have cardinality 0, got %d", cardinality)
		}

		// Verify cache is now valid
		if h.header.cacheInvalid {
			t.Error("cache should be valid after Count()")
		}
	})

	t.Run("single item", func(t *testing.T) {
		h := New()
		h.Add([]byte("single-item"))
		cardinality := h.Count()

		// For a single item, estimate should be close to 1
		if cardinality == 0 {
			t.Error("expected non-zero cardinality for single item")
		}

		if cardinality > 10 {
			t.Errorf("expected cardinality close to 1, got %d", cardinality)
		}

		t.Logf("Single item cardinality estimate: %d", cardinality)
	})

	t.Run("small cardinality in sparse mode", func(t *testing.T) {
		h := New()
		numItems := 100

		for i := 0; i < numItems; i++ {
			item := fmt.Sprintf("item-%d", i)
			h.Add([]byte(item))
		}

		// Should still be in sparse mode
		if h.header.encoding != sparse {
			t.Error("expected HLL to remain in sparse mode for small cardinality")
		}

		cardinality := h.Count()

		// For p=14, standard error is ~0.81%. We'll allow 5% error for small cardinalities
		expectedErrorMargin := 0.05
		error := math.Abs(float64(cardinality) - float64(numItems))
		relativeError := error / float64(numItems)

		t.Logf("Small cardinality: True=%d, Estimated=%d, Error=%.2f%%", numItems, cardinality, relativeError*100)

		if relativeError > expectedErrorMargin {
			t.Errorf("error too high: got %.4f, want less than %.4f", relativeError, expectedErrorMargin)
		}
	})

	t.Run("medium cardinality triggering conversion", func(t *testing.T) {
		h := New()
		numItems := 10000

		for i := 0; i < numItems; i++ {
			item := fmt.Sprintf("item-%d", i)
			h.Add([]byte(item))
		}

		// Should have converted to dense mode
		if h.header.encoding != dense {
			t.Error("expected HLL to convert to dense mode for medium cardinality")
		}

		cardinality := h.Count()

		// For p=14, we expect ~0.81% standard error. We'll test for < 2%
		expectedErrorMargin := 0.02
		error := math.Abs(float64(cardinality) - float64(numItems))
		relativeError := error / float64(numItems)

		t.Logf("Medium cardinality: True=%d, Estimated=%d, Error=%.2f%%", numItems, cardinality, relativeError*100)

		if relativeError > expectedErrorMargin {
			t.Errorf("error too high: got %.4f, want less than %.4f", relativeError, expectedErrorMargin)
		}
	})

	t.Run("large cardinality in dense mode", func(t *testing.T) {
		h := New()
		numItems := 100000

		for i := 0; i < numItems; i++ {
			item := fmt.Sprintf("item-%d", i)
			h.Add([]byte(item))
		}

		if h.header.encoding != dense {
			t.Fatal("expected HLL to be in dense mode for large cardinality")
		}

		cardinality := h.Count()

		// For p=14, standard error is ~0.81%. We'll test for < 2%
		expectedErrorMargin := 0.02
		error := math.Abs(float64(cardinality) - float64(numItems))
		relativeError := error / float64(numItems)

		t.Logf("Large cardinality: True=%d, Estimated=%d, Error=%.2f%%", numItems, cardinality, relativeError*100)

		if relativeError > expectedErrorMargin {
			t.Errorf("error too high: got %.4f, want less than %.4f", relativeError, expectedErrorMargin)
		}
	})

	t.Run("cache behavior: multiple calls return same value", func(t *testing.T) {
		h := New()

		// Add some items
		for i := 0; i < 1000; i++ {
			h.Add([]byte(fmt.Sprintf("item-%d", i)))
		}

		// First call should compute and cache
		firstCount := h.Count()
		if h.header.cacheInvalid {
			t.Error("cache should be valid after Count()")
		}

		// Second call should return cached value
		secondCount := h.Count()
		if firstCount != secondCount {
			t.Errorf("cached Count() should return same value: first=%d, second=%d", firstCount, secondCount)
		}

		// Add more items - cache should be invalidated
		h.Add([]byte("new-item"))
		if !h.header.cacheInvalid {
			t.Error("cache should be invalid after Add()")
		}

		// Count should recompute
		thirdCount := h.Count()
		if thirdCount < firstCount {
			t.Errorf("count should not decrease after adding items: was %d, became %d", firstCount, thirdCount)
		}
	})

	t.Run("duplicates don't affect cardinality", func(t *testing.T) {
		h := New()

		// Add 100 unique items
		for i := 0; i < 100; i++ {
			h.Add([]byte(fmt.Sprintf("item-%d", i)))
		}

		firstCount := h.Count()

		// Add the same 100 items again (duplicates)
		for i := 0; i < 100; i++ {
			h.Add([]byte(fmt.Sprintf("item-%d", i)))
		}

		secondCount := h.Count()

		// Cardinality should remain approximately the same
		if math.Abs(float64(secondCount)-float64(firstCount)) > 5 {
			t.Errorf("duplicates should not significantly change cardinality: before=%d, after=%d", firstCount, secondCount)
		}

		t.Logf("Cardinality with duplicates: before=%d, after=%d", firstCount, secondCount)
	})
}

func TestMergeInto(t *testing.T) {
	// Helper to reset the accumulator
	makeAccumulator := func() []byte {
		// Use a simple byte array (one byte per register) for the merge accumulator
		// because memory compactness doesn't matter for this temporary buffer.
		return make([]byte, M)
	}

	t.Run("Merge Sparse into Accumulator", func(t *testing.T) {
		// 1. Setup Accumulator with some pre-existing data
		raw := makeAccumulator()
		raw[100] = 5 // Index 100 has rank 5

		// 2. Setup a Sparse HLL
		h := New()
		// We manually manipulate sparseData to avoid hashing randomness for this test
		h.sparseData = []sparseRegister{
			{index: 100, value: 3}, // Lower than accumulator (should be ignored)
			{index: 200, value: 7}, // New value (should be added)
		}

		// 3. Perform Merge
		h.MergeInto(raw)

		// 4. Assertions
		if raw[100] != 5 {
			t.Errorf("Index 100: expected 5 (preserved max), got %d", raw[100])
		}
		if raw[200] != 7 {
			t.Errorf("Index 200: expected 7 (new max), got %d", raw[200])
		}
		if raw[300] != 0 {
			t.Errorf("Index 300: expected 0 (untouched), got %d", raw[300])
		}
	})

	t.Run("Merge Dense into Accumulator", func(t *testing.T) {
		// 1. Setup Accumulator
		raw := makeAccumulator()
		raw[100] = 5

		// 2. Setup a Dense HLL
		h := New()
		h.convertToDense() // Force dense mode
		// Manually set registers in the packed dense representation
		h.denseData[100] = 3 // Lower (ignore)
		h.denseData[200] = 7 // Higher (update)

		// 3. Perform Merge
		h.MergeInto(raw)

		// 4. Assertions
		if raw[100] != 5 {
			t.Errorf("Index 100: expected 5 (preserved max), got %d", raw[100])
		}
		if raw[200] != 7 {
			t.Errorf("Index 200: expected 7 (new max), got %d", raw[200])
		}
	})
}

func TestHLLCountRaw(t *testing.T) {
	// 1. Setup a standard HLL to get a baseline truth
	hStandard := New()
	items := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, item := range items {
		hStandard.Add([]byte(item))
	}
	expected := hStandard.Count()

	// 2. Setup a "Raw" HLL manually
	// This simulates what the Handler will do: create a temp HLL and fill it.
	rawAccumulator := make([]byte, M)
	hStandard.MergeInto(rawAccumulator)

	hRaw := &HLL{
		header: hllHeader{
			encoding:     raw,
			cacheInvalid: true,
		},
		denseData: rawAccumulator, // 16KB unpacked
	}

	// 3. Call Count() on the Raw HLL
	got := hRaw.Count()

	if got != expected {
		t.Errorf("Raw HLL Count mismatch: got %d, want %d", got, expected)
	}
}
