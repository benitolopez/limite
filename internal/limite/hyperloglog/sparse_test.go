package hyperloglog

import (
	"testing"
)

func TestConvertToDense(t *testing.T) {
	// 1. Arrange: Create a new HLL and manually set it to a sparse state.
	h := New()
	if h.header.encoding != sparse {
		t.Fatal("New() should create an HLL in sparse encoding")
	}

	// The test data is a pre-sorted list of non-zero registers.
	h.sparseData = []sparseRegister{
		{index: 100, value: 5},
		{index: 1000, value: 7},
		{index: 15000, value: 3},
	}

	// 2. Act: Call the function we are testing.
	h.convertToDense()

	// 3. Assert: Verify that the HLL is now in the correct dense state.
	if h.header.encoding != dense {
		t.Errorf("expected encoding to be hllDense, but got %v", h.header.encoding)
	}

	if len(h.denseData) != denseSize {
		t.Errorf("denseData has wrong size: got %d, want %d", len(h.denseData), denseSize)
	}

	if h.sparseData != nil {
		t.Error("sparseData should be nil after converting to dense")
	}

	// Verify that the specific register values were correctly copied.
	if got := h.denseData[100]; got != 5 {
		t.Errorf("register 100: got value %d, want 5", got)
	}
	if got := h.denseData[1000]; got != 7 {
		t.Errorf("register 1000: got value %d, want 7", got)
	}
	if got := h.denseData[15000]; got != 3 {
		t.Errorf("register 15000: got value %d, want 3", got)
	}

	// Verify that other registers remain zero.
	if got := h.denseData[101]; got != 0 {
		t.Errorf("register 101 should be 0, but got %d", got)
	}
}
