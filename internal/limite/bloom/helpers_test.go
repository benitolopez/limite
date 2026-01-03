package bloom

import (
	"testing"
)

func TestEstimateParameters(t *testing.T) {
	// Case 1: Small capacity
	// n=100, p=0.01
	// m = -100 * ln(0.01) / (ln(2)^2) ~= 958 bits
	// Bytes = 119.75
	// Align 64 -> 128 bytes (2 blocks)
	size, k := EstimateParameters(100, 0.01)

	if size != 128 {
		t.Errorf("Expected 128 bytes, got %d", size)
	}
	if k != 8 {
		t.Errorf("Expected k=8 (fixed), got %d", k)
	}

	// Case 2: Large capacity
	// n=10000, p=0.01
	// m ~= 95850 bits ~= 11981 bytes
	// 11981 / 64 = 187.2 -> Round up to 188 blocks
	// 188 * 64 = 12032 bytes
	size, _ = EstimateParameters(10000, 0.01)
	if size != 12032 {
		t.Errorf("Expected 12032 bytes, got %d", size)
	}
}
