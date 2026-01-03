package bloom

import (
	"testing"
	"unsafe"
)

// TestBlockSize ensures the Block struct matches the L1 Cache Line size (64 bytes).
// We use [8]uint64 because 8 * 8 bytes = 64 bytes.
func TestBlockSize(t *testing.T) {
	var b Block
	size := unsafe.Sizeof(b)
	if size != 64 {
		t.Fatalf("Block size mismatch: expected 64 bytes, got %d", size)
	}
}

func TestBlockAdd(t *testing.T) {
	var b Block

	// Construct a hash where h1=8 and h2=8
	// Lower 32 bits = 8, Upper 32 bits = 8
	hash := uint64(8) | (uint64(8) << 32)

	// 1. First Add: Should modify the block and return true
	if !b.Add(hash) {
		t.Errorf("Expected Add to return true for fresh insert")
	}

	// 2. Verify the Bit Pattern
	// We expect 7 bits in Word[0] (indices 8, 16, 24, 32, 40, 48, 56)
	// And 1 bit in Word[1] (index 64, which is bit 0 of the second word)

	expectedWord0 := uint64(0)
	for i := 0; i < 7; i++ {
		expectedWord0 |= (1 << (8 + i*8))
	}

	if b[0] != expectedWord0 {
		t.Errorf("Word[0] incorrect.\nGot:  %064b\nWant: %064b", b[0], expectedWord0)
	}

	if b[1] != 1 {
		t.Errorf("Word[1] incorrect. Expected bit 0 set (value 1), got %d", b[1])
	}

	// 3. Second Add: Should return false (bits already set)
	if b.Add(hash) {
		t.Errorf("Expected Add to return false for duplicate insert")
	}
}

func TestBlockCheck(t *testing.T) {
	var b Block
	hashPresent := uint64(0xAAAA_BBBB_CCCC_DDDD)
	hashAbsent := uint64(0x1111_2222_3333_4444)

	// 1. Check empty block -> Should be False
	if b.Check(hashPresent) {
		t.Error("Check returned true on empty block")
	}

	// 2. Add item
	b.Add(hashPresent)

	// 3. Check present item -> Should be True
	if !b.Check(hashPresent) {
		t.Error("Check returned false for item just added")
	}

	// 4. Check absent item -> Should be False
	// (With only 8 bits set out of 512, the probability of collision is microscopic)
	if b.Check(hashAbsent) {
		t.Error("Check returned true for absent item (false positive)")
	}
}
