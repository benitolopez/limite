package cms

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	width, depth := uint32(100), uint32(5)
	c := New(width, depth)

	// Verify dimensions
	if c.Width() != width {
		t.Errorf("Width: got %d, want %d", c.Width(), width)
	}
	if c.Depth() != depth {
		t.Errorf("Depth: got %d, want %d", c.Depth(), depth)
	}
	if c.Count() != 0 {
		t.Errorf("Count: got %d, want 0", c.Count())
	}

	// Verify backing slice size
	expectedSize := HeaderSize + int(width)*int(depth)*4
	if len(c.Bytes()) != expectedSize {
		t.Errorf("Bytes length: got %d, want %d", len(c.Bytes()), expectedSize)
	}

	// Verify magic
	if !HasValidMagic(c.Bytes()) {
		t.Error("HasValidMagic returned false for valid CMS")
	}
}

func TestNewFromBytes(t *testing.T) {
	// Create a CMS and get its bytes
	original := New(50, 3)
	original.Incr([]byte("test"), 10)

	// Load from bytes (zero-copy)
	loaded, err := NewFromBytes(original.Bytes())
	if err != nil {
		t.Fatalf("NewFromBytes failed: %v", err)
	}

	// Verify dimensions match
	if loaded.Width() != original.Width() {
		t.Errorf("Width mismatch: got %d, want %d", loaded.Width(), original.Width())
	}
	if loaded.Depth() != original.Depth() {
		t.Errorf("Depth mismatch: got %d, want %d", loaded.Depth(), original.Depth())
	}
	if loaded.Count() != original.Count() {
		t.Errorf("Count mismatch: got %d, want %d", loaded.Count(), original.Count())
	}

	// Verify query returns same value
	if loaded.Query([]byte("test")) != original.Query([]byte("test")) {
		t.Error("Query mismatch after loading from bytes")
	}
}

func TestNewFromBytes_Errors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{
			name: "too short",
			data: make([]byte, HeaderSize-1),
			want: ErrInvalidData,
		},
		{
			name: "wrong magic",
			data: func() []byte {
				d := make([]byte, HeaderSize+100)
				binary.LittleEndian.PutUint32(d[0:4], 0xDEADBEEF)
				return d
			}(),
			want: ErrInvalidMagic,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFromBytes(tt.data)
			if err != tt.want {
				t.Errorf("got error %v, want %v", err, tt.want)
			}
		})
	}
}

func TestHasValidMagic(t *testing.T) {
	// Valid CMS
	c := New(10, 3)
	if !HasValidMagic(c.Bytes()) {
		t.Error("HasValidMagic returned false for valid CMS")
	}

	// Too short
	if HasValidMagic([]byte{1, 2, 3}) {
		t.Error("HasValidMagic returned true for too-short data")
	}

	// Wrong magic
	wrongMagic := make([]byte, 20)
	if HasValidMagic(wrongMagic) {
		t.Error("HasValidMagic returned true for wrong magic")
	}
}

func TestIncr_Basic(t *testing.T) {
	c := New(100, 5)

	// First increment
	changed := c.Incr([]byte("apple"), 1)
	if !changed {
		t.Error("First Incr should return true")
	}

	if got := c.Query([]byte("apple")); got != 1 {
		t.Errorf("Query after first Incr: got %d, want 1", got)
	}

	// Second increment
	changed = c.Incr([]byte("apple"), 1)
	if !changed {
		t.Error("Second Incr should return true")
	}

	if got := c.Query([]byte("apple")); got != 2 {
		t.Errorf("Query after second Incr: got %d, want 2", got)
	}

	// Check total count
	if c.Count() != 2 {
		t.Errorf("Total Count: got %d, want 2", c.Count())
	}
}

func TestIncr_ZeroDelta(t *testing.T) {
	c := New(100, 5)

	// Delta of 0 should be a no-op
	changed := c.Incr([]byte("test"), 0)
	if changed {
		t.Error("Incr with delta=0 should return false")
	}

	if c.Count() != 0 {
		t.Errorf("Count should be 0 after delta=0 Incr, got %d", c.Count())
	}
}

func TestIncr_LargeDelta(t *testing.T) {
	c := New(100, 5)

	// Large delta
	c.Incr([]byte("item"), 1000)
	if got := c.Query([]byte("item")); got != 1000 {
		t.Errorf("Query after large delta: got %d, want 1000", got)
	}
}

func TestQuery_NonExistent(t *testing.T) {
	c := New(100, 5)

	// Query for item that was never added should return 0
	if got := c.Query([]byte("nonexistent")); got != 0 {
		t.Errorf("Query for nonexistent item: got %d, want 0", got)
	}
}

func TestConservativeUpdate(t *testing.T) {
	// Create a small CMS to test Conservative Update behavior
	// Width 10, Depth 3 makes it easy to reason about
	c := New(10, 3)

	item := []byte("apple")

	// First increment
	c.Incr(item, 1)
	if got := c.Query(item); got != 1 {
		t.Errorf("Initial query: got %d, want 1", got)
	}

	// Now we'll manually simulate a "heavy hitter" collision scenario.
	// We artificially inflate one of the counters that 'apple' hashes to.
	//
	// Current state for 'apple': all rows have counter = 1
	// We'll set row 0's counter to 100 to simulate a heavy hitter collision.
	//
	// After Incr(apple, 1) with Conservative Update:
	// - min across rows = 1 (rows 1 and 2 still at 1)
	// - targetVal = 1 + 1 = 2
	// - Row 0: current=100, 100 < 2 is false, NO update
	// - Row 1: current=1, 1 < 2 is true, UPDATE to 2
	// - Row 2: current=1, 1 < 2 is true, UPDATE to 2
	// Result: [100, 2, 2], Query returns min = 2

	// Get the hash values and index for row 0
	h1, h2 := c.getHashes(item)
	idx0 := c.getIndex(0, h1, h2)

	// Artificially set row 0's counter to 100
	offset := HeaderSize + (uint64(0)*uint64(c.Width())+uint64(idx0))*4
	binary.LittleEndian.PutUint32(c.backing[offset:], 100)

	// Now perform another increment
	c.Incr(item, 1)

	// Check that row 0's counter is still 100 (not 101)
	// This proves Conservative Update is working
	val0 := binary.LittleEndian.Uint32(c.backing[offset:])
	if val0 != 100 {
		t.Errorf("Conservative update failed: row 0 counter is %d, want 100", val0)
	}

	// Query should return 2 (the minimum across all rows)
	if got := c.Query(item); got != 2 {
		t.Errorf("Query after CU: got %d, want 2", got)
	}
}

func TestMultipleItems(t *testing.T) {
	c := New(1000, 5)

	// Add multiple items with different counts
	items := map[string]uint32{
		"apple":  5,
		"banana": 10,
		"cherry": 3,
		"date":   7,
	}

	for item, count := range items {
		c.Incr([]byte(item), count)
	}

	// Verify each item's count
	for item, expectedCount := range items {
		got := c.Query([]byte(item))
		// Due to potential collisions, got >= expected
		if got < expectedCount {
			t.Errorf("Query(%q): got %d, want >= %d", item, got, expectedCount)
		}
	}
}

func TestDimensionsFromProb(t *testing.T) {
	tests := []struct {
		epsilon     float64
		delta       float64
		wantWidth   uint32
		wantDepth   uint32
		description string
	}{
		{
			epsilon:     0.01,
			delta:       0.01,
			wantWidth:   272, // ceil(e / 0.01) = ceil(271.83) = 272
			wantDepth:   5,   // ceil(ln(100)) = ceil(4.605) = 5
			description: "1% error, 1% failure probability",
		},
		{
			epsilon:     0.001,
			delta:       0.01,
			wantWidth:   2719, // ceil(e / 0.001) = ceil(2718.28) = 2719
			wantDepth:   5,
			description: "0.1% error, 1% failure probability",
		},
		{
			epsilon:     0.001,
			delta:       0.001,
			wantWidth:   2719,
			wantDepth:   7, // ceil(ln(1000)) = ceil(6.908) = 7
			description: "0.1% error, 0.1% failure probability",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			width, depth := DimensionsFromProb(tt.epsilon, tt.delta)
			if width != tt.wantWidth {
				t.Errorf("Width: got %d, want %d", width, tt.wantWidth)
			}
			if depth != tt.wantDepth {
				t.Errorf("Depth: got %d, want %d", depth, tt.wantDepth)
			}
		})
	}
}

func TestDimensionsFromProb_InvalidInputs(t *testing.T) {
	// Invalid inputs should be sanitized
	width, depth := DimensionsFromProb(0, 0)
	if width == 0 || depth == 0 {
		t.Error("DimensionsFromProb should handle invalid inputs")
	}

	width, depth = DimensionsFromProb(-1, -1)
	if width == 0 || depth == 0 {
		t.Error("DimensionsFromProb should handle negative inputs")
	}
}

// BenchmarkIncr measures the performance of the Incr operation.
func BenchmarkIncr(b *testing.B) {
	c := New(1000, 5)
	item := []byte("benchmark-item")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Incr(item, 1)
	}
}

// BenchmarkQuery measures the performance of the Query operation.
func BenchmarkQuery(b *testing.B) {
	c := New(1000, 5)
	item := []byte("benchmark-item")
	c.Incr(item, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Query(item)
	}
}

// BenchmarkIncr_Unique measures Incr with unique items (worst case for cache).
func BenchmarkIncr_Unique(b *testing.B) {
	c := New(1000, 5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := fmt.Sprintf("item-%d", i)
		c.Incr([]byte(item), 1)
	}
}
