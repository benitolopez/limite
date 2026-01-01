package topk

import (
	"math"
	"testing"
)

// TestDecayMath verifies that our optimizations (Lookup Table + Large Counter Trick)
// yield results mathematically equivalent to the slow math.Pow() function.
func TestDecayMath(t *testing.T) {
	cfg := Config{Decay: 0.9}
	tk := New(cfg)

	// Test 1: Small Counts (Lookup Table Range: 0-255)
	for i := uint64(0); i < decayLookupSize; i++ {
		expected := math.Pow(0.9, float64(i))
		actual := tk.getDecayProb(i)

		if math.Abs(expected-actual) > 1e-12 {
			t.Errorf("Decay mismatch at %d: want %v, got %v", i, expected, actual)
		}
	}

	// Test 2: Large Counts (The "Trick" Range: >255)
	largeCounts := []uint64{256, 300, 510, 511, 1000, 5000}

	for _, count := range largeCounts {
		expected := math.Pow(0.9, float64(count))
		actual := tk.getDecayProb(count)

		// For large powers, allow slightly larger tolerance due to FP accumulation
		if math.Abs(expected-actual) > 1e-9 {
			t.Errorf("Large decay mismatch at %d: want %v, got %v", count, expected, actual)
		}
	}
}

// TestHeapLinearScan verifies the custom heap implementation.
func TestHeapLinearScan(t *testing.T) {
	tk := New(Config{K: 10})

	// Add some items
	tk.Add([]string{"A", "B", "C"})

	// Test linear search
	idx, found := tk.heap.linearSearch("A")
	if !found {
		t.Error("Linear scan failed to find existing key 'A'")
	}
	if tk.heap[idx].Key != "A" {
		t.Error("Linear scan returned wrong index")
	}

	_, found = tk.heap.linearSearch("Z")
	if found {
		t.Error("Linear scan found non-existent key 'Z'")
	}
}

// TestHeapMinProperty verifies the min-heap invariant.
func TestHeapMinProperty(t *testing.T) {
	h := make(list, 0)

	// Push items with varying counts
	h.Push(Item{Key: "A", Count: 10})
	h.Push(Item{Key: "B", Count: 5})
	h.Push(Item{Key: "C", Count: 20})
	h.Push(Item{Key: "D", Count: 1})

	// Root should be minimum
	if h[0].Key != "D" || h[0].Count != 1 {
		t.Errorf("Expected root to be D(1), got %s(%d)", h[0].Key, h[0].Count)
	}

	// Update B to 50 and fix
	idx, _ := h.linearSearch("B")
	h[idx].Count = 50
	h.Fix(idx)

	// Root should still be D (1)
	if h[0].Key != "D" || h[0].Count != 1 {
		t.Errorf("After fix, expected root to be D(1), got %s(%d)", h[0].Key, h[0].Count)
	}
}

// TestHeavyKeeperMechanics verifies that heavy hitters survive noise.
func TestHeavyKeeperMechanics(t *testing.T) {
	// Small K, reasonable width/depth
	tk := New(Config{K: 5, Width: 2048, Depth: 3, Decay: 0.9})

	// The Elephant: "heavy" appears 1000 times
	for i := 0; i < 1000; i++ {
		tk.Add([]string{"heavy"})
	}

	// The Noise: 5000 random unique items appear once
	for i := 0; i < 5000; i++ {
		tk.Add([]string{string([]byte{byte(i % 256), byte(i / 256)})})
	}

	// Verify "heavy" survived in the Top K
	found, count := tk.Query("heavy")
	if !found {
		t.Fatal("Heavy hitter was evicted by noise!")
	}

	// HeavyKeeper may underestimate, but should be reasonably close
	if count < 900 {
		t.Errorf("Heavy hitter count decayed too much: got %d, want ~1000", count)
	}
}

// TestAddReturnSemantics verifies Redis TOPK.ADD return format.
func TestAddReturnSemantics(t *testing.T) {
	// K=2 for easy testing
	tk := New(Config{K: 2, Width: 512, Depth: 3})

	// Add first item - should enter, nothing expelled
	result := tk.Add([]string{"first"})
	if result[0].Expelled {
		t.Errorf("First item should not expel anything, got %v", result[0])
	}

	// Add second item - should enter, nothing expelled
	result = tk.Add([]string{"second"})
	if result[0].Expelled {
		t.Errorf("Second item should not expel anything, got %v", result[0])
	}

	// Bump "first" count significantly
	for i := 0; i < 100; i++ {
		tk.Add([]string{"first"})
	}

	// Bump "second" count significantly
	for i := 0; i < 50; i++ {
		tk.Add([]string{"second"})
	}

	// Add "third" with count 1 - should fail to enter (returned as expelled)
	result = tk.Add([]string{"third"})
	if !result[0].Expelled {
		t.Error("Third item should be returned as expelled (failed to enter)")
	}
	if result[0].Key != "third" {
		t.Errorf("Expected expelled item to be 'third', got %s", result[0].Key)
	}
}

// TestAddExistingItem verifies that adding an existing item returns not expelled.
func TestAddExistingItem(t *testing.T) {
	tk := New(Config{K: 5})

	// Add item
	tk.Add([]string{"existing"})

	// Add same item again - should return not expelled
	result := tk.Add([]string{"existing"})
	if result[0].Expelled {
		t.Errorf("Adding existing item should not expel, got %v", result[0])
	}
}

// TestSerialization verifies full round-trip persistence.
func TestSerialization(t *testing.T) {
	// Setup and populate
	tk := New(Config{K: 5, Width: 100, Depth: 3, Decay: 0.9})

	tk.Add([]string{"apple"})
	for i := 0; i < 10; i++ {
		tk.Add([]string{"banana"})
	}
	tk.Add([]string{"cherry"})

	// Serialize
	data := tk.Bytes()

	// Deserialize
	restored, err := NewFromBytes(data)
	if err != nil {
		t.Fatalf("NewFromBytes failed: %v", err)
	}

	// Verify config
	if restored.K() != tk.K() {
		t.Errorf("K mismatch: want %d, got %d", tk.K(), restored.K())
	}
	if restored.Width() != tk.Width() {
		t.Errorf("Width mismatch: want %d, got %d", tk.Width(), restored.Width())
	}
	if restored.Depth() != tk.Depth() {
		t.Errorf("Depth mismatch: want %d, got %d", tk.Depth(), restored.Depth())
	}
	if restored.Decay() != tk.Decay() {
		t.Errorf("Decay mismatch: want %v, got %v", tk.Decay(), restored.Decay())
	}

	// Verify data
	found, count := restored.Query("banana")
	if !found {
		t.Error("Data lost: 'banana' not found in restored TopK")
	}
	if count < 10 {
		t.Errorf("Data corrupted: 'banana' count %d, expected ~10", count)
	}
}

// TestHasValidMagic verifies the magic check helper.
func TestHasValidMagic(t *testing.T) {
	tk := New(DefaultConfig())
	data := tk.Bytes()

	if !HasValidMagic(data) {
		t.Error("HasValidMagic returned false for valid data")
	}

	// Test with invalid data
	if HasValidMagic([]byte{0, 0, 0, 0}) {
		t.Error("HasValidMagic returned true for invalid magic")
	}

	// Test with short data
	if HasValidMagic([]byte{0, 0}) {
		t.Error("HasValidMagic returned true for short data")
	}
}

// TestList verifies the List function returns sorted items.
func TestList(t *testing.T) {
	tk := New(Config{K: 5})

	// Add items with different frequencies
	for i := 0; i < 100; i++ {
		tk.Add([]string{"high"})
	}
	for i := 0; i < 50; i++ {
		tk.Add([]string{"medium"})
	}
	for i := 0; i < 10; i++ {
		tk.Add([]string{"low"})
	}

	list := tk.List()

	// Should be sorted descending
	if len(list) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(list))
	}

	if list[0].Key != "high" {
		t.Errorf("Expected first item to be 'high', got %s", list[0].Key)
	}
	if list[1].Key != "medium" {
		t.Errorf("Expected second item to be 'medium', got %s", list[1].Key)
	}
	if list[2].Key != "low" {
		t.Errorf("Expected third item to be 'low', got %s", list[2].Key)
	}

	// Verify descending order
	for i := 0; i < len(list)-1; i++ {
		if list[i].Count < list[i+1].Count {
			t.Errorf("List not sorted: %d < %d at index %d", list[i].Count, list[i+1].Count, i)
		}
	}
}

// TestEviction verifies that heavy items evict light items.
func TestEviction(t *testing.T) {
	// K=1 to make eviction obvious
	tk := New(Config{K: 1, Width: 512, Depth: 3})

	// Add "light" with small count
	tk.Add([]string{"light"})

	// Verify "light" is in TopK
	found, _ := tk.Query("light")
	if !found {
		t.Fatal("'light' should be in TopK initially")
	}

	// Add "heavy" with much larger count
	for i := 0; i < 100; i++ {
		result := tk.Add([]string{"heavy"})
		// At some point, "light" should be expelled
		if result[0].Expelled && result[0].Key == "light" {
			// Good - "light" was expelled
			break
		}
	}

	// Verify "heavy" is now in TopK
	found, _ = tk.Query("heavy")
	if !found {
		t.Error("'heavy' should be in TopK")
	}

	// Verify "light" is no longer in TopK
	found, _ = tk.Query("light")
	if found {
		t.Error("'light' should have been evicted")
	}
}

// TestAddWeighted verifies the weighted add functionality.
func TestAddWeighted(t *testing.T) {
	tk := New(Config{K: 5, Width: 2048, Depth: 5})

	// Add items with different weights
	items := []string{"heavy", "medium", "light"}
	increments := []uint64{100, 50, 10}

	result := tk.AddWeighted(items, increments)

	// All should enter (heap not full)
	for i, r := range result {
		if r.Expelled {
			t.Errorf("Item %d should not be expelled", i)
		}
	}

	// Verify counts are approximately correct
	found, count := tk.Query("heavy")
	if !found || count < 90 {
		t.Errorf("Expected heavy to be found with count ~100, got found=%v count=%d", found, count)
	}

	found, count = tk.Query("medium")
	if !found || count < 45 {
		t.Errorf("Expected medium to be found with count ~50, got found=%v count=%d", found, count)
	}

	found, count = tk.Query("light")
	if !found || count < 8 {
		t.Errorf("Expected light to be found with count ~10, got found=%v count=%d", found, count)
	}

	// Verify list is sorted correctly
	list := tk.List()
	if len(list) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(list))
	}
	if list[0].Key != "heavy" {
		t.Errorf("Expected first item to be 'heavy', got %s", list[0].Key)
	}
}

// TestAddWeightedEviction verifies that weighted items can evict lighter items.
func TestAddWeightedEviction(t *testing.T) {
	// K=2 to make eviction obvious
	tk := New(Config{K: 2, Width: 512, Depth: 3})

	// First add two light items
	tk.AddWeighted([]string{"light1", "light2"}, []uint64{1, 1})

	// Add a heavy item - should evict one of the light ones
	result := tk.AddWeighted([]string{"heavy"}, []uint64{100})

	// heavy should have evicted one of the lights
	if !result[0].Expelled {
		// heavy entered, something was expelled - check the list
		list := tk.List()
		if len(list) != 2 {
			t.Errorf("Expected 2 items in list, got %d", len(list))
		}
		// heavy should be in the list
		found := false
		for _, item := range list {
			if item.Key == "heavy" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected 'heavy' to be in the list")
		}
	}
}

// TestAddWeightedZeroIncrement verifies zero increment behavior.
func TestAddWeightedZeroIncrement(t *testing.T) {
	tk := New(Config{K: 5})

	// Add an item first
	tk.Add([]string{"existing"})

	// Zero increment on existing item - should return not expelled
	result := tk.AddWeighted([]string{"existing"}, []uint64{0})
	if result[0].Expelled {
		t.Error("Zero increment on existing item should return not expelled")
	}

	// Zero increment on non-existing item - should return expelled
	result = tk.AddWeighted([]string{"nonexistent"}, []uint64{0})
	if !result[0].Expelled {
		t.Error("Zero increment on non-existing item should return expelled")
	}
}

// TestAddWeightedSerialization verifies that weighted items persist correctly.
func TestAddWeightedSerialization(t *testing.T) {
	tk := New(Config{K: 5, Width: 100, Depth: 3})

	tk.AddWeighted([]string{"weighted"}, []uint64{50})

	// Serialize
	data := tk.Bytes()

	// Deserialize
	restored, err := NewFromBytes(data)
	if err != nil {
		t.Fatalf("NewFromBytes failed: %v", err)
	}

	// Verify the weighted item persisted
	found, count := restored.Query("weighted")
	if !found {
		t.Error("Weighted item not found after deserialization")
	}
	if count < 45 {
		t.Errorf("Weighted item count too low after deserialization: %d", count)
	}
}

// BenchmarkAddWeighted measures weighted write throughput.
func BenchmarkAddWeighted(b *testing.B) {
	tk := New(Config{K: 100, Width: 4096, Depth: 5, Decay: 0.9})
	items := []string{"item1", "item2", "item3", "item4", "item5"}
	increments := []uint64{10, 20, 30, 40, 50}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tk.AddWeighted(items, increments)
	}
}

// BenchmarkAdd measures write throughput.
func BenchmarkAdd(b *testing.B) {
	tk := New(Config{K: 100, Width: 4096, Depth: 5, Decay: 0.9})
	keys := []string{"item1", "item2", "item3", "item4", "item5"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tk.Add(keys)
	}
}

// BenchmarkQuery measures read throughput.
func BenchmarkQuery(b *testing.B) {
	tk := New(Config{K: 100, Width: 4096, Depth: 5, Decay: 0.9})

	// Pre-populate
	for i := 0; i < 100; i++ {
		tk.Add([]string{"item" + string(rune(i))})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tk.Query("item50")
	}
}

// BenchmarkDecayProb measures decay calculation performance.
func BenchmarkDecayProb(b *testing.B) {
	tk := New(Config{Decay: 0.9})

	b.Run("Small_Lookup", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = tk.getDecayProb(100)
		}
	})

	b.Run("Large_Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = tk.getDecayProb(5000)
		}
	})

	b.Run("Standard_MathPow", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = math.Pow(0.9, 5000)
		}
	})
}
