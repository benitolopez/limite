package bloom

import (
	"fmt"
	"math"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

func TestNewScalableFilter_Fresh(t *testing.T) {
	// 1. Create with nil data
	sf, err := NewScalableFilter(nil, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create fresh filter: %v", err)
	}

	// 2. Check Defaults using Safe Accessors
	meta := sf.Metadata()

	if meta.Magic() != Magic {
		t.Errorf("Wrong Magic. Got %x, Want %x", meta.Magic(), Magic)
	}
	if meta.NumLayers() != 0 {
		t.Errorf("Fresh filter should have 0 layers, got %d", meta.NumLayers())
	}

	// 3. Check Backing Buffer
	if len(sf.backing) != 24 {
		t.Errorf("Backing buffer size wrong. Got %d, Want 24", len(sf.backing))
	}
}

func TestScalableFilter_Check(t *testing.T) {
	// 1. Manually build a filter with 1 layer
	// Header(24) + [LayerHeader(32) + 1 Block(64)] = 120 bytes
	data := make([]byte, 120)

	// Initialize Magic manually so NewScalableFilter accepts the blob
	meta := Metadata(data[:24])
	meta.SetMagic(Magic)
	meta.SetNumLayers(1)

	// 2. Load
	sf, err := NewScalableFilter(data, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	// 3. Manually wire up the internal slice for the test
	// (NewScalableFilter scanned 1 layer, but it was empty/zero.
	// We need to point sf.layers[0] to our data correctly and set the size).

	// Configure Layer 1 Header (Offset 24)
	hdr := FilterHeader(data[24:56])
	hdr.SetSize(64) // 1 Block

	// We need to overwrite the layer entry NewScalableFilter created
	// (which might have failed logic because Size was 0 during scan)
	// Actually, if Size was 0 during NewScalableFilter, the scan loop might have skipped or error'd.
	//
	// Let's rely on manual injection for this specific unit test to isolate Check().
	*sf.layers = nil // Reset what the loader did

	blockPtr := unsafe.Pointer(&data[56])
	blocks := unsafe.Slice((*Block)(blockPtr), 1)
	*sf.layers = append(*sf.layers, layerOffset{header: hdr, data: blocks})

	// 4. Add an item
	item := []byte("test-item")
	itemHash := xxhash.Sum64(item)

	// In Check(), we use:
	// BlockIndex = hash % 1 = 0
	// InternalHash = mix(hash)
	internalHash := mix(itemHash)

	// Add using internal hash directly to the block
	(*sf.layers)[0].data[0].Add(internalHash)

	// 5. Test Check
	if !sf.Check(item) {
		t.Error("Check returned false for present item")
	}

	// 6. Test Absent
	if sf.Check([]byte("missing-item")) {
		t.Error("Check returned true for missing item")
	}
}

func TestScalableFilter_Add_Growth(t *testing.T) {
	// 1. Define Initial State
	initialCap := uint64(1)
	initialErr := 0.1

	// 2. Manually construct a filter with 1 FULL layer
	data := make([]byte, MetadataSize)

	// FIX: Write Magic/NumLayers to raw bytes BEFORE calling NewScalableFilter
	meta := Metadata(data)
	meta.SetMagic(Magic)
	meta.SetNumLayers(1)

	// Calculate size for the first layer
	layerSize, _ := EstimateParameters(initialCap, initialErr)

	// Create Header
	hdrBytes := make([]byte, LayerHeaderSize)
	hdr := FilterHeader(hdrBytes)
	hdr.SetCapacity(initialCap)
	hdr.SetCount(initialCap) // Count == Capacity means FULL
	hdr.SetSize(layerSize)
	hdr.SetErrorRate(initialErr)

	// Create Data
	dataBytes := make([]byte, layerSize)

	// Append Header + Data to backing
	// Note: We append to 'data' which currently holds just the metadata
	data = append(data, hdrBytes...)
	data = append(data, dataBytes...)

	// 3. Load the prepared data with a config matching the manually built layer
	sf, err := NewScalableFilter(data, Config{InitialCapacity: initialCap, ErrorRate: initialErr})
	if err != nil {
		t.Fatalf("Failed to load prepared filter: %v", err)
	}

	// 4. Add Item (Should trigger Growth -> Layer 2)
	item := []byte("trigger-growth")
	newBacking, added, err := sf.Add(item)
	if err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	if !added {
		t.Error("Item should have been added")
	}

	// 5. Verify Growth
	// We expect the backing slice to have grown
	if len(newBacking) <= len(data) {
		t.Error("Backing slice did not grow")
	}

	// We expect 2 layers now
	if sf.Metadata().NumLayers() != 2 {
		t.Errorf("Expected 2 layers, got %d", sf.Metadata().NumLayers())
	}

	// 6. Verify Layer 2 Configuration
	layer2 := (*sf.layers)[1]

	expectedCap := initialCap * 2
	if layer2.header.Capacity() != expectedCap {
		t.Errorf("Expected Layer 2 capacity %d, got %d", expectedCap, layer2.header.Capacity())
	}

	expectedErr := initialErr * 0.5
	if math.Abs(layer2.header.ErrorRate()-expectedErr) > 0.000001 {
		t.Errorf("Expected Layer 2 error %f, got %f", expectedErr, layer2.header.ErrorRate())
	}
}

// TestFalsePositiveRate verifies that the bloom filter achieves approximately
// the configured false positive rate. This is the key correctness property
// of a bloom filter: it should have zero false negatives and a bounded FPR.
func TestFalsePositiveRate(t *testing.T) {
	testCases := []struct {
		name       string
		capacity   uint64
		errorRate  float64
		numItems   int
		numQueries int
		tolerance  float64 // Allow FPR to be within this factor of expected
	}{
		{
			name:       "1% error rate, 1000 items",
			capacity:   1000,
			errorRate:  0.01,
			numItems:   1000,
			numQueries: 100000,
			tolerance:  2.0, // Allow up to 2x expected FPR
		},
		{
			name:       "5% error rate, 500 items",
			capacity:   500,
			errorRate:  0.05,
			numItems:   500,
			numQueries: 50000,
			tolerance:  2.0,
		},
		{
			name:       "0.1% error rate, 10000 items",
			capacity:   10000,
			errorRate:  0.001,
			numItems:   10000,
			numQueries: 100000,
			tolerance:  4.0, // Lower FPR has more variance + blocked filter overhead
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Create filter with specified config
			cfg := Config{
				InitialCapacity: tc.capacity,
				ErrorRate:       tc.errorRate,
			}
			sf, err := NewScalableFilter(nil, cfg)
			if err != nil {
				t.Fatalf("Failed to create filter: %v", err)
			}
			defer sf.Release()

			// 2. Add items to the filter
			var backing []byte
			for i := 0; i < tc.numItems; i++ {
				item := fmt.Sprintf("item-%d", i)
				backing, _, err = sf.Add([]byte(item))
				if err != nil {
					t.Fatalf("Failed to add item %d: %v", i, err)
				}
			}

			// Reload filter from backing to simulate real usage
			sf.Release()
			sf, err = NewScalableFilter(backing, cfg)
			if err != nil {
				t.Fatalf("Failed to reload filter: %v", err)
			}
			defer sf.Release()

			// 3. Verify NO false negatives (items we added MUST be found)
			for i := 0; i < tc.numItems; i++ {
				item := fmt.Sprintf("item-%d", i)
				if !sf.Check([]byte(item)) {
					t.Errorf("False negative detected for item-%d", i)
				}
			}

			// 4. Count false positives (items we NEVER added)
			falsePositives := 0
			for i := 0; i < tc.numQueries; i++ {
				// Use a different prefix to ensure these were never added
				item := fmt.Sprintf("notexist-%d", i)
				if sf.Check([]byte(item)) {
					falsePositives++
				}
			}

			// 5. Calculate actual FPR
			actualFPR := float64(falsePositives) / float64(tc.numQueries)
			expectedFPR := tc.errorRate

			t.Logf("Results: items=%d, queries=%d, false_positives=%d",
				tc.numItems, tc.numQueries, falsePositives)
			t.Logf("Expected FPR: %.4f%%, Actual FPR: %.4f%%",
				expectedFPR*100, actualFPR*100)
			t.Logf("Layers: %d, Total items stored: %d",
				sf.Metadata().NumLayers(), sf.Metadata().TotalItems())

			// 6. Verify FPR is within tolerance
			// FPR can vary due to randomness, so we allow some tolerance
			maxAllowedFPR := expectedFPR * tc.tolerance
			if actualFPR > maxAllowedFPR {
				t.Errorf("FPR too high: got %.4f%%, max allowed %.4f%% (%.1fx expected)",
					actualFPR*100, maxAllowedFPR*100, tc.tolerance)
			}

			// Also check it's not suspiciously low (might indicate a bug)
			// A working bloom filter should have SOME false positives
			if tc.numQueries >= 10000 && actualFPR == 0 {
				t.Logf("Warning: Zero false positives is suspicious for %d queries", tc.numQueries)
			}
		})
	}
}

// TestFalsePositiveRate_ScaledFilter tests FPR when the filter grows beyond
// its initial capacity, triggering the scalable bloom filter behavior.
func TestFalsePositiveRate_ScaledFilter(t *testing.T) {
	// Start with small capacity to force scaling
	cfg := Config{
		InitialCapacity: 100,
		ErrorRate:       0.01,
	}

	sf, err := NewScalableFilter(nil, cfg)
	if err != nil {
		t.Fatalf("Failed to create filter: %v", err)
	}
	defer sf.Release()

	// Add 10x the initial capacity to force multiple layers
	numItems := 1000
	var backing []byte
	for i := 0; i < numItems; i++ {
		item := fmt.Sprintf("item-%d", i)
		backing, _, err = sf.Add([]byte(item))
		if err != nil {
			t.Fatalf("Failed to add item %d: %v", i, err)
		}
	}

	// Reload and verify
	sf.Release()
	sf, err = NewScalableFilter(backing, cfg)
	if err != nil {
		t.Fatalf("Failed to reload filter: %v", err)
	}
	defer sf.Release()

	numLayers := sf.Metadata().NumLayers()
	t.Logf("Filter scaled to %d layers for %d items (initial capacity: %d)",
		numLayers, numItems, cfg.InitialCapacity)

	if numLayers < 2 {
		t.Errorf("Expected multiple layers, got %d", numLayers)
	}

	// Verify no false negatives
	for i := 0; i < numItems; i++ {
		item := fmt.Sprintf("item-%d", i)
		if !sf.Check([]byte(item)) {
			t.Errorf("False negative detected for item-%d", i)
		}
	}

	// Count false positives
	numQueries := 100000
	falsePositives := 0
	for i := 0; i < numQueries; i++ {
		item := fmt.Sprintf("notexist-%d", i)
		if sf.Check([]byte(item)) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(numQueries)

	// For scalable bloom filters, the combined FPR should be bounded by:
	// FPR_total ≈ FPR_0 * (1 + r + r^2 + ...) where r = tightening ratio
	// With r=0.5: FPR_total ≈ FPR_0 * 2 = 0.02 for our case
	// We allow 3x tolerance due to variance
	maxExpectedFPR := cfg.ErrorRate * 2 * 3 // ~6%

	t.Logf("Scaled filter FPR: %.4f%% (max expected: %.4f%%)",
		actualFPR*100, maxExpectedFPR*100)

	if actualFPR > maxExpectedFPR {
		t.Errorf("Scaled filter FPR too high: %.4f%% > %.4f%%",
			actualFPR*100, maxExpectedFPR*100)
	}
}
