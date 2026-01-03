package hyperloglog

import (
	"bytes"
	"testing"
)

// TestHeaderRoundTrip verifies that the header serialization and deserialization
// functions work correctly together.
func TestHeaderRoundTrip(t *testing.T) {
	// We'll define a few test cases to check different states.
	testCases := []struct {
		name   string
		header hllHeader
	}{
		{
			name: "Sparse encoding with invalid cache",
			header: hllHeader{
				encoding:          sparse,
				cachedCardinality: 0,
				cacheInvalid:      true,
			},
		},
		{
			name: "Dense encoding with valid cache",
			header: hllHeader{
				encoding:          dense,
				cachedCardinality: 12345,
				cacheInvalid:      false,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Step 1: Serialize the original header.
			serializedData := tc.header.serialize()

			// Basic sanity checks on the serialized data.
			if serializedData == nil {
				t.Fatal("serialize() returned nil, expected a 16-byte slice")
			}
			if len(serializedData) != headerSize {
				t.Fatalf("serialized data has wrong length: got %d, want %d", len(serializedData), headerSize)
			}

			// Check that the magic string was written correctly.
			if !bytes.Equal(serializedData[0:4], []byte(Magic)) {
				t.Fatalf("magic string is incorrect: got %q, want %q", serializedData[0:4], Magic)
			}

			// Step 2: Deserialize the data back into a new header struct.
			deserializedHeader, err := deserializeHeader(serializedData)
			if err != nil {
				t.Fatalf("deserializeHeader() returned an unexpected error: %v", err)
			}

			// Step 3: Verify that every field is identical.
			if deserializedHeader.encoding != tc.header.encoding {
				t.Errorf("encoding mismatch: got %v, want %v", deserializedHeader.encoding, tc.header.encoding)
			}
			if deserializedHeader.cacheInvalid != tc.header.cacheInvalid {
				t.Errorf("cacheInvalid flag mismatch: got %v, want %v", deserializedHeader.cacheInvalid, tc.header.cacheInvalid)
			}
			if deserializedHeader.cachedCardinality != tc.header.cachedCardinality {
				t.Errorf("cachedCardinality mismatch: got %v, want %v", deserializedHeader.cachedCardinality, tc.header.cachedCardinality)
			}
		})
	}
}

// TestDeserializeHeaderErrors tests the error cases of the deserializer.
func TestDeserializeHeaderErrors(t *testing.T) {
	t.Run("Slice too short", func(t *testing.T) {
		shortSlice := make([]byte, 10)
		_, err := deserializeHeader(shortSlice)
		if err == nil {
			t.Error("expected an error for a short slice, but got nil")
		}
	})

	t.Run("Wrong magic string", func(t *testing.T) {
		badMagicSlice := []byte("NOT_HYLL_and_more_bytes")
		_, err := deserializeHeader(badMagicSlice)
		if err == nil {
			t.Error("expected an error for a wrong magic string, but got nil")
		}
	})
}
