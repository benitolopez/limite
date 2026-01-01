package bloom

import (
	"testing"
)

func TestMetadataAccessors(t *testing.T) {
	data := make([]byte, MetadataSize)
	meta := Metadata(data)

	// 1. Check Initial State
	if meta.Magic() != 0 {
		t.Error("New metadata should have 0 magic")
	}

	// 2. Set Values
	meta.SetMagic(Magic)
	meta.SetTotalItems(500)
	meta.SetNumLayers(3)

	// 3. Verify Values
	if meta.Magic() != Magic {
		t.Errorf("Magic mismatch. Got %x", meta.Magic())
	}
	if meta.TotalItems() != 500 {
		t.Errorf("TotalItems mismatch. Got %d", meta.TotalItems())
	}
	if meta.NumLayers() != 3 {
		t.Errorf("NumLayers mismatch. Got %d", meta.NumLayers())
	}
}
