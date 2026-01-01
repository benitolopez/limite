package bloom

import (
	"errors"
	"unsafe"
)

// layerOffset is an internal index entry used to map logical layers
// to physical byte ranges in the backing slice.
type layerOffset struct {
	header FilterHeader // The safe view
	data   []Block      // The unsafe block view
}

// reloadLayers scans the backing slice and rebuilds the internal layers index.
// This is necessary on initial load and whenever the backing slice is reallocated
// (e.g. during growth), as the underlying memory addresses will change.
//
// The function reuses the existing pooled slice capacity when possible,
// minimizing allocations on the hot path.
func (sf *ScalableFilter) reloadLayers() error {
	// Safety check to prevent int overflow and OOM on corrupted files.
	rawCount := sf.Metadata().NumLayers()
	if rawCount > MaxLayers {
		return errors.New("bloom: too many layers (possible corruption)")
	}

	numLayers := int(rawCount)

	// Reuse the existing pooled slice. Reset length but keep capacity.
	// If the capacity is insufficient (rare case of many layers), we must
	// reallocate. This preserves the pool benefits for the common case.
	layers := *sf.layers
	if cap(layers) >= numLayers {
		layers = layers[:0]
	} else {
		// Rare path: filter has grown beyond pooled capacity.
		// Allocate a new slice with exact capacity needed.
		layers = make([]layerOffset, 0, numLayers)
	}

	offset := MetadataSize
	dataLen := len(sf.backing)

	for i := 0; i < numLayers; i++ {
		// Validate we have enough bytes for the layer header.
		if offset+LayerHeaderSize > dataLen {
			return errors.New("bloom: buffer too short for layer header")
		}

		// Map the header as a zero-copy slice view into the backing array.
		hdr := FilterHeader(sf.backing[offset : offset+LayerHeaderSize])
		offset += LayerHeaderSize

		// Validate we have enough bytes for the layer data.
		dataSize := int(hdr.Size())
		if offset+dataSize > dataLen {
			return errors.New("bloom: buffer too short for layer data")
		}

		// Blocked Bloom Filters must be aligned to 64-byte cache lines.
		if dataSize%64 != 0 {
			return errors.New("bloom: layer size not aligned to 64 bytes")
		}

		// Cast the raw bytes directly to a slice of Blocks. This enables
		// zero-copy operations while preserving strong typing.
		numBlocks := dataSize / 64
		ptr := unsafe.Pointer(&sf.backing[offset])
		blocks := unsafe.Slice((*Block)(ptr), numBlocks)

		layers = append(layers, layerOffset{
			header: hdr,
			data:   blocks,
		})

		offset += dataSize
	}

	// Update the pointer to the (possibly new) slice.
	*sf.layers = layers

	return nil
}

// addLayer appends a new filter layer to the chain. This involves allocating
// space in the backing slice and updating metadata.
func (sf *ScalableFilter) addLayer(cap uint64, errRate float64) error {
	// Calculate the optimal size and prepare the layer header.
	size, _ := EstimateParameters(cap, errRate)

	hdrBytes := make([]byte, LayerHeaderSize)
	hdr := FilterHeader(hdrBytes)
	hdr.SetSize(size)
	hdr.SetCapacity(cap)
	hdr.SetCount(0)
	hdr.SetErrorRate(errRate)

	// Append header and zeroed data to storage. Note that append() might
	// reallocate the underlying array, invalidating all unsafe pointers
	// in sf.layers.
	dataBytes := make([]byte, size)
	sf.backing = append(sf.backing, hdrBytes...)
	sf.backing = append(sf.backing, dataBytes...)

	sf.Metadata().SetNumLayers(sf.Metadata().NumLayers() + 1)

	// Rescan the backing slice to update sf.layers with valid pointers
	// to the (possibly new) memory address.
	return sf.reloadLayers()
}
