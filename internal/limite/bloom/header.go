package bloom

import (
	"encoding/binary"
	"math"
)

// Metadata is a flyweight wrapper over the first 24 bytes of the backing slice.
// It provides safe, endian-aware access to the global header fields without
// requiring unsafe pointer casting or memory copying. The layout contains the
// magic number (8 bytes), total items inserted across all layers (8 bytes),
// and number of active layers (8 bytes).
type Metadata []byte

// FilterHeader is a view into a layer's 32-byte header.
type FilterHeader []byte

const (
	// Magic is the safety signature "BLOOM001" in Hex.
	Magic = 0x424C4F4F4D303031

	// Sizes in bytes
	MetadataSize    = 24
	LayerHeaderSize = 32
)

// Magic returns the safety signature.
// Uses Little Endian to ensure portability across different CPU architectures.
func (m Metadata) Magic() uint64 {
	return binary.LittleEndian.Uint64(m[0:8])
}

// SetMagic writes the safety signature.
func (m Metadata) SetMagic(v uint64) {
	binary.LittleEndian.PutUint64(m[0:8], v)
}

// TotalItems returns the global count of items across all layers.
func (m Metadata) TotalItems() uint64 {
	return binary.LittleEndian.Uint64(m[8:16])
}

// SetTotalItems updates the global count.
func (m Metadata) SetTotalItems(v uint64) {
	binary.LittleEndian.PutUint64(m[8:16], v)
}

// NumLayers returns the number of active filters in the chain.
func (m Metadata) NumLayers() uint64 {
	return binary.LittleEndian.Uint64(m[16:24])
}

// SetNumLayers updates the layer count.
func (m Metadata) SetNumLayers(v uint64) {
	binary.LittleEndian.PutUint64(m[16:24], v)
}

func (h FilterHeader) Size() uint64 {
	return binary.LittleEndian.Uint64(h[0:8])
}

func (h FilterHeader) SetSize(v uint64) {
	binary.LittleEndian.PutUint64(h[0:8], v)
}

func (h FilterHeader) Capacity() uint64 {
	return binary.LittleEndian.Uint64(h[8:16])
}

func (h FilterHeader) SetCapacity(v uint64) {
	binary.LittleEndian.PutUint64(h[8:16], v)
}

func (h FilterHeader) Count() uint64 {
	return binary.LittleEndian.Uint64(h[16:24])
}

func (h FilterHeader) SetCount(v uint64) {
	binary.LittleEndian.PutUint64(h[16:24], v)
}

func (h FilterHeader) ErrorRate() float64 {
	bits := binary.LittleEndian.Uint64(h[24:32])
	return math.Float64frombits(bits)
}

func (h FilterHeader) SetErrorRate(v float64) {
	bits := math.Float64bits(v)
	binary.LittleEndian.PutUint64(h[24:32], bits)
}
