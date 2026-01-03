package hyperloglog

import (
	"encoding/binary"
	"errors"
)

const (
	headerSize = 16
	Magic      = "HYLL"
)

type hllHeader struct {
	encoding          encoding
	cachedCardinality uint64
	cacheInvalid      bool
}

// serialize encodes the in-memory hllHeader struct into its 16-byte on-disk
// representation. This function is the counterpart to deserializeHeader
// and ensures the data is stored in a portable, well-defined format.
func (h hllHeader) serialize() []byte {
	//
	// DESIGN
	// ------
	//
	// The header has a fixed 16-byte layout. We create a buffer of this size
	// and populate it field by field according to the specification.
	//
	// +------+----------+-----+-------------------------------+
	// | Bytes| Field    | Size| Notes                         |
	// +------+----------+-----+-------------------------------+
	// | 0-3  | Magic    | 4   | "HYLL"                        |
	// | 4    | Encoding | 1   | 0 for dense, 1 for sparse     |
	// | 5-7  | Not Used | 3   | Reserved, must be zero        |
	// | 8-15 | Card.    | 8   | Cached cardinality (uint64)   |
	// +------+----------+-----+-------------------------------+
	//

	buffer := make([]byte, headerSize)

	// Copy the 4-byte magic string to identify this as an HLL object.
	buffer[0] = Magic[0]
	buffer[1] = Magic[1]
	buffer[2] = Magic[2]
	buffer[3] = Magic[3]

	// Set the encoding flag (hllDense or hllSparse).
	buffer[4] = byte(h.encoding)

	// Encode the 64-bit cached cardinality into the buffer using little-endian
	// byte order. This is critical for ensuring the data format is portable
	// across machine architectures with different native endianness.
	binary.LittleEndian.PutUint64(buffer[8:16], h.cachedCardinality)

	// Set the "dirty bit" if the cache is invalid. The most significant bit
	// of the entire 8-byte cardinality field (which is the MSB of the last
	// byte, `buffer[15]`, in little-endian) is used as a flag. A `1`
	// indicates the cached value is stale and must be recomputed.
	//
	// This assumes the true cardinality will never be large enough to require
	// the 64th bit (2^63), which is a safe assumption for any real-world dataset.
	//
	// To prevent ambiguity, we must first ensure the MSB of the stored
	// cardinality is 0 before we conditionally set it based on the cacheInvalid flag.
	buffer[15] &= 0x7F
	if h.cacheInvalid {
		buffer[15] |= 0x80 // Set the MSB to 1
	}

	return buffer
}

// deserializeHeader parses a raw byte slice and decodes the first 16 bytes
// into an hllHeader struct. It performs validation to ensure the data is a
// valid HLL object. If validation fails, it returns a specific error
// describing the issue. This function is the counterpart to serialize.
func deserializeHeader(data []byte) (*hllHeader, error) {
	// The first validation step is to ensure the slice is long enough to
	// possibly contain our header.
	if len(data) < headerSize {
		return nil, errors.New("invalid HLL data: slice is too short for header")
	}

	// Next, we validate the 4-byte magic string. This is the primary
	// mechanism for type identification, confirming that we are operating
	// on an HLL object and not some other data type.
	if data[0] != Magic[0] || data[1] != Magic[1] ||
		data[2] != Magic[2] || data[3] != Magic[3] {
		return nil, errors.New("invalid HLL data: magic string not found")
	}

	h := &hllHeader{}

	// The byte at offset 4 stores the encoding. We perform a basic sanity
	// check to ensure the value is within the known range (0 or 1).
	h.encoding = encoding(data[4])
	if h.encoding > sparse {
		return nil, errors.New("invalid HLL data: unknown encoding value")
	}

	// Decode the 8-byte, little-endian cached cardinality. This raw 64-bit
	// value also contains the "dirty bit" in its most significant bit.
	rawCardinality := binary.LittleEndian.Uint64(data[8:16])

	// The most significant bit of the cardinality field is overloaded to act
	// as a "dirty" flag. A `1` indicates the cache is invalid. We extract
	// this flag by checking the 64th bit. Then, we clear this bit from the
	// raw value to get the true cached cardinality, which is safe to do
	// as the true value will never be large enough to use this bit.
	h.cacheInvalid = (rawCardinality >> 63) == 1
	h.cachedCardinality = rawCardinality & ^(uint64(1) << 63)

	return h, nil
}
