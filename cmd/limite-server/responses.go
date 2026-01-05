package main

import (
	"io"
	"strconv"
)

// Pre-allocated response buffers for common cases.
//
// These byte slices are created once at startup and reused for every response.
// This eliminates allocations entirely for the most frequent responses:
// - PONG (from PING command)
// - OK (from successful write commands)
// - 0 and 1 (from HLL.ADD, which returns whether registers changed)
// - Nil (for missing keys)
var (
	respOK   = []byte("+OK\r\n")
	respPong = []byte("+PONG\r\n")
	respZero = []byte(":0\r\n")
	respOne  = []byte(":1\r\n")
	respNil  = []byte("$-1\r\n")
)

func (app *application) writeSimpleStringResponse(w io.Writer, s string) error {
	// Fast path for common responses.
	if s == "OK" {
		_, err := w.Write(respOK)
		return err
	}
	if s == "PONG" {
		_, err := w.Write(respPong)
		return err
	}

	// Fallback: build the response without fmt.Sprintf.
	// Format: +string\r\n
	buf := make([]byte, 0, 1+len(s)+2)
	buf = append(buf, '+')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	_, err := w.Write(buf)
	return err
}

func (app *application) writeErrorResponse(w io.Writer, errStr string) error {
	// Format: -string\r\n
	// Errors are not on the hot path, but we still avoid fmt.Sprintf.
	buf := make([]byte, 0, 1+len(errStr)+2)
	buf = append(buf, '-')
	buf = append(buf, errStr...)
	buf = append(buf, '\r', '\n')
	_, err := w.Write(buf)
	return err
}

func (app *application) writeBulkStringResponse(w io.Writer, s string) error {
	// Format: $length\r\nstring\r\n
	// Used for INFO command output. Not a hot path.
	buf := make([]byte, 0, 16+len(s))
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	_, err := w.Write(buf)
	return err
}

func (app *application) writeIntegerResponse(w io.Writer, i int) error {
	return app.writeIntegerResponse64(w, int64(i))
}

func (app *application) writeIntegerResponse64(w io.Writer, i int64) error {
	// Fast path for 0 and 1, which cover ~99% of HLL.ADD responses.
	// HLL.ADD returns 1 if any register changed, 0 otherwise.
	// Using pre-allocated buffers eliminates all allocations for these cases.
	if i == 0 {
		_, err := w.Write(respZero)
		return err
	}
	if i == 1 {
		_, err := w.Write(respOne)
		return err
	}

	// Fallback: use strconv.AppendInt which is ~10x faster than fmt.Sprintf.
	// Format: :integer\r\n
	buf := make([]byte, 0, 24)
	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, i, 10)
	buf = append(buf, '\r', '\n')
	_, err := w.Write(buf)
	return err
}

func (app *application) writeNilResponse(w io.Writer) error {
	// Format: $-1\r\n (Null Bulk String)
	_, err := w.Write(respNil)
	return err
}

// writeBulkBytesResponse writes a byte slice as a RESP Bulk String.
// This allows us to write directly from the Store to the Network without
// string conversion, avoiding an allocation when returning binary data.
func (app *application) writeBulkBytesResponse(w io.Writer, data []byte) error {
	// Format: $length\r\n<bytes>\r\n
	buf := make([]byte, 0, 16+len(data))
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(data)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, data...)
	buf = append(buf, '\r', '\n')
	_, err := w.Write(buf)
	return err
}

// writeIntegerArrayResponse writes a RESP array of integers.
// Format: *count\r\n:int1\r\n:int2\r\n...:intN\r\n
//
// This is optimized for the common case where values are 0 or 1 (Bloom filter
// and set membership results). The entire response is written in a single
// Write call for atomicity.
func (app *application) writeIntegerArrayResponse(w io.Writer, values []int) error {
	// Estimate buffer size: header (~6 bytes) + per element (~5 bytes for 0/1)
	buf := make([]byte, 0, 6+len(values)*5)

	// Write array header: *count\r\n
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(values)), 10)
	buf = append(buf, '\r', '\n')

	// Write each integer element
	for _, v := range values {
		buf = append(buf, ':')
		buf = strconv.AppendInt(buf, int64(v), 10)
		buf = append(buf, '\r', '\n')
	}

	_, err := w.Write(buf)
	return err
}
