// Limite Protocol Implementation (RESP)
//
// Why RESP?
// We utilize the REdis Serialization Protocol (RESP) as our server's transport
// layer. This design choice provides two critical advantages:
//
// Ecosystem Compatibility: By speaking RESP, this server immediately works
// with existing, battle-tested tools like 'redis-cli' and 'redis-benchmark'.
// Developers can use standard Redis client libraries in Python, Go, Java,
// etc., without needing a custom Limite driver.
//
// Binary Safety & Simplicity: RESP is prefix-oriented. Every chunk of data
// is prefixed with its length (e.g., "$5\r\nhello"). This allows us to
// store arbitrary binary data (images, serialized structs, HLL bytes)
// without worrying about delimiters or escaping.
//
// Implementation Scope:
// This parser implements the "Request Subset" of RESP. While the full protocol
// defines 5 data types, a server only receives commands in two formats:
//
// RESP Arrays (Standard): The standard format for programmatic clients. A
// command is an Array (*) containing Bulk Strings ($).
// Example: "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
//
// Inline Commands (Human/Debug): A simplified, space-separated format used
// by tools like 'netcat' or 'telnet'. This is crucial for easy manual debugging.
// Example: "GET key\r\n"
//
// Security Hardening
// ==================
//
// This parser is hardened against malicious clients attempting denial-of-service
// attacks. Three attack vectors are mitigated:
//
// Bulk String Attack: A client sends "$999999999\r\n" to force a huge allocation.
// Mitigated by MaxBulkLength check before allocation.
//
// Array Count Attack: A client sends "*999999999\r\n" to allocate a huge slice.
// Mitigated by MaxArrayLen check before allocation.
//
// Infinite Line Attack: A client sends bytes without ever sending '\n', causing
// unbounded buffering. Mitigated by MaxLineSize check in readLine.

package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
)

// Protocol limits to prevent denial-of-service attacks.
// These values match Redis defaults and are safe for production use.
const (
	// MaxBulkLength limits bulk string size to 512MB.
	// This matches Redis's proto-max-bulk-len default.
	MaxBulkLength = 512 * 1024 * 1024

	// MaxArrayLen limits the number of elements in a RESP array.
	// 1M elements is more than enough for any legitimate command.
	MaxArrayLen = 1 << 20

	// MaxLineSize limits header/inline command line length.
	// 64KB is generous for any protocol header.
	MaxLineSize = 64 * 1024
)

var (
	ErrInvalidSyntax = errors.New("ERR protocol error: invalid syntax")
	ErrLineTooLong   = errors.New("ERR protocol error: line too long")
	ErrBulkTooLarge  = errors.New("ERR protocol error: bulk string exceeds 512MB limit")
	ErrArrayTooLong  = errors.New("ERR protocol error: array exceeds 1M elements limit")
)

type Parser struct {
	reader *bufio.Reader
}

func NewParser(conn io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReaderSize(conn, 4096),
	}
}

// Parse reads a single command from the connection.
//
// Supports two RESP formats:
//   - Inline commands: "PING\r\n" or "SET key value\r\n"
//   - RESP arrays: "*1\r\n$4\r\nPING\r\n"
//
// Inline commands are used by redis-benchmark and redis-cli for simple
// commands. RESP arrays are the standard format for programmatic clients.
func (p *Parser) Parse() ([]string, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, ErrInvalidSyntax
	}

	// Check the first byte to determine the format.
	// '*' indicates a RESP array, anything else is an inline command.
	if line[0] == '*' {
		return p.parseRESPArray(line)
	}

	return p.parseInline(line)
}

// readLine reads bytes until '\n', enforcing MaxLineSize to prevent
// denial-of-service attacks from clients that never send a newline.
func (p *Parser) readLine() ([]byte, error) {
	line, isPrefix, err := p.reader.ReadLine()
	if err != nil {
		return nil, err
	}

	// Fast path: line fit in buffer, no continuation needed.
	if !isPrefix {
		return line, nil
	}

	// Slow path: line exceeded buffer, accumulate chunks with size limit.
	var buf bytes.Buffer
	buf.Write(line)

	for isPrefix {
		line, isPrefix, err = p.reader.ReadLine()
		if err != nil {
			return nil, err
		}

		// Check BEFORE writing to prevent allocating beyond limit.
		if buf.Len()+len(line) > MaxLineSize {
			return nil, ErrLineTooLong
		}
		buf.Write(line)
	}

	return buf.Bytes(), nil
}

// parseInline parses an inline command.
//
// Inline format: "COMMAND arg1 arg2\r\n"
// Arguments are space-separated. This is the format used by redis-benchmark
// and telnet/netcat for manual testing.
func (p *Parser) parseInline(line []byte) ([]string, error) {
	parts := bytes.Fields(line)
	if len(parts) == 0 {
		return nil, ErrInvalidSyntax
	}

	// Convert to strings for the handler layer.
	result := make([]string, len(parts))
	for i, part := range parts {
		result[i] = string(part)
	}

	return result, nil
}

// parseRESPArray parses a RESP array command.
//
// RESP format: *<count>\r\n$<len>\r\n<data>\r\n...
// This is the standard Redis protocol format.
func (p *Parser) parseRESPArray(header []byte) ([]string, error) {
	// Parse the array length from "*<count>".
	count, err := strconv.Atoi(string(bytes.TrimSpace(header[1:])))
	if err != nil {
		return nil, ErrInvalidSyntax
	}

	// Handle null arrays (*-1) and empty arrays (*0).
	if count <= 0 {
		return []string{}, nil
	}

	// Security: prevent massive slice allocation.
	if count > MaxArrayLen {
		return nil, ErrArrayTooLong
	}

	// Pre-allocate the slice with validated count.
	command := make([]string, 0, count)

	for i := 0; i < count; i++ {
		str, err := p.parseBulkString()
		if err != nil {
			return nil, err
		}
		command = append(command, str)
	}

	return command, nil
}

// Buffered returns the number of bytes buffered in the reader.
// Used to detect if the client has sent multiple commands in a batch
// (pipelining), allowing the server to delay flushing until the buffer
// is drained.
func (p *Parser) Buffered() int {
	return p.reader.Buffered()
}

// parseBulkString reads a Bulk String from the connection.
// Format: $<length>\r\n<data>\r\n
//
// Also handles null bulk strings ($-1) which are valid RESP but return
// an empty string since Limite commands don't use null arguments.
func (p *Parser) parseBulkString() (string, error) {
	line, err := p.readLine()
	if err != nil {
		return "", err
	}

	if len(line) == 0 || line[0] != '$' {
		return "", ErrInvalidSyntax
	}

	// Parse the length.
	length, err := strconv.Atoi(string(bytes.TrimSpace(line[1:])))
	if err != nil {
		return "", ErrInvalidSyntax
	}

	// Handle null bulk string ($-1).
	// This is valid RESP; we return empty string since our commands
	// don't distinguish between null and empty.
	if length == -1 {
		return "", nil
	}

	// Security: reject negative lengths (other than -1) and oversized strings.
	if length < 0 {
		return "", ErrInvalidSyntax
	}
	if length > MaxBulkLength {
		return "", ErrBulkTooLarge
	}

	// Read data + trailing CRLF in one syscall for efficiency.
	buf := make([]byte, length+2)
	if _, err := io.ReadFull(p.reader, buf); err != nil {
		return "", err
	}

	// Validate trailing CRLF for protocol strictness.
	if buf[length] != '\r' || buf[length+1] != '\n' {
		return "", ErrInvalidSyntax
	}

	return string(buf[:length]), nil
}
