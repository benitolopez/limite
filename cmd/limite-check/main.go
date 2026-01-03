// limite-check is a diagnostic tool for inspecting and validating Limite snapshot
// files. It performs a streaming verification of the binary preamble, checking
// structural integrity and the CRC64 checksum without loading data into memory.
//
// This tool is the first line of defense when troubleshooting persistence
// issues. It can answer questions like:
//
//   - Is the journal file corrupted?
//   - How many keys are stored in each shard?
//   - What data types are present (HyperLogLog, Bloom Filter, etc.)?
//   - Is there a text tail (Hybrid mode) after the binary section?
//
// Usage Examples
// ==============
//
// Basic validation (just checks structure and checksum):
//
//	limite-check -file journal.aof
//
// Verbose mode (lists all keys with their types):
//
//	limite-check -file journal.aof -v
//
// Dump mode (shows raw byte values, useful for debugging):
//
//	limite-check -file journal.aof -dump
//
// Exit Codes
// ==========
//
// 0: The file is valid.
// 1: The file is corrupted or unreadable (checksum mismatch, truncated, etc.)
//
// Hybrid AOF Support
// ==================
//
// This tool validates only the binary preamble portion of a Hybrid AOF file.
// If text commands follow the checksum (the "tail"), we detect their presence
// and report it, but we don't parse or validate the RESP data.

package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"time"
)

const (
	persistenceMagic = "LIM1"
	OpCodeShardData  = 0xFE
	OpCodeEOF        = 0xFF
)

// CountReader wraps an io.Reader to track the cumulative byte offset. This is
// used to report the exact file position in error messages, helping users
// pinpoint corruption locations for manual repair or forensic analysis.
type CountReader struct {
	r     io.Reader
	count int64
}

// Read implements io.Reader, passing through to the underlying reader while
// accumulating the byte count.
func (cr *CountReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.count += int64(n)
	return n, err
}

// ReadByte implements io.ByteReader. This is required because bufio.Reader
// uses ByteReader for single-byte reads when available, and we need to count
// those bytes too.
func (cr *CountReader) ReadByte() (byte, error) {
	var buf [1]byte
	n, err := cr.r.Read(buf[:])
	cr.count += int64(n)
	return buf[0], err
}

func main() {
	filePath := flag.String("file", "journal.aof", "Path to the AOF/Snapshot file")
	verbose := flag.Bool("v", false, "Verbose mode (print keys)")
	dump := flag.Bool("dump", false, "Show values (prints raw bytes as quoted strings)")
	flag.Parse()

	f, err := os.Open(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[err] Cannot open file: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()

	fmt.Printf("[offset 0] Checking Limite file %s\n", *filePath)

	// Pipeline: File -> CountReader -> Bufio
	// We verify the hash of the BINARY section manually.

	crcTable := crc64.MakeTable(crc64.ISO)
	hasher := crc64.New(crcTable)

	// Track offset for logging
	counter := &CountReader{r: f}

	// Buffer for performance
	reader := bufio.NewReader(counter)

	// Start by verifying the magic header bytes.
	header := make([]byte, len(persistenceMagic))
	if _, err := io.ReadFull(reader, header); err != nil {
		die(counter.count, "Failed to read header", err)
	}
	if string(header) != persistenceMagic {
		die(counter.count, fmt.Sprintf("Invalid Magic Header: expected '%s', got '%s'", persistenceMagic, header), nil)
	}
	hasher.Write(header) // Hash Header

	// Now iterate through shard blocks until we hit the EOF marker.
	lenBuf := make([]byte, 4)
	totalKeys := 0
	start := time.Now()
	stats := make(map[string]int)

	for {
		// Each block starts with an opcode byte.
		opcode, err := reader.ReadByte()
		if err != nil {
			die(counter.count, "Failed reading Opcode", err)
		}
		hasher.Write([]byte{opcode})

		// The EOF marker signals the end of the binary section.
		if opcode == OpCodeEOF {
			break
		}

		// Any other opcode besides ShardData indicates corruption.
		if opcode != OpCodeShardData {
			die(counter.count, fmt.Sprintf("Unexpected Opcode: %x", opcode), nil)
		}

		// Read which shard this block belongs to.
		shardIDByte, err := reader.ReadByte()
		if err != nil {
			die(counter.count, "Failed reading Shard ID", err)
		}
		hasher.Write([]byte{shardIDByte})
		shardID := int(shardIDByte)

		// Read how many keys are in this shard block.
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			die(counter.count, "Failed reading key count", err)
		}
		hasher.Write(lenBuf)
		count := binary.LittleEndian.Uint32(lenBuf)

		if count > 0 {
			fmt.Printf("[offset %d] Processing Shard %d: %d keys\n", counter.count, shardID, count)
		}

		// Process each key-value pair in this shard.
		for i := uint32(0); i < count; i++ {
			// Key: length prefix followed by raw bytes.
			if _, err := io.ReadFull(reader, lenBuf); err != nil {
				die(counter.count, "Truncated key len", err)
			}
			hasher.Write(lenBuf)
			kLen := binary.LittleEndian.Uint32(lenBuf)

			keyBuf := make([]byte, kLen)
			if _, err := io.ReadFull(reader, keyBuf); err != nil {
				die(counter.count, "Truncated key data", err)
			}
			hasher.Write(keyBuf)

			// Value: same structure as key.
			if _, err := io.ReadFull(reader, lenBuf); err != nil {
				die(counter.count, "Truncated val len", err)
			}
			hasher.Write(lenBuf)
			vLen := binary.LittleEndian.Uint32(lenBuf)

			valBuf := make([]byte, vLen)
			if _, err := io.ReadFull(reader, valBuf); err != nil {
				die(counter.count, "Truncated val data", err)
			}
			hasher.Write(valBuf)

			totalKeys++

			typeName, details := identifyType(valBuf)
			stats[typeName]++

			if *verbose || *dump {
				info := ""
				if details != "" {
					info = fmt.Sprintf("(%s)", details)
				}
				fmt.Printf("[offset %d] Key '%s' [%s] %s\n", counter.count, string(keyBuf), typeName, info)
			}

			if *dump {
				fmt.Printf("      Value: %q\n", valBuf)
			}
		}
	}

	// The checksum follows immediately after the EOF marker. Since we've been
	// feeding every byte to the hasher, we can now compare against the stored value.
	calculatedChecksum := hasher.Sum64()

	storedChecksumBytes := make([]byte, 8)
	if _, err := io.ReadFull(reader, storedChecksumBytes); err != nil {
		die(counter.count, "Failed to read checksum", err)
	}
	storedChecksum := binary.LittleEndian.Uint64(storedChecksumBytes)

	if storedChecksum != calculatedChecksum {
		fmt.Printf("[offset %d] Checksum MISMATCH\n", counter.count)
		fmt.Printf("   File:       %016x\n", storedChecksum)
		fmt.Printf("   Calculated: %016x\n", calculatedChecksum)
		os.Exit(1)
	}

	fmt.Printf("[offset %d] Checksum OK (%016x)\n", counter.count, storedChecksum)
	fmt.Printf("[offset %d] Binary Snapshot looks OK\n", counter.count)

	// Check if there's any data after the checksum. In Hybrid mode, RESP text
	// commands follow the binary section.
	_, err = reader.Peek(1)
	if err == nil {
		fmt.Printf("[offset %d] Found AOF Text Tail (Hybrid Mode)\n", counter.count)
		fmt.Println("             (Text data verification is skipped by this tool)")
	} else if err != io.EOF {
		fmt.Printf("[warn] Error checking for tail: %v\n", err)
	}

	fmt.Println("\nSummary:")
	fmt.Printf("  Process Time: %v\n", time.Since(start))
	fmt.Printf("  Total Keys:   %d\n", totalKeys)
	for t, c := range stats {
		fmt.Printf("    %d\t%s\n", c, t)
	}
}

// Bloom Filter magic: "BLOOM001" as uint64 little-endian = 0x424C4F4F4D303031
const bloomMagic = 0x424C4F4F4D303031

// Count-Min Sketch magic: "CMS1" as uint32 little-endian = 0x31534D43
const cmsMagic = 0x31534D43

// Top-K magic: "TOPK" as uint32 little-endian = 0x4B504F54
const topkMagic = 0x4B504F54

// identifyType inspects the raw bytes of a value to determine its data type.
// Limite data structures embed type markers (magic bytes) at the start of their
// serialized form, allowing us to identify them without additional metadata.
//
// Currently recognized types:
//   - "DATA" prefix: String (plain key-value data)
//   - "HYLL" prefix: HyperLogLog (Sparse or Dense encoding)
//   - "BLOOM001" magic: Bloom Filter (Scalable Blocked)
//   - "CMS1" magic: Count-Min Sketch
//   - "TOPK" magic: Top-K (HeavyKeeper algorithm)
//   - Otherwise: Raw (unknown or custom data)
//
// For HLLs, we also extract the cached cardinality from the header if available.
// For Bloom Filters, we extract the total items and number of layers.
// For Count-Min Sketches, we extract width, depth, and total count.
// For Top-K, we extract K, width, depth, and heap size.
// For Strings, we show the value (truncated if too long) and indicate if it's numeric.
func identifyType(data []byte) (string, string) {
	// Check for String magic ("DATA" at offset 0)
	if len(data) >= 4 && string(data[0:4]) == "DATA" {
		payload := data[4:]

		// Check if the payload is a valid integer (for INCR/DECR values)
		if len(payload) > 0 && isIntegerString(payload) {
			return "String", fmt.Sprintf("Int:%s", string(payload))
		}

		// For non-numeric strings, show length and a preview
		if len(payload) == 0 {
			return "String", "Len:0 (empty)"
		}

		// Show a preview of the value (truncated if too long)
		const maxPreview = 24
		preview := string(payload)
		if len(preview) > maxPreview {
			preview = preview[:maxPreview] + "..."
		}

		return "String", fmt.Sprintf("Len:%d Val:%q", len(payload), preview)
	}

	// Check for HyperLogLog magic ("HYLL" at offset 0)
	if len(data) >= 4 && string(data[0:4]) == "HYLL" {
		// Byte 4 is the encoding flag: 0 = Dense, 1 = Sparse
		subtype := "Sparse"
		if len(data) > 4 && data[4] == 0 {
			subtype = "Dense"
		}

		// The cached cardinality is stored in bytes 8-15 (little-endian uint64).
		// Bit 63 is a "dirty" flag, so we mask it out to get the actual value.
		details := ""
		if len(data) >= 16 {
			card := binary.LittleEndian.Uint64(data[8:16])
			card = card & ^(uint64(1) << 63)
			details = fmt.Sprintf("Card:~%d", card)
		}

		return "HLL-" + subtype, details
	}

	// Check for Bloom Filter magic (0x424C4F4F4D303031 = "BLOOM001")
	// Layout: Magic(8) + TotalItems(8) + NumLayers(8) = 24 bytes header
	if len(data) >= 24 {
		magic := binary.LittleEndian.Uint64(data[0:8])
		if magic == bloomMagic {
			totalItems := binary.LittleEndian.Uint64(data[8:16])
			numLayers := binary.LittleEndian.Uint64(data[16:24])
			details := fmt.Sprintf("Items:%d, Layers:%d", totalItems, numLayers)
			return "BloomFilter", details
		}
	}

	// Check for Count-Min Sketch magic (0x31534D43 = "CMS1")
	// Layout: Magic(4) + Width(4) + Depth(4) + Count(8) = 20 bytes header
	if len(data) >= 20 {
		magic := binary.LittleEndian.Uint32(data[0:4])
		if magic == cmsMagic {
			width := binary.LittleEndian.Uint32(data[4:8])
			depth := binary.LittleEndian.Uint32(data[8:12])
			count := binary.LittleEndian.Uint64(data[12:20])
			details := fmt.Sprintf("Width:%d, Depth:%d, Count:%d", width, depth, count)
			return "CountMinSketch", details
		}
	}

	// Check for Top-K magic (0x4B504F54 = "TOPK")
	// Layout: Magic(4) + K(4) + Width(4) + Depth(4) + Decay(8) + HeapN(4) = 28 bytes header
	if len(data) >= 28 {
		magic := binary.LittleEndian.Uint32(data[0:4])
		if magic == topkMagic {
			k := binary.LittleEndian.Uint32(data[4:8])
			width := binary.LittleEndian.Uint32(data[8:12])
			depth := binary.LittleEndian.Uint32(data[12:16])
			heapN := binary.LittleEndian.Uint32(data[24:28])
			details := fmt.Sprintf("K:%d, Width:%d, Depth:%d, HeapSize:%d", k, width, depth, heapN)
			return "TopK", details
		}
	}

	return "Raw", ""
}

// isIntegerString checks if a byte slice represents a valid integer string.
// This is used to identify INCR/DECR values vs plain strings.
func isIntegerString(b []byte) bool {
	if len(b) == 0 {
		return false
	}

	start := 0
	if b[0] == '-' || b[0] == '+' {
		if len(b) == 1 {
			return false // Just a sign, no digits
		}
		start = 1
	}

	for i := start; i < len(b); i++ {
		if b[i] < '0' || b[i] > '9' {
			return false
		}
	}

	return true
}

// die prints a fatal error message with the current file offset and exits.
// The offset helps users locate the exact byte position of corruption.
func die(offset int64, msg string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "[offset %d] Fatal: %s: %v\n", offset, msg, err)
	} else {
		fmt.Fprintf(os.Stderr, "[offset %d] Fatal: %s\n", offset, msg)
	}
	os.Exit(1)
}
