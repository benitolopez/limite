package main

import (
	"strconv"
	"strings"
)

// encodeCommand serializes a command and its arguments into a RESP (Redis Serialization Protocol) Array.
//
// This function is the "Serializer" for our Persistence layer. It takes Go data structures
// (strings) and converts them into the wire format needed for the AOF file.
//
// Format:
//   - Array Header: "*" + <number_of_elements> + "\r\n"
//   - Bulk Strings: "$" + <string_length> + "\r\n" + <string_content> + "\r\n"
//
// Example:
//
//	Input:  "SET", ["key", "val"]
//	Output: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n"
func encodeCommand(command string, args []string) []byte {
	var sb strings.Builder

	// Optimization: Pre-allocate a small buffer to avoid resizing for small commands.
	// 64 bytes covers most simple commands like "HLL.ADD key value".
	sb.Grow(64)

	// 1. Write the Array Header
	// The total number of elements is 1 (the command itself) + the number of arguments.
	// Example: *3\r\n
	sb.WriteString("*")
	sb.WriteString(strconv.Itoa(len(args) + 1))
	sb.WriteString("\r\n")

	// 2. Write the Command as the first Bulk String
	// Example: $3\r\nSET\r\n
	sb.WriteString("$")
	sb.WriteString(strconv.Itoa(len(command)))
	sb.WriteString("\r\n")
	sb.WriteString(command)
	sb.WriteString("\r\n")

	// 3. Write each Argument as a Bulk String
	// We handle empty strings correctly here (len 0), writing "$0\r\n\r\n".
	for i := 0; i < len(args); i++ {
		sb.WriteString("$")
		sb.WriteString(strconv.Itoa(len(args[i])))
		sb.WriteString("\r\n")
		sb.WriteString(args[i])
		sb.WriteString("\r\n")
	}

	// Convert the builder's buffer directly to a byte slice.
	return []byte(sb.String())
}
