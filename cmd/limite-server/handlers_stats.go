// handlers_stats.go implements memory inspection commands.
//
// This file provides the MEMORY command for inspecting memory usage of stored
// keys. This is useful for debugging and capacity planning.
//
// Concurrency Strategy
// ====================
// All commands use View() for read-only access with shared locking, ensuring
// minimal impact on concurrent operations.

package main

import (
	"fmt"
	"io"
	"strings"
)

// handleMemory handles the MEMORY command.
// Syntax: MEMORY USAGE <key>
func (app *application) handleMemory(w io.Writer, args []string) {
	if len(args) < 1 {
		app.wrongNumberOfArgsResponse(w, "MEMORY")
		return
	}

	subcommand := strings.ToUpper(args[0])
	subArgs := args[1:]

	switch subcommand {
	case "USAGE":
		app.handleMemoryUsage(w, subArgs)
	default:
		msg := fmt.Sprintf("ERR unknown subcommand '%s'. Try MEMORY USAGE <key>", subcommand)
		_ = app.writeErrorResponse(w, msg)
	}
}

// handleMemoryUsage handles MEMORY USAGE <key>.
// Syntax: MEMORY USAGE <key>
func (app *application) handleMemoryUsage(w io.Writer, args []string) {
	//
	// DESIGN
	// ------
	//
	// We report an approximation of the total memory consumed by a key,
	// including the overhead from Go's internal data structures. This follows
	// the Redis MEMORY USAGE semantics: returns nil for missing keys, and an
	// integer byte count for existing ones.
	//
	// The overhead constant (72 bytes) accounts for:
	// - String header for the key (16 bytes)
	// - Slice header for the value (24 bytes)
	// - Map bucket overhead per entry (~32 bytes)
	//

	if len(args) != 1 {
		_ = app.writeErrorResponse(w, "ERR wrong number of arguments for 'MEMORY USAGE' command")
		return
	}

	key := args[0]
	var size int
	found := false

	const mapOverhead = 72

	_ = app.store.View(key, func(data []byte) error {
		if data != nil {
			found = true
			size = len(key) + len(data) + mapOverhead
		}
		return nil
	})

	if !found {
		_ = app.writeNilResponse(w)
		return
	}

	_ = app.writeIntegerResponse(w, size)
}
