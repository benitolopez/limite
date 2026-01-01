// handlers_string.go implements the basic string key-value commands.
//
// This file provides Redis-compatible string operations (SET, GET, INCR, DECR,
// INCRBY, DECRBY) built on top of the generic byte store. Each string value
// is prefixed with a 4-byte magic header "DATA" to distinguish it from other
// data types (HLL, Bloom Filter) and enable type checking at runtime.
//
// Storage Format
// ==============
//
// String values are stored as:
//
//	+------+---------------+
//	| DATA | Payload       |
//	+------+---------------+
//	 4 bytes  variable
//
// The "DATA" magic header serves two purposes:
//   1. Type identification: GET on an HLL key returns WRONGTYPE error
//   2. Future extensibility: We can add metadata between header and payload
//
// Concurrency
// ===========
//
// - SET uses Store.Set (exclusive lock, brief)
// - GET uses Store.Get (read lock, brief)
// - INCR/DECR use Store.Mutate (exclusive lock, atomic read-modify-write)
//
// Unlike GET, which could use View for zero-copy access, we use Get to
// release the lock before performing network I/O. This follows the pattern
// established in HLL.COUNT and provides better concurrency under load.

package main

import (
	"bytes"
	"io"
	"math"
	"strconv"
)

// stringMagic is the 4-byte header for standard Key-Value data.
// We use this to distinguish Strings/Ints/JSON from HLLs and Bloom Filters.
var stringMagic = []byte{'D', 'A', 'T', 'A'}

// handleSet handles the SET command.
// Syntax: SET key value
//
// Sets a key to hold a string value. If the key already holds a value, it is
// overwritten, regardless of its type. Any previous time to live associated
// with the key is discarded.
func (app *application) handleSet(w io.Writer, args []string) {
	if len(args) != 2 {
		app.wrongNumberOfArgsResponse(w, "SET")
		return
	}

	key, value := args[0], args[1]
	valBytes := []byte(value)

	// Prepend magic header: allocate exactly what we need
	storage := make([]byte, len(stringMagic)+len(valBytes))
	copy(storage, stringMagic)
	copy(storage[len(stringMagic):], valBytes)

	app.store.Set(key, storage)
	app.logCommand("SET", args)
	_ = app.writeSimpleStringResponse(w, "OK")
}

// handleGet handles the GET command.
// Syntax: GET key
//
// Returns the string value of key. If the key does not exist, nil is returned.
// If the key holds a value that is not a string (e.g., HLL), an error is returned.
func (app *application) handleGet(w io.Writer, args []string) {
	if len(args) != 1 {
		app.wrongNumberOfArgsResponse(w, "GET")
		return
	}

	key := args[0]

	// Use Get (not View) to release the lock before network I/O.
	// This follows the pattern in HLL.COUNT for better concurrency.
	data, found := app.store.Get(key)
	if !found {
		_ = app.writeNilResponse(w)
		return
	}

	// Validate magic header
	if len(data) < len(stringMagic) || !bytes.Equal(data[:len(stringMagic)], stringMagic) {
		app.wrongTypeResponse(w)
		return
	}

	// Return payload without the header
	_ = app.writeBulkBytesResponse(w, data[len(stringMagic):])
}

// handleIncr handles the INCR command.
// Syntax: INCR key
//
// Increments the integer value of key by one. If the key does not exist, it is
// set to 0 before performing the operation. An error is returned if the key
// contains a value of the wrong type or contains a string that cannot be
// represented as integer.
func (app *application) handleIncr(w io.Writer, args []string) {
	if len(args) != 1 {
		app.wrongNumberOfArgsResponse(w, "INCR")
		return
	}
	app.incrByGeneric(w, args[0], 1, "INCR", args)
}

// handleDecr handles the DECR command.
// Syntax: DECR key
//
// Decrements the integer value of key by one. If the key does not exist, it is
// set to 0 before performing the operation.
func (app *application) handleDecr(w io.Writer, args []string) {
	if len(args) != 1 {
		app.wrongNumberOfArgsResponse(w, "DECR")
		return
	}
	app.incrByGeneric(w, args[0], -1, "DECR", args)
}

// handleIncrBy handles the INCRBY command.
// Syntax: INCRBY key increment
//
// Increments the integer value of key by the given amount.
func (app *application) handleIncrBy(w io.Writer, args []string) {
	if len(args) != 2 {
		app.wrongNumberOfArgsResponse(w, "INCRBY")
		return
	}
	incr, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		_ = app.writeErrorResponse(w, "ERR value is not an integer or out of range")
		return
	}
	app.incrByGeneric(w, args[0], incr, "INCRBY", args)
}

// handleDecrBy handles the DECRBY command.
// Syntax: DECRBY key decrement
//
// Decrements the integer value of key by the given amount.
func (app *application) handleDecrBy(w io.Writer, args []string) {
	if len(args) != 2 {
		app.wrongNumberOfArgsResponse(w, "DECRBY")
		return
	}
	decr, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		_ = app.writeErrorResponse(w, "ERR value is not an integer or out of range")
		return
	}
	// DECRBY key N is equivalent to INCRBY key -N
	app.incrByGeneric(w, args[0], -decr, "DECRBY", args)
}

// incrByGeneric implements the core logic for INCR, DECR, INCRBY, and DECRBY.
//
// This function uses Store.Mutate to perform an atomic read-modify-write
// operation, preventing the "lost update" race condition that would occur
// if we used separate Get and Set calls.
//
// The delta parameter specifies the amount to add (positive) or subtract
// (negative) from the current value.
func (app *application) incrByGeneric(w io.Writer, key string, delta int64, cmdName string, args []string) {
	var resultValue int64
	var invalidIntErr, wrongTypeErr, overflowErr bool
	var storeUpdated bool

	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		var currentVal int64 = 0

		if data != nil {
			// Type check: must have DATA header
			if len(data) < len(stringMagic) || !bytes.Equal(data[:len(stringMagic)], stringMagic) {
				wrongTypeErr = true
				return data, false
			}

			// Parse the integer value from the payload
			val, err := strconv.ParseInt(string(data[len(stringMagic):]), 10, 64)
			if err != nil {
				invalidIntErr = true
				return data, false
			}
			currentVal = val
		}

		// Overflow check: detect if the operation would overflow int64
		// For positive delta: overflow if currentVal > MaxInt64 - delta
		// For negative delta: underflow if currentVal < MinInt64 - delta
		if (delta > 0 && currentVal > math.MaxInt64-delta) ||
			(delta < 0 && currentVal < math.MinInt64-delta) {
			overflowErr = true
			return data, false
		}

		resultValue = currentVal + delta
		storeUpdated = true

		// Serialize: [DATA header] + [ASCII integer]
		// Pre-allocate with capacity for header (4) + max int64 digits (20)
		newBuf := make([]byte, 0, len(stringMagic)+20)
		newBuf = append(newBuf, stringMagic...)
		newBuf = strconv.AppendInt(newBuf, resultValue, 10)
		return newBuf, true
	})

	// Handle errors after releasing the lock
	if wrongTypeErr {
		app.wrongTypeResponse(w)
		return
	}
	if invalidIntErr {
		_ = app.writeErrorResponse(w, "ERR value is not an integer or out of range")
		return
	}
	if overflowErr {
		_ = app.writeErrorResponse(w, "ERR increment or decrement would overflow")
		return
	}

	// Log to AOF only if the store was actually updated
	if storeUpdated {
		app.logCommand(cmdName, args)
	}

	_ = app.writeIntegerResponse(w, int(resultValue))
}
