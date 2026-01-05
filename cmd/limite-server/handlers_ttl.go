// handlers_ttl.go implements the TTL (Time-To-Live) related commands.
//
// This file provides expiration functionality for keys, enabling automatic
// deletion after a specified time. All times are in milliseconds for
// high-precision use cases like rate limiting and sliding windows.
//
// Commands
// ========
//
//   - EXPIRE key ms:       Set TTL relative to now (in milliseconds)
//   - EXPIREAT key ts:     Set TTL as absolute Unix timestamp (milliseconds)
//   - TTL key:             Get remaining TTL in milliseconds
//   - PERSIST key:         Remove expiration, making the key permanent
//
// Expiration Model
// ================
//
// Limite uses a hybrid expiration strategy inspired by Redis:
//
//   1. Lazy Expiration: When a key is accessed (GET, etc.), we check if it
//      has expired. If so, we treat it as non-existent. This is cheap but
//      means expired keys may linger in memory if never accessed.
//
//   2. Active Expiration: A background goroutine runs every 100ms, sampling
//      random keys and deleting expired ones. This uses adaptive sampling:
//      sample 20 keys per shard, repeat if >10% were expired.
//
// AOF Logging
// ===========
//
// For deterministic replay, EXPIRE commands are logged as EXPIREAT with the
// computed absolute timestamp. This ensures consistent behavior regardless of
// when the AOF is replayed.
//
// If a key's expiration is set to a time in the past (or negative duration),
// the key is immediately deleted and logged as DEL.

package main

import (
	"io"
	"math"
	"strconv"
	"time"
)

// handleExpire handles the EXPIRE command.
// Syntax: EXPIRE key milliseconds
//
// Sets a timeout on a key. After the timeout has expired, the key will be
// automatically deleted. Returns 1 if the timeout was set, 0 if the key
// does not exist.
//
// If milliseconds is negative or zero, the key is deleted immediately.
func (app *application) handleExpire(w io.Writer, args []string) {
	if len(args) != 2 {
		app.wrongNumberOfArgsResponse(w, "EXPIRE")
		return
	}

	key := args[0]
	ms, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		app.writeErrorResponse(w, "ERR value is not an integer or out of range")
		return
	}

	now := time.Now().UnixMilli()

	// Overflow guard: adding ms to now must not wrap around.
	// We only check positive values since negative ms means "delete now".
	if ms > 0 && ms > math.MaxInt64-now {
		app.writeErrorResponse(w, "ERR invalid expire time")
		return
	}

	absolute := now + ms

	// Check if key exists before attempting to set expiry
	if !app.store.Exists(key) {
		_ = app.writeIntegerResponse(w, 0)
		return
	}

	// If the computed expiry is in the past (negative or zero ms), we delete
	// the key immediately rather than setting a meaningless expiration.
	if absolute <= now {
		app.store.Delete(key)
		app.logCommand("DEL", []string{key})
		_ = app.writeIntegerResponse(w, 1)
		return
	}

	// Set the expiry and log as EXPIREAT for deterministic replay
	app.store.SetExpiry(key, absolute)
	app.logCommand("EXPIREAT", []string{key, strconv.FormatInt(absolute, 10)})
	_ = app.writeIntegerResponse(w, 1)
}

// handleExpireAt handles the EXPIREAT command.
// Syntax: EXPIREAT key unix-time-milliseconds
//
// Sets an absolute Unix timestamp (in milliseconds) at which the key will
// expire. Returns 1 if the timeout was set, 0 if the key does not exist.
//
// If the timestamp is in the past, the key is deleted immediately.
func (app *application) handleExpireAt(w io.Writer, args []string) {
	if len(args) != 2 {
		app.wrongNumberOfArgsResponse(w, "EXPIREAT")
		return
	}

	key := args[0]
	timestamp, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		app.writeErrorResponse(w, "ERR value is not an integer or out of range")
		return
	}

	if !app.store.Exists(key) {
		_ = app.writeIntegerResponse(w, 0)
		return
	}

	now := time.Now().UnixMilli()

	// Timestamp in the past means the key should already be gone.
	// Delete it and log as DEL for clarity.
	if timestamp <= now {
		app.store.Delete(key)
		app.logCommand("DEL", []string{key})
		_ = app.writeIntegerResponse(w, 1)
		return
	}

	app.store.SetExpiry(key, timestamp)
	app.logCommand("EXPIREAT", []string{key, strconv.FormatInt(timestamp, 10)})
	_ = app.writeIntegerResponse(w, 1)
}

// handleTTL handles the TTL command.
// Syntax: TTL key
//
// Returns the remaining time to live of a key in milliseconds.
//
// Return values:
//   - -2 if the key does not exist
//   - -1 if the key exists but has no associated expiration
//   - Remaining TTL in milliseconds otherwise
func (app *application) handleTTL(w io.Writer, args []string) {
	if len(args) != 1 {
		app.wrongNumberOfArgsResponse(w, "TTL")
		return
	}

	key := args[0]

	expiry, exists := app.store.GetExpiry(key)
	if !exists {
		_ = app.writeIntegerResponse(w, -2)
		return
	}

	// expiry == -1 means key exists but has no expiry set
	if expiry == -1 {
		_ = app.writeIntegerResponse(w, -1)
		return
	}

	// Calculate remaining TTL, clamping to zero if we're past expiry
	// (can happen in the tiny window between GetExpiry and now)
	remaining := expiry - time.Now().UnixMilli()
	if remaining < 0 {
		remaining = 0
	}

	_ = app.writeIntegerResponse64(w, remaining)
}

// handlePersist handles the PERSIST command.
// Syntax: PERSIST key
//
// Removes the expiration from a key, making it persist indefinitely.
// Returns 1 if the timeout was removed, 0 if the key does not exist
// or has no associated timeout.
func (app *application) handlePersist(w io.Writer, args []string) {
	if len(args) != 1 {
		app.wrongNumberOfArgsResponse(w, "PERSIST")
		return
	}

	key := args[0]

	// Check current expiry state
	expiry, exists := app.store.GetExpiry(key)
	if !exists {
		_ = app.writeIntegerResponse(w, 0)
		return
	}

	// Key exists but has no expiry - nothing to remove
	if expiry == -1 {
		_ = app.writeIntegerResponse(w, 0)
		return
	}

	// Remove the expiry by setting it to 0
	app.store.SetExpiry(key, 0)
	app.logCommand("PERSIST", []string{key})
	_ = app.writeIntegerResponse(w, 1)
}
