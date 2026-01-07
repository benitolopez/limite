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
//   - EXPIRENX key ms:     Set TTL only if key has no expiry (NX)
//   - EXPIREXX key ms:     Set TTL only if key has an expiry (XX)
//   - EXPIREAT key ts:     Set TTL as absolute Unix timestamp (milliseconds)
//   - EXPIREATNX key ts:   Set absolute TTL only if key has no expiry (NX)
//   - EXPIREATXX key ts:   Set absolute TTL only if key has an expiry (XX)
//   - TTL key:             Get remaining TTL in milliseconds
//   - PERSIST key:         Remove expiration, making the key permanent
//
// Expiration Model
// ================
//
// Limite uses a hybrid expiration strategy:
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

// =============================================================================
// Relative Expiration (Duration in ms)
// =============================================================================

// handleExpire handles the EXPIRE command.
// Syntax: EXPIRE key milliseconds
func (app *application) handleExpire(w io.Writer, args []string) {
	app.expireGeneric(w, args, false, ExpireModeAlways, "EXPIRE")
}

// handleExpireNX handles the EXPIRENX command.
// Syntax: EXPIRENX key milliseconds
//
// Sets expiry only if the key has NO existing expiration.
func (app *application) handleExpireNX(w io.Writer, args []string) {
	app.expireGeneric(w, args, false, ExpireModeNX, "EXPIRENX")
}

// handleExpireXX handles the EXPIREXX command.
// Syntax: EXPIREXX key milliseconds
//
// Sets expiry only if the key already HAS an expiration.
func (app *application) handleExpireXX(w io.Writer, args []string) {
	app.expireGeneric(w, args, false, ExpireModeXX, "EXPIREXX")
}

// =============================================================================
// Absolute Expiration (Timestamp in ms)
// =============================================================================

// handleExpireAt handles the EXPIREAT command.
// Syntax: EXPIREAT key unix-time-milliseconds
func (app *application) handleExpireAt(w io.Writer, args []string) {
	app.expireGeneric(w, args, true, ExpireModeAlways, "EXPIREAT")
}

// handleExpireAtNX handles the EXPIREATNX command.
// Syntax: EXPIREATNX key unix-time-milliseconds
//
// Sets absolute expiry only if the key has NO existing expiration.
func (app *application) handleExpireAtNX(w io.Writer, args []string) {
	app.expireGeneric(w, args, true, ExpireModeNX, "EXPIREATNX")
}

// handleExpireAtXX handles the EXPIREATXX command.
// Syntax: EXPIREATXX key unix-time-milliseconds
//
// Sets absolute expiry only if the key already HAS an expiration.
func (app *application) handleExpireAtXX(w io.Writer, args []string) {
	app.expireGeneric(w, args, true, ExpireModeXX, "EXPIREATXX")
}

// =============================================================================
// Generic Implementation
// =============================================================================

// expireGeneric implements the core logic for all EXPIRE variants.
// Syntax: EXPIRE[AT][NX|XX] key value
func (app *application) expireGeneric(w io.Writer, args []string, isAbsolute bool, mode ExpiryMode, cmdName string) {
	//
	// DESIGN
	// ------
	//
	// All six EXPIRE commands (EXPIRE, EXPIRENX, EXPIREXX, EXPIREAT, EXPIREATNX,
	// EXPIREATXX) share the same core logic. The differences are:
	//
	//   - isAbsolute: Whether the value is a Unix timestamp or a relative duration
	//   - mode: Whether to check NX (no existing expiry) or XX (has expiry) conditions
	//
	// The NX/XX condition checking happens atomically inside Store.SetExpiry under
	// the shard lock. This prevents a TOCTOU race where we might check the condition,
	// release the lock, and have another goroutine modify the expiry before we set it.
	//
	// Edge case: When the computed expiry is in the past (negative duration or past
	// timestamp), we delete the key immediately. However, for NX/XX modes, we still
	// check the condition first. This means "EXPIRENX key -1" on a key that already
	// has an expiry will return 0 and NOT delete the key. This is intentional—the
	// NX condition failed, so no action is taken.
	//
	if len(args) != 2 {
		app.wrongNumberOfArgsResponse(w, cmdName)
		return
	}

	key := args[0]
	val, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		app.writeErrorResponse(w, "ERR value is not an integer or out of range")
		return
	}

	now := time.Now().UnixMilli()
	var absolute int64

	if isAbsolute {
		absolute = val
	} else {
		// Overflow guard: int64 addition can wrap around silently.
		if val > 0 && val > math.MaxInt64-now {
			app.writeErrorResponse(w, "ERR invalid expire time")
			return
		}
		absolute = now + val
	}

	// Past expiry: delete the key, but only if NX/XX condition passes.
	if absolute <= now {
		if !app.store.SetExpiry(key, absolute, mode) {
			_ = app.writeIntegerResponse(w, 0)
			return
		}
		app.store.Delete(key)
		app.logCommand("DEL", []string{key})
		_ = app.writeIntegerResponse(w, 1)
		return
	}

	if !app.store.SetExpiry(key, absolute, mode) {
		_ = app.writeIntegerResponse(w, 0)
		return
	}

	// Log as EXPIREAT for deterministic AOF replay regardless of original command.
	app.logCommand("EXPIREAT", []string{key, strconv.FormatInt(absolute, 10)})
	_ = app.writeIntegerResponse(w, 1)
}

// =============================================================================
// TTL Query
// =============================================================================

// handleTTL handles the TTL command.
// Syntax: TTL key
//
// Returns -2 if key doesn't exist, -1 if no expiry, else remaining ms.
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

	if expiry == -1 {
		_ = app.writeIntegerResponse(w, -1)
		return
	}

	// Clamp to zero if we raced past expiry between GetExpiry and now.
	remaining := expiry - time.Now().UnixMilli()
	if remaining < 0 {
		remaining = 0
	}

	_ = app.writeIntegerResponse64(w, remaining)
}

// =============================================================================
// Persist (Remove Expiration)
// =============================================================================

// handlePersist handles the PERSIST command.
// Syntax: PERSIST key
//
// Removes expiration from a key. Returns 1 if removed, 0 if key doesn't exist
// or has no expiration.
func (app *application) handlePersist(w io.Writer, args []string) {
	if len(args) != 1 {
		app.wrongNumberOfArgsResponse(w, "PERSIST")
		return
	}

	key := args[0]

	expiry, exists := app.store.GetExpiry(key)
	if !exists || expiry == -1 {
		_ = app.writeIntegerResponse(w, 0)
		return
	}

	app.store.SetExpiry(key, 0, ExpireModeAlways)
	app.logCommand("PERSIST", []string{key})
	_ = app.writeIntegerResponse(w, 1)
}
