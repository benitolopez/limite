// handlers_cms.go implements the Count-Min Sketch commands.
//
// This file provides CMS operations (CMS.INIT, CMS.INITBYPROB, CMS.INCRBY,
// CMS.QUERY) for frequency estimation. Count-Min Sketch is a probabilistic
// data structure that estimates the frequency of events in a data stream,
// trading accuracy for sub-linear space.
//
// This implementation uses the Conservative Update technique, which significantly
// reduces over-counting compared to standard CMS by only updating counters to
// the new minimum "floor" rather than incrementing all counters blindly.
//
// Storage Format
// ==============
// Each CMS key is stored as a serialized byte slice with a "CMS1" magic header
// prefix for type identification. The binary format is:
//   - Magic (4 bytes): 0x31534D43 ("CMS1" in Little Endian)
//   - Width (4 bytes): Number of columns
//   - Depth (4 bytes): Number of rows
//   - Count (8 bytes): Total items added
//   - Counters: Width * Depth uint32 values
//
// Concurrency Strategy
// ====================
// - CMS.INIT/CMS.INITBYPROB: Uses Mutate() to create if not exists
// - CMS.INCRBY: Uses Mutate() for atomic read-modify-write operations
// - CMS.QUERY: Uses View() for read-only access

package main

import (
	"io"
	"strconv"

	"pds.lopezb.com/internal/pds/cms"
)

// handleCMSInit handles the CMS.INIT command.
// Syntax: CMS.INIT key width depth
//
// Creates a new Count-Min Sketch with the specified dimensions.
// Returns an error if the key already exists.
func (app *application) handleCMSInit(w io.Writer, args []string) {
	if len(args) != 3 {
		app.wrongNumberOfArgsResponse(w, "CMS.INIT")
		return
	}

	key := args[0]

	width, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil || width == 0 {
		_ = app.writeErrorResponse(w, "ERR invalid width")
		return
	}

	depth, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil || depth == 0 {
		_ = app.writeErrorResponse(w, "ERR invalid depth")
		return
	}

	var keyExists bool

	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		if data != nil {
			keyExists = true
			return data, false
		}

		// Create new CMS
		c := cms.New(uint32(width), uint32(depth))
		return c.Bytes(), true
	})

	if keyExists {
		_ = app.writeErrorResponse(w, "ERR key already exists")
		return
	}

	app.logCommand("CMS.INIT", args)
	_ = app.writeSimpleStringResponse(w, "OK")
}

// handleCMSInitByProb handles the CMS.INITBYPROB command.
// Syntax: CMS.INITBYPROB key epsilon delta
//
// Creates a new Count-Min Sketch with dimensions calculated from error parameters.
// The epsilon value controls the error bound (e.g., 0.01 for 1% error) and delta
// controls the probability of exceeding that bound (e.g., 0.01 for 1% chance).
// Returns an error if the key already exists.
func (app *application) handleCMSInitByProb(w io.Writer, args []string) {
	if len(args) != 3 {
		app.wrongNumberOfArgsResponse(w, "CMS.INITBYPROB")
		return
	}

	key := args[0]

	epsilon, err := strconv.ParseFloat(args[1], 64)
	if err != nil || epsilon <= 0 || epsilon >= 1 {
		_ = app.writeErrorResponse(w, "ERR invalid epsilon (must be between 0 and 1)")
		return
	}

	delta, err := strconv.ParseFloat(args[2], 64)
	if err != nil || delta <= 0 || delta >= 1 {
		_ = app.writeErrorResponse(w, "ERR invalid delta (must be between 0 and 1)")
		return
	}

	width, depth := cms.DimensionsFromProb(epsilon, delta)

	var keyExists bool

	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		if data != nil {
			keyExists = true
			return data, false
		}

		c := cms.New(width, depth)
		return c.Bytes(), true
	})

	if keyExists {
		_ = app.writeErrorResponse(w, "ERR key already exists")
		return
	}

	app.logCommand("CMS.INITBYPROB", args)
	_ = app.writeSimpleStringResponse(w, "OK")
}

// handleCMSIncrBy handles the CMS.INCRBY command.
// Syntax: CMS.INCRBY key item increment [item increment ...]
//
// Increments the count of one or more items in the CMS.
// Returns an array of the estimated counts after increment.
// The key must exist (created via CMS.INIT or CMS.INITBYPROB).
func (app *application) handleCMSIncrBy(w io.Writer, args []string) {
	// Must have key + at least one (item, increment) pair
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		app.wrongNumberOfArgsResponse(w, "CMS.INCRBY")
		return
	}

	key := args[0]
	pairs := args[1:]

	// Pre-allocate results
	results := make([]int, 0, len(pairs)/2)

	var storeUpdated bool
	var typeError, notFoundError, parseError bool

	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		// Key must exist (no auto-create)
		if data == nil {
			notFoundError = true
			return nil, false
		}

		// Type check using magic bytes
		if !cms.HasValidMagic(data) {
			typeError = true
			return data, false
		}

		// Load CMS (zero-copy)
		c, err := cms.NewFromBytes(data)
		if err != nil {
			typeError = true
			return data, false
		}

		// Process item/increment pairs
		for i := 0; i < len(pairs); i += 2 {
			item := []byte(pairs[i])

			delta, err := strconv.ParseUint(pairs[i+1], 10, 32)
			if err != nil {
				parseError = true
				return data, false
			}

			// Perform Conservative Update
			if c.Incr(item, uint32(delta)) {
				storeUpdated = true
			}

			// Get current count for response
			results = append(results, int(c.Query(item)))
		}

		return c.Bytes(), storeUpdated
	})

	if notFoundError {
		_ = app.writeErrorResponse(w, "ERR key not found")
		return
	}
	if typeError {
		app.wrongTypeResponse(w)
		return
	}
	if parseError {
		_ = app.writeErrorResponse(w, "ERR invalid increment value")
		return
	}

	if storeUpdated {
		app.logCommand("CMS.INCRBY", args)
	}

	_ = app.writeIntegerArrayResponse(w, results)
}

// handleCMSQuery handles the CMS.QUERY command.
// Syntax: CMS.QUERY key item [item ...]
//
// Returns the estimated count for one or more items.
// If the key doesn't exist, returns 0 for all items.
func (app *application) handleCMSQuery(w io.Writer, args []string) {
	if len(args) < 2 {
		app.wrongNumberOfArgsResponse(w, "CMS.QUERY")
		return
	}

	key := args[0]
	items := args[1:]

	results := make([]int, len(items))
	var typeError bool

	// Use View for read-only access (shared lock)
	_ = app.store.View(key, func(data []byte) error {
		// Key doesn't exist - return all zeros
		if data == nil {
			return nil
		}

		// Type check
		if !cms.HasValidMagic(data) {
			typeError = true
			return nil
		}

		// Load CMS (zero-copy)
		c, err := cms.NewFromBytes(data)
		if err != nil {
			typeError = true
			return nil
		}

		// Query each item
		for i, item := range items {
			results[i] = int(c.Query([]byte(item)))
		}

		return nil
	})

	if typeError {
		app.wrongTypeResponse(w)
		return
	}

	_ = app.writeIntegerArrayResponse(w, results)
}
