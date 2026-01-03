// handlers_bloom.go implements the Bloom Filter commands.
//
// This file provides Bloom Filter operations (BF.ADD, BF.MADD, BF.EXISTS,
// BF.MEXISTS) for probabilistic set membership testing. Bloom filters can
// definitively say when an element is NOT in a set, but may have false
// positives when checking if an element IS in a set.
//
// Storage Format
// ==============
// Each Bloom Filter key is stored as a serialized byte slice managed by the
// scalable filter implementation. The filter automatically grows by adding
// new sub-filters as the number of elements increases, maintaining the
// configured false positive rate.
//
// Concurrency Strategy
// ====================
// - BF.ADD: Uses Mutate() for atomic read-modify-write operations
// - BF.MADD: Uses Mutate() for atomic batch add with single lock acquisition
// - BF.EXISTS: Uses View() for read-only access with shared locking
// - BF.MEXISTS: Uses View() for read-only batch access with shared locking

package main

import (
	"io"

	"limite.lopezb.com/internal/limite/bloom"
)

// handleBFAdd handles the BF.ADD command.
// Syntax: BF.ADD key item
//
// Returns 1 if the element was added to the filter, or 0 if it was already present.
func (app *application) handleBFAdd(w io.Writer, args []string) {
	if len(args) != 2 {
		app.wrongNumberOfArgsResponse(w, "BF.ADD")
		return
	}

	key := args[0]
	item := args[1]

	var wasAdded int
	var storeUpdated bool
	var logicErr error

	// Prepare bloom filter config from server settings.
	bfConfig := bloom.Config{
		InitialCapacity: app.config.bfInitialCapacity,
		ErrorRate:       app.config.bfErrorRate,
	}

	// Use Mutate to ensure thread-safety during the read-modify-write cycle.
	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		// Load or create the filter. If data is nil, NewScalableFilter
		// creates a fresh filter with the configured settings.
		filter, err := bloom.NewScalableFilter(data, bfConfig)
		if err != nil {
			logicErr = err
			return data, false
		}
		// Return pooled resources when done with this scope.
		defer filter.Release()

		// Add the item. The filter handles hashing internally and returns
		// whether the element was newly added or already present.
		newData, added, err := filter.Add([]byte(item))
		if err != nil {
			logicErr = err
			return data, false
		}

		if added {
			wasAdded = 1
			storeUpdated = true
			return newData, true
		}

		return data, false
	})

	if logicErr != nil {
		_ = app.writeErrorResponse(w, "ERR "+logicErr.Error())
		return
	}

	if storeUpdated {
		app.logCommand("BF.ADD", args)
	}

	_ = app.writeIntegerResponse(w, wasAdded)
}

// handleBFMAdd handles the BF.MADD command.
// Syntax: BF.MADD key item [item ...]
//
// Adds one or more items to a Bloom Filter in a single atomic operation.
// Returns an array of integers, where each integer corresponds to an input item:
// 1 if the item was newly added, 0 if it was already present.
func (app *application) handleBFMAdd(w io.Writer, args []string) {
	if len(args) < 2 {
		app.wrongNumberOfArgsResponse(w, "BF.MADD")
		return
	}

	key := args[0]
	items := args[1:]

	// Pre-allocate results slice to match input item count.
	results := make([]int, len(items))
	var storeUpdated bool
	var logicErr error

	// Prepare bloom filter config from server settings.
	bfConfig := bloom.Config{
		InitialCapacity: app.config.bfInitialCapacity,
		ErrorRate:       app.config.bfErrorRate,
	}

	// Use Mutate to ensure thread-safety during the batch read-modify-write cycle.
	// All items are processed within a single lock acquisition for efficiency.
	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		// Load or create the filter. If data is nil, NewScalableFilter
		// creates a fresh filter with the configured settings.
		filter, err := bloom.NewScalableFilter(data, bfConfig)
		if err != nil {
			logicErr = err
			return data, false
		}
		// Return pooled resources when done with this scope.
		defer filter.Release()

		// Track the current backing slice. The filter may reallocate during
		// growth, so we capture the final slice after all additions.
		currentData := data

		// Process each item in order. The results array maintains the same
		// order as the input items for correct response mapping.
		for i, item := range items {
			newData, added, err := filter.Add([]byte(item))
			if err != nil {
				logicErr = err
				return data, false
			}

			// Always update backing slice reference in case of reallocation.
			currentData = newData

			if added {
				results[i] = 1
				storeUpdated = true
			} else {
				results[i] = 0
			}
		}

		if storeUpdated {
			return currentData, true
		}
		return data, false
	})

	if logicErr != nil {
		_ = app.writeErrorResponse(w, "ERR "+logicErr.Error())
		return
	}

	if storeUpdated {
		app.logCommand("BF.MADD", args)
	}

	_ = app.writeIntegerArrayResponse(w, results)
}

// handleBFExists handles the BF.EXISTS command.
// Syntax: BF.EXISTS key item
//
// Returns 1 if the element is probably in the filter (subject to false positive
// rate), or 0 if the element is definitely not present or the key does not exist.
func (app *application) handleBFExists(w io.Writer, args []string) {
	if len(args) != 2 {
		app.wrongNumberOfArgsResponse(w, "BF.EXISTS")
		return
	}

	key := args[0]
	item := args[1]

	// Use View to hold the read lock while we access the data.
	// This prevents races with concurrent Mutate operations (BF.ADD).
	// Multiple BF.EXISTS can run in parallel (RLock), but block if BF.ADD is active.
	var result int

	err := app.store.View(key, func(data []byte) error {
		if data == nil {
			result = 0
			return nil
		}

		// Load the filter view. NewScalableFilter is zero-copy, so this is cheap.
		// Config is passed for validation, though Check doesn't use it for logic.
		bfConfig := bloom.Config{
			InitialCapacity: app.config.bfInitialCapacity,
			ErrorRate:       app.config.bfErrorRate,
		}

		filter, err := bloom.NewScalableFilter(data, bfConfig)
		if err != nil {
			return err
		}
		defer filter.Release()

		if filter.Check([]byte(item)) {
			result = 1
		} else {
			result = 0
		}
		return nil
	})
	if err != nil {
		_ = app.writeErrorResponse(w, "ERR "+err.Error())
		return
	}

	_ = app.writeIntegerResponse(w, result)
}

// handleBFMExists handles the BF.MEXISTS command.
// Syntax: BF.MEXISTS key item [item ...]
//
// Tests if one or more items exist in a Bloom Filter. Returns an array of
// integers, where each integer corresponds to an input item: 1 if the element
// is probably in the filter (subject to false positive rate), 0 if the element
// is definitely not present. If the key does not exist, returns all zeros.
func (app *application) handleBFMExists(w io.Writer, args []string) {
	if len(args) < 2 {
		app.wrongNumberOfArgsResponse(w, "BF.MEXISTS")
		return
	}

	key := args[0]
	items := args[1:]

	// Pre-allocate results slice with zeros (default: not found).
	results := make([]int, len(items))

	// Use View to hold the read lock while we access the data.
	// This prevents races with concurrent Mutate operations (BF.ADD/BF.MADD).
	// Multiple BF.MEXISTS can run in parallel (RLock), but block if writes are active.
	err := app.store.View(key, func(data []byte) error {
		// If the key doesn't exist, results remain all zeros.
		if data == nil {
			return nil
		}

		// Load the filter view. NewScalableFilter is zero-copy, so this is cheap.
		bfConfig := bloom.Config{
			InitialCapacity: app.config.bfInitialCapacity,
			ErrorRate:       app.config.bfErrorRate,
		}

		filter, err := bloom.NewScalableFilter(data, bfConfig)
		if err != nil {
			return err
		}
		defer filter.Release()

		// Check each item. The results array maintains the same order as the
		// input items for correct response mapping.
		for i, item := range items {
			if filter.Check([]byte(item)) {
				results[i] = 1
			}
		}
		return nil
	})
	if err != nil {
		_ = app.writeErrorResponse(w, "ERR "+err.Error())
		return
	}

	_ = app.writeIntegerArrayResponse(w, results)
}
