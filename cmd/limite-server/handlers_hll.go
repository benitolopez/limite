// handlers_hll.go implements the HyperLogLog commands.
//
// This file provides Redis-compatible HyperLogLog operations (HLL.ADD, HLL.COUNT,
// HLL.MERGE) for cardinality estimation. HyperLogLog is a probabilistic data
// structure that can estimate the number of unique elements in a set using
// a fixed amount of memory (approximately 12KB per key).
//
// Storage Format
// ==============
// Each HyperLogLog key is stored as a serialized byte slice with a "HYLL" magic
// header prefix for type identification. The internal format supports both
// sparse and dense representations, automatically upgrading as cardinality grows.
//
// Concurrency Strategy
// ====================
// - HLL.ADD: Uses Mutate() for atomic read-modify-write operations
// - HLL.COUNT: Optimistic read with cached cardinality, falls back to Mutate()
//   only when the cache is dirty
// - HLL.MERGE: Two-phase operation - reads sources with View(), then updates
//   destination with Mutate()

package main

import (
	"fmt"
	"io"

	"limite.lopezb.com/internal/limite/hyperloglog"
)

// handleHLLAdd handles the HLL.ADD command.
// Syntax: HLL.ADD key element [element ...]
func (app *application) handleHLLAdd(w io.Writer, args []string) {
	//
	// DESIGN
	// ------
	//
	// This handler serves as the synchronization point between the concurrent
	// network layer and the HyperLogLog logic. To ensure data consistency,
	// it employs a strict Read-Modify-Write pattern using the Store's `Mutate`
	// primitive.
	//
	// The lifecycle of an ADD operation involves:
	// 1. Lazy Creation: If a key is missing, it is implicitly created.
	// 2. Type Checking: Existing keys are verified via the "HYLL" magic string.
	// 3. Batch Update: All elements are processed in a single atomic transaction.
	// 4. Conditional Persistence: We only serialize and write back to the
	//    store if the internal state actually changes, optimizing for the
	//    common case where elements are already present.
	//

	if len(args) < 2 {
		app.wrongNumberOfArgsResponse(w, "HLL.ADD")
		return
	}

	key := args[0]
	elements := args[1:]

	var registersChanged int
	var storeUpdated bool
	var typeError, decodeError bool

	// We acquire a Write Lock on the specific key to prevent race conditions
	// (the "Lost Update" problem). The callback receives the current raw bytes
	// and returns the modified bytes to be stored.
	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		var hll *hyperloglog.HLL
		var err error

		// Determine if we are initializing a new structure or loading an
		// existing one. If the data is nil, we assume a new key. If data
		// exists, we validate the magic string to prevent corrupting non-HLL
		// keys before deserializing.
		if data == nil {
			hll = hyperloglog.NewWithThreshold(app.config.hllSparseThreshold)
		} else {
			if len(data) < 4 || string(data[0:4]) != hyperloglog.Magic {
				typeError = true
				// Returning false ensures we do not write back to the store.
				return data, false
			}
			hll, err = hyperloglog.Deserialize(data)
			if err != nil {
				decodeError = true
				return data, false
			}
		}

		// Iterate over all provided elements. The `hll.Add` method returns
		// true if that specific insertion altered the internal register state.
		// We track this to satisfy the Redis protocol, which requires returning
		// 1 if at least one register was modified, and 0 otherwise.
		modified := false
		for _, el := range elements {
			if hll.Add([]byte(el)) {
				modified = true
			}
		}

		// We only incur the CPU cost of serialization if the data actually
		// changed. The exception is when `data == nil`: if we created a new
		// key, we must save it even if `modified` is false (which can happen
		// if the first item hashes to a zero-rank).
		if modified {
			registersChanged = 1
			storeUpdated = true
			return hll.Serialize(), true
		} else if data == nil {
			storeUpdated = true
			return hll.Serialize(), true
		}

		// If nothing changed and the key already existed, we return false
		// to tell the Store to skip the map assignment.
		return data, false
	})

	if typeError {
		app.wrongTypeResponse(w)
		return
	}
	if decodeError {
		_ = app.writeErrorResponse(w, "ERR internal HLL corruption")
		return
	}

	if storeUpdated {
		app.logCommand("HLL.ADD", args)
	}

	_ = app.writeIntegerResponse(w, registersChanged)
}

// handleHLLCount handles the HLL.COUNT command.
// Syntax: HLL.COUNT key
func (app *application) handleHLLCount(w io.Writer, args []string) {
	//
	// DESIGN
	// ------
	//
	// The command implements two distinct strategies based on the input arguments:
	//
	// A. MULTI-KEY (MERGE STRATEGY)
	//    If multiple keys are provided, we compute the cardinality of their union.
	//    We create a temporary "Raw Accumulator" and merge all HLLs into it using
	//    the rule Union[i] = MAX(HLL_A[i], HLL_B[i]). We then count the result from
	//    this temporary structure without modifying or writing back to the source keys.
	//
	// B. SINGLE-KEY (CACHE STRATEGY)
	//    Calculating HyperLogLog cardinality requires iterating over 16,384 registers
	//    and performing floating-point math. Doing this on every request would degrade
	//    performance and block the server if done under a lock.
	//
	//    To solve this, we employ an "Optimistic Read, Fallback to Write" strategy
	//    with Double-Checked Locking.
	//
	//    1. Fast Path (Read Lock): We peek at the HLL header. If the "dirty bit"
	//       is clear, the cached cardinality is valid. We return it immediately
	//       allowing high concurrency (multiple readers).
	//
	//    2. Slow Path (Write Lock): If the cache is invalid (dirty), we acquire
	//       exclusive access to recompute the value and save it back to the store.
	//       This ensures the expensive computation happens only once per update cycle.
	//

	if len(args) < 1 {
		app.wrongNumberOfArgsResponse(w, "HLL.COUNT")
		return
	}

	// CASE 1: MULTI-KEY MERGE
	if len(args) > 1 {
		// Instead of allocating 16KB per request (which causes massive GC pressure
		// at high throughput), we grab a pre-allocated buffer from the pool.
		maxRegisters := hyperloglog.GetAccumulator()
		defer hyperloglog.PutAccumulator(maxRegisters)

		for _, key := range args {
			// Acquire a Read Lock for the key.
			data, found := app.store.Get(key)
			if !found {
				continue // Treat missing keys as empty HLLs (cardinality 0).
			}

			// Validate type.
			if len(data) < 4 || string(data[0:4]) != hyperloglog.Magic {
				app.wrongTypeResponse(w)
				return
			}

			// Deserialize the HLL.
			hll, err := hyperloglog.Deserialize(data)
			if err != nil {
				_ = app.writeErrorResponse(w, "ERR internal HLL corruption")
				return
			}

			// Merge this HLL's state into our temporary accumulator.
			hll.MergeInto(*maxRegisters)
		}

		// Calculate the cardinality of the accumulator using the 'Raw' mode.
		// NewRaw creates a wrapper around the byte slice so we can reuse the Count() logic.
		tempHLL := hyperloglog.NewRaw(*maxRegisters)
		count := tempHLL.Count()

		_ = app.writeIntegerResponse(w, int(count))
		return
	}

	// CASE 2: SINGLE KEY OPTIMIZATION
	key := args[0]

	// We attempt to retrieve the value using a shared Read Lock (`Get`).
	data, found := app.store.Get(key)
	if !found {
		_ = app.writeIntegerResponse(w, 0)
		return
	}

	if len(data) < 4 || string(data[0:4]) != hyperloglog.Magic {
		app.wrongTypeResponse(w)
		return
	}

	// We inspect the header bits to determine if the cached value is trusted.
	// If `valid` is true, the dirty bit is 0, and we can return the count
	// instantly without deserializing the full payload or upgrading our lock.
	if count, valid := hyperloglog.GetCachedCount(data); valid {
		_ = app.writeIntegerResponse(w, int(count))
		return
	}

	// The cache is stale. We must escalate to a Write Lock (`Mutate`) to
	// perform the computation and persist the result. This effectively "heals"
	// the cache for subsequent readers.
	var count uint64
	var decodeError bool

	app.store.Mutate(key, func(currData []byte) ([]byte, bool) {
		// If the key was deleted by another client while we were waiting for
		// the lock, we abort the operation.
		if currData == nil {
			return nil, false
		}

		// Another client may have acquired the write lock and updated the cache
		// in the time window between our `Get` and this `Mutate`. We re-check
		// the header before doing any heavy work. If it is now valid,
		// we simply return the new value and exit without writing.
		if cached, valid := hyperloglog.GetCachedCount(currData); valid {
			count = cached
			return currData, false
		}

		// The cache is genuinely dirty. We must deserialize the full struct,
		// run the Ertl estimation algorithm (which is CPU intensive), and
		// update the internal state.
		hll, err := hyperloglog.Deserialize(currData)
		if err != nil {
			decodeError = true
			return currData, false
		}

		// Compute the cardinality. Note that `hll.Count()` automatically
		// clears the dirty bit in the header of the struct.
		// We then serialize the struct back to bytes. This incurs a memory
		// copy cost (allocating ~12KB), but saves significant CPU time for
		// all future read operations.
		count = hll.Count()
		return hll.Serialize(), true
	})

	if decodeError {
		_ = app.writeErrorResponse(w, "ERR internal HLL corruption")
		return
	}

	_ = app.writeIntegerResponse(w, int(count))
}

// handleHLLMerge handles the HLL.MERGE command.
// Syntax: HLL.MERGE destKey srcKey [srcKey ...]
func (app *application) handleHLLMerge(w io.Writer, args []string) {
	//
	// DESIGN
	// ------
	//
	// This operation combines N source HLLs into a single destination HLL.
	// The execution is split into two distinct phases to prevent Deadlocks
	// and maximize throughput.
	//
	// 1. PHASE 1: READ SOURCES (Non-Blocking)
	//    We iterate through all source keys (args[1:]), acquiring a Read Lock
	//    for each. We merge their states into a temporary "Raw Accumulator"
	//    (16KB byte slice). This phase performs the heavy lifting (deserialization
	//    and merging) while allowing concurrent access to the store.
	//
	// 2. PHASE 2: WRITE DESTINATION (Exclusive)
	//    Once the accumulator contains the union of all sources, we acquire
	//    a Write Lock on the destination key (args[0]). This phase is highly
	//    optimized to be "Zero-Allocation" by updating the destination's
	//    existing byte slice in-place whenever possible.
	//

	if len(args) < 2 {
		app.wrongNumberOfArgsResponse(w, "HLL.MERGE")
		return
	}

	// 1. GET ACCUMULATOR
	// We use the pool to get a 16KB zeroed buffer. This avoids allocating
	// a large slice for every request, reducing GC pressure.
	accumulator := hyperloglog.GetAccumulator()
	defer hyperloglog.PutAccumulator(accumulator)

	// 2. PHASE 1: READ SOURCES
	// Iterate through all source keys. We use Read Locks to allow concurrency.
	for _, key := range args[1:] {
		// Variable to capture specific errors inside the closure
		var logicErr error

		// We use View to hold the Read Lock for the duration of the operation.
		// This is required to safely use DeserializeUnsafe.
		_ = app.store.View(key, func(data []byte) error {
			if data == nil {
				return nil // Continue to next key
			}

			if !hyperloglog.HasValidMagic(data) {
				logicErr = fmt.Errorf("WRONGTYPE")
				return logicErr
			}

			// Optimization: DeserializeUnsafe
			// We only read from the HLL to merge into our local accumulator.
			// We do not modify the HLL, so pointing directly to Store memory
			// using unsafe deserialization avoids a 12KB copy per key.
			hll, err := hyperloglog.DeserializeUnsafe(data)
			if err != nil {
				logicErr = fmt.Errorf("CORRUPTION")
				return logicErr
			}

			hll.MergeInto(*accumulator)
			return nil
		})

		// Check if we encountered an error inside the lock
		if logicErr != nil {
			if logicErr.Error() == "WRONGTYPE" {
				app.wrongTypeResponse(w)
				return
			}
			if logicErr.Error() == "CORRUPTION" {
				_ = app.writeErrorResponse(w, "ERR internal hll corruption")
				return
			}
		}
	}

	var typeError bool
	var decodeError bool
	var storeUpdated bool

	// 3. PHASE 2: UPDATE DESTINATION
	// We acquire the write lock only for the destination key.
	app.store.Mutate(args[0], func(currData []byte) ([]byte, bool) {
		// A. CASE: NEW KEY
		// If destination doesn't exist, we compress the accumulator into a new slice.
		if currData == nil {
			storeUpdated = true
			// Pass 'nil' for reuseBuffer to force allocation.
			return hyperloglog.CompressToSerialized(*accumulator, nil), true
		}

		// B. CASE: EXISTING KEY
		// Validate magic string.
		if len(currData) < 4 || string(currData[0:4]) != hyperloglog.Magic {
			typeError = true
			return nil, false
		}

		// Check Encoding: Sparse Fallback
		// In-place updates require the destination to be Dense (Byte 4 == 0).
		// If it is Sparse, we must upgrade it. This path is rare under load.
		if currData[4] != 0 { // 0 is hllDense
			hll, err := hyperloglog.DeserializeUnsafe(currData)
			if err != nil {
				decodeError = true
				return nil, false
			}
			hll.MergeInto(*accumulator)
			storeUpdated = true
			// We pass 'nil' because the existing Sparse buffer is too small
			// to hold the new Dense result.
			return hyperloglog.CompressToSerialized(*accumulator, nil), true
		}

		// OPTIMIZATION: IN-PLACE UPDATE (Dense)
		// We point directly to the data part of the existing slice.
		// UpdateDenseFromAccumulator reads the old values, compares with the
		// accumulator, and updates bytes ONLY if the Max increases.
		denseData := currData[16:] // Skip 16-byte header

		changed := hyperloglog.UpdateDenseFromAccumulator(denseData, *accumulator)

		// If nothing changed (common in repeated merges), we return false.
		// This prevents the Store from broadcasting updates.
		if !changed {
			return currData, false
		}

		// If changed, we must mark the cache as invalid/dirty.
		// The MSB of the last byte (index 15) is the dirty bit.
		currData[15] |= 0x80
		storeUpdated = true

		return currData, true
	})

	if typeError {
		app.wrongTypeResponse(w)
		return
	}
	if decodeError {
		_ = app.writeErrorResponse(w, "ERR internal hll corruption")
		return
	}

	// We only write to disk if the memory state actually changed.
	if storeUpdated {
		app.logCommand("HLL.MERGE", args)
	}

	_ = app.writeSimpleStringResponse(w, "OK")
}
