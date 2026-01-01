// handlers_topk.go implements the Top-K HeavyKeeper commands for heavy hitter
// detection. Top-K is a probabilistic data structure that efficiently tracks
// the most frequent items in a data stream.
//
// The implementation uses HeavyKeeper with pre-computed decay tables for fast
// probability lookups and linear heap scanning for cache-friendly membership
// checks. Each TopK key is stored as a serialized byte slice with a "TOPK"
// magic header. Write operations (RESERVE, ADD) use Mutate() for atomic
// read-modify-write; read operations (QUERY, LIST) use View() for lock-free
// access to the serialized data.

package main

import (
	"io"
	"strconv"
	"sync"

	"pds.lopezb.com/internal/pds/topk"
)

// respBufferPool reuses byte slices for RESP responses to reduce GC pressure.
var respBufferPool = sync.Pool{
	New: func() any {
		// 4KB initial size handles most responses.
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

// handleTopKReserve creates a new Top-K structure with the specified parameters.
// Returns an error if the key already exists. Optional parameters allow tuning
// width, depth, and decay; if omitted, defaults are used (width=2048, depth=5,
// decay=0.9).
func (app *application) handleTopKReserve(w io.Writer, args []string) {
	if len(args) < 2 {
		app.wrongNumberOfArgsResponse(w, "TOPK.RESERVE")
		return
	}

	key := args[0]

	k, err := strconv.Atoi(args[1])
	if err != nil || k <= 0 {
		_ = app.writeErrorResponse(w, "ERR k must be a positive integer")
		return
	}

	cfg := topk.Config{
		K:     k,
		Width: 2048,
		Depth: 5,
		Decay: 0.9,
	}

	// Parse optional parameters [width depth decay]
	if len(args) > 2 {
		if len(args) != 5 {
			app.wrongNumberOfArgsResponse(w, "TOPK.RESERVE")
			return
		}

		width, err := strconv.Atoi(args[2])
		if err != nil || width <= 0 {
			_ = app.writeErrorResponse(w, "ERR invalid width")
			return
		}
		cfg.Width = width

		depth, err := strconv.Atoi(args[3])
		if err != nil || depth <= 0 {
			_ = app.writeErrorResponse(w, "ERR invalid depth")
			return
		}
		cfg.Depth = depth

		decay, err := strconv.ParseFloat(args[4], 64)
		if err != nil || decay <= 0 || decay >= 1 {
			_ = app.writeErrorResponse(w, "ERR decay must be between 0 and 1")
			return
		}
		cfg.Decay = decay
	}

	var keyExists bool

	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		if data != nil {
			keyExists = true
			return data, false
		}

		tk := topk.New(cfg)
		return tk.Bytes(), true
	})

	if keyExists {
		_ = app.writeErrorResponse(w, "ERR key already exists")
		return
	}

	app.logCommand("TOPK.RESERVE", args)
	_ = app.writeSimpleStringResponse(w, "OK")
}

// handleTopKAdd adds items to a Top-K sketch, auto-creating with defaults if
// the key doesn't exist. Returns an array with one entry per item: nil if the
// item entered or stayed in TopK, or the expelled item's key as a string if
// something was kicked out (or the item itself failed to enter).
func (app *application) handleTopKAdd(w io.Writer, args []string) {
	if len(args) < 2 {
		app.wrongNumberOfArgsResponse(w, "TOPK.ADD")
		return
	}

	key := args[0]
	items := args[1:]

	var expelled []topk.AddResult
	var storeUpdated bool
	var typeError bool

	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		var tk *topk.TopK
		var err error

		if data == nil {
			// Auto-create with default config
			tk = topk.New(topk.DefaultConfig())
		} else {
			// Type check
			if !topk.HasValidMagic(data) {
				typeError = true
				return data, false
			}

			tk, err = topk.NewFromBytes(data)
			if err != nil {
				typeError = true
				return data, false
			}
		}

		expelled = tk.Add(items)
		storeUpdated = true

		return tk.Bytes(), true
	})

	if typeError {
		app.wrongTypeResponse(w)
		return
	}

	if storeUpdated {
		app.logCommand("TOPK.ADD", args)
	}

	app.writeTopKAddResponse(w, expelled)
}

// handleTopKQuery checks if items are currently in the Top-K list. Returns an
// array of integers (1 if present, 0 otherwise) in the same order as the input.
func (app *application) handleTopKQuery(w io.Writer, args []string) {
	if len(args) < 2 {
		app.wrongNumberOfArgsResponse(w, "TOPK.QUERY")
		return
	}

	key := args[0]
	items := args[1:]

	results := make([]int, len(items))
	var typeError bool

	_ = app.store.View(key, func(data []byte) error {
		if data == nil {
			// Key doesn't exist - all items return 0
			return nil
		}

		if !topk.HasValidMagic(data) {
			typeError = true
			return nil
		}

		tk, err := topk.NewFromBytes(data)
		if err != nil {
			typeError = true
			return nil
		}

		for i, item := range items {
			found, _ := tk.Query(item)
			if found {
				results[i] = 1
			}
		}

		return nil
	})

	if typeError {
		app.wrongTypeResponse(w)
		return
	}

	_ = app.writeIntegerArrayResponse(w, results)
}

// handleTopKIncrBy adds items with specific increment values to a Top-K sketch.
// Syntax: TOPK.INCRBY key item increment [item increment ...]
// Returns an array with one entry per item: nil if the item entered or stayed in
// TopK, or the expelled item's key as a string if something was kicked out.
func (app *application) handleTopKIncrBy(w io.Writer, args []string) {
	// Args: key item1 incr1 item2 incr2 ...
	// Minimum 3 args (key + 1 pair)
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		app.wrongNumberOfArgsResponse(w, "TOPK.INCRBY")
		return
	}

	key := args[0]
	numPairs := (len(args) - 1) / 2
	items := make([]string, numPairs)
	increments := make([]uint64, numPairs)

	for i := 0; i < numPairs; i++ {
		items[i] = args[1+i*2]
		val, err := strconv.ParseUint(args[2+i*2], 10, 64)
		if err != nil {
			_ = app.writeErrorResponse(w, "ERR increment must be a positive integer")
			return
		}
		increments[i] = val
	}

	var expelled []topk.AddResult
	var storeUpdated bool
	var typeError bool

	app.store.Mutate(key, func(data []byte) ([]byte, bool) {
		var tk *topk.TopK
		var err error

		if data == nil {
			// Auto-create with default config
			tk = topk.New(topk.DefaultConfig())
		} else {
			if !topk.HasValidMagic(data) {
				typeError = true
				return data, false
			}
			tk, err = topk.NewFromBytes(data)
			if err != nil {
				typeError = true
				return data, false
			}
		}

		expelled = tk.AddWeighted(items, increments)
		storeUpdated = true

		return tk.Bytes(), true
	})

	if typeError {
		app.wrongTypeResponse(w)
		return
	}

	if storeUpdated {
		app.logCommand("TOPK.INCRBY", args)
	}

	app.writeTopKAddResponse(w, expelled)
}

// handleTopKList returns the current heavy hitters sorted by count (descending).
// With the WITHCOUNT option, returns alternating key/count pairs.
func (app *application) handleTopKList(w io.Writer, args []string) {
	if len(args) < 1 {
		app.wrongNumberOfArgsResponse(w, "TOPK.LIST")
		return
	}

	key := args[0]
	withCount := false

	if len(args) > 1 {
		if len(args) > 2 {
			app.wrongNumberOfArgsResponse(w, "TOPK.LIST")
			return
		}
		switch args[1] {
		case "WITHCOUNT", "withcount":
			withCount = true
		default:
			_ = app.writeErrorResponse(w, "ERR syntax error")
			return
		}
	}

	var list []topk.Item
	var typeError bool

	_ = app.store.View(key, func(data []byte) error {
		if data == nil {
			return nil
		}

		if !topk.HasValidMagic(data) {
			typeError = true
			return nil
		}

		tk, err := topk.NewFromBytes(data)
		if err != nil {
			typeError = true
			return nil
		}

		list = tk.List()
		return nil
	})

	if typeError {
		app.wrongTypeResponse(w)
		return
	}

	app.writeTopKListResponse(w, list, withCount)
}

// writeTopKAddResponse formats the TOPK.ADD and TOPK.INCRBY response as an
// array of bulk strings (expelled keys) or null bulk strings (nil entries).
// Uses a pooled buffer to reduce GC pressure under high pipeline load.
func (app *application) writeTopKAddResponse(w io.Writer, expelled []topk.AddResult) {
	// Get buffer from pool and reset length.
	bufp := respBufferPool.Get().(*[]byte)
	buf := (*bufp)[:0]

	// Array header
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(expelled)), 10)
	buf = append(buf, '\r', '\n')

	for _, result := range expelled {
		if !result.Expelled {
			// Null bulk string
			buf = append(buf, "$-1\r\n"...)
		} else {
			// Bulk string with key
			buf = append(buf, '$')
			buf = strconv.AppendInt(buf, int64(len(result.Key)), 10)
			buf = append(buf, '\r', '\n')
			buf = append(buf, result.Key...)
			buf = append(buf, '\r', '\n')
		}
	}

	_, _ = w.Write(buf)

	// Return buffer to pool if not too large (prevent memory hoarding).
	if cap(buf) <= 65536 {
		*bufp = buf
		respBufferPool.Put(bufp)
	}
}

// writeTopKListResponse formats the TOPK.LIST response as an array of keys,
// or alternating key/count pairs when withCount is true.
func (app *application) writeTopKListResponse(w io.Writer, list []topk.Item, withCount bool) {
	if list == nil {
		// Empty array for missing key
		_, _ = w.Write([]byte("*0\r\n"))
		return
	}

	count := len(list)
	if withCount {
		count *= 2
	}

	// Estimate buffer size
	buf := make([]byte, 0, 6+count*20)

	// Array header
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(count), 10)
	buf = append(buf, '\r', '\n')

	for _, item := range list {
		// Key as bulk string
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(item.Key)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, item.Key...)
		buf = append(buf, '\r', '\n')

		if withCount {
			// Count as integer
			buf = append(buf, ':')
			buf = strconv.AppendUint(buf, item.Count, 10)
			buf = append(buf, '\r', '\n')
		}
	}

	_, _ = w.Write(buf)
}
