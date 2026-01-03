// Package topk implements the HeavyKeeper algorithm for Top-K heavy hitter detection.
//
// HeavyKeeper is a probabilistic data structure that efficiently tracks the most
// frequent items (heavy hitters) in a data stream. It combines a Count-Min Sketch-like
// bucket array with probabilistic decay to prevent low-frequency items from polluting
// the counters of heavy hitters.
//
// Performance Optimization: Lazy Heap Deserialization
// ====================================================
//
// The critical insight is that most items in a stream are "mice" - low-frequency items
// that will never enter the Top-K heap. For these items, we can skip heap parsing
// entirely by scanning the raw bytes to check existence and comparing against the
// minimum heap count.
//
// Only when an item actually needs to modify the heap (new entry, count update, or
// eviction) do we "hydrate" the heap into Go structs. This optimization dramatically
// reduces allocations for the common case.
//
// Binary Format
// =============
//
//	+-------+-------+-------+-------+-------+-------+-----------+-----------+
//	| Magic | K     | Width | Depth | Decay | HeapN | Buckets   | Heap...   |
//	| 4B    | 4B    | 4B    | 4B    | 8B    | 4B    | W*D*16B   | Variable  |
//	+-------+-------+-------+-------+-------+-------+-----------+-----------+
//	                        Header (28 bytes)
//
// Buckets start at fixed offset 28, enabling direct zero-copy access.
// Each bucket is 16 bytes: Fingerprint(8B) + Count(8B).
// Heap items follow buckets: KeyLen(4B) + Key(var) + Count(8B) + Fingerprint(8B).
package topk

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

const (
	// Magic identifies TopK data. "TOPK" in Little Endian.
	Magic uint32 = 0x4B504F54

	// HeaderSize is the fixed header: Magic(4) + K(4) + Width(4) + Depth(4) + Decay(8) + HeapN(4).
	HeaderSize = 28

	// decayLookupSize covers counts 0-255 with O(1) lookup.
	decayLookupSize = 256
)

var (
	ErrInvalidData  = errors.New("topk: invalid data")
	ErrInvalidMagic = errors.New("topk: invalid magic")

	// globalSeed is an atomic counter for RNG seeding, avoiding time.Now() syscall.
	globalSeed uint64 = 1

	// defaultDecayThresholds is the pre-computed decay table for decay=0.9.
	// This avoids 256 math.Pow calls on every NewFromBytes() for the common case.
	defaultDecayThresholds [decayLookupSize]uint64

	// decayTableCache caches decay tables for non-default decay values.
	// Key: math.Float64bits(decay), Value: *[256]uint64
	decayTableCache sync.Map
)

func init() {
	// Pre-compute decay thresholds for the default decay=0.9.
	for i := 0; i < decayLookupSize; i++ {
		prob := math.Pow(0.9, float64(i))
		if prob >= 1.0 {
			defaultDecayThresholds[i] = math.MaxUint64
		} else {
			defaultDecayThresholds[i] = uint64(prob * float64(math.MaxUint64))
		}
	}
}

// AddResult holds the result of adding an item to TopK.
// Using a value type with a flag avoids heap allocation for the common nil case.
type AddResult struct {
	Expelled bool   // True if an item was expelled (or failed to enter)
	Key      string // The expelled item's key (only valid if Expelled is true)
	Count    uint64 // The expelled item's count
}

// Config holds initialization parameters.
type Config struct {
	K     int
	Width int
	Depth int
	Decay float64
}

// DefaultConfig returns sensible defaults: K=50, width=2048, depth=5, decay=0.9.
func DefaultConfig() Config {
	return Config{K: 50, Width: 2048, Depth: 5, Decay: 0.9}
}

// TopK implements HeavyKeeper with zero-copy bucket access and lazy heap parsing.
type TopK struct {
	backing []byte // Header + Buckets + Heap serialized

	// Cached header values for fast access.
	k     int
	width int
	depth int
	decay float64

	// widthMask enables fast modulo via bitwise AND when width is power of 2.
	// If width is power of 2: widthMask = width-1, use (hash & widthMask).
	// Otherwise: widthMask = 0, use (hash % width).
	widthMask uint64

	// Heap state. If heapHydrated is false, heap data resides only in backing.
	// If heapHydrated is true, heap slice is authoritative.
	heap         list
	heapHydrated bool
	heapDirty    bool // If true, Bytes() must rebuild the heap section

	// Integer-based decay thresholds for fast RNG comparison.
	// Pointer to shared table (8 bytes) instead of embedded array (2KB copy).
	decayThresholds *[decayLookupSize]uint64
	rngState        uint64
}

// New creates a TopK with the given configuration.
func New(cfg Config) *TopK {
	if cfg.K <= 0 {
		cfg.K = 50
	}
	if cfg.Width <= 0 {
		cfg.Width = 2048
	}
	if cfg.Depth <= 0 {
		cfg.Depth = 5
	}
	if cfg.Decay <= 0 || cfg.Decay >= 1 {
		cfg.Decay = 0.9
	}

	bucketsSize := cfg.Width * cfg.Depth * 16
	totalSize := HeaderSize + bucketsSize

	backing := make([]byte, totalSize)

	// Write header.
	binary.LittleEndian.PutUint32(backing[0:4], Magic)
	binary.LittleEndian.PutUint32(backing[4:8], uint32(cfg.K))
	binary.LittleEndian.PutUint32(backing[8:12], uint32(cfg.Width))
	binary.LittleEndian.PutUint32(backing[12:16], uint32(cfg.Depth))
	binary.LittleEndian.PutUint64(backing[16:24], math.Float64bits(cfg.Decay))
	binary.LittleEndian.PutUint32(backing[24:28], 0) // HeapN = 0

	tk := &TopK{
		backing:      backing,
		k:            cfg.K,
		width:        cfg.Width,
		depth:        cfg.Depth,
		decay:        cfg.Decay,
		heap:         make(list, 0, cfg.K),
		heapHydrated: true, // New structure starts with empty, valid heap
		rngState:     atomic.AddUint64(&globalSeed, 1),
	}

	tk.initWidthMask()
	tk.initDecayThresholds()
	return tk
}

// NewFromBytes wraps existing data with lazy heap parsing. Heap is NOT parsed
// until actually needed, enabling zero-alloc paths for "mice" (small items).
func NewFromBytes(data []byte) (*TopK, error) {
	if len(data) < HeaderSize {
		return nil, ErrInvalidData
	}

	if binary.LittleEndian.Uint32(data[0:4]) != Magic {
		return nil, ErrInvalidMagic
	}

	k := int(binary.LittleEndian.Uint32(data[4:8]))
	width := int(binary.LittleEndian.Uint32(data[8:12]))
	depth := int(binary.LittleEndian.Uint32(data[12:16]))
	decay := math.Float64frombits(binary.LittleEndian.Uint64(data[16:24]))

	if k <= 0 || width <= 0 || depth <= 0 {
		return nil, ErrInvalidData
	}

	bucketsSize := width * depth * 16
	heapStart := HeaderSize + bucketsSize

	if len(data) < heapStart {
		return nil, ErrInvalidData
	}

	tk := &TopK{
		backing:      data,
		k:            k,
		width:        width,
		depth:        depth,
		decay:        decay,
		heap:         nil,   // Lazy - not parsed yet
		heapHydrated: false, // Flag indicating heap is still in bytes
		rngState:     atomic.AddUint64(&globalSeed, 1),
	}

	tk.initWidthMask()
	tk.initDecayThresholds()
	return tk, nil
}

// initWidthMask sets widthMask for fast modulo when width is power of 2.
// Using bitwise AND instead of modulo avoids expensive DIV instruction.
func (tk *TopK) initWidthMask() {
	// Check if width is power of 2: (width & (width-1)) == 0
	if (tk.width & (tk.width - 1)) == 0 {
		tk.widthMask = uint64(tk.width - 1)
	} else {
		tk.widthMask = math.MaxUint64 // Signal to use modulo
	}
}

// initDecayThresholds sets the decay threshold pointer.
// Uses pointer to shared table (8 bytes) instead of copying 2KB array.
// For the common case of decay=0.9, uses the pre-computed global table.
func (tk *TopK) initDecayThresholds() {
	// Fast path: use pre-computed table for default decay=0.9.
	if tk.decay == 0.9 {
		tk.decayThresholds = &defaultDecayThresholds
		return
	}

	// Check cache for this decay value.
	decayBits := math.Float64bits(tk.decay)
	if cached, ok := decayTableCache.Load(decayBits); ok {
		tk.decayThresholds = cached.(*[decayLookupSize]uint64)
		return
	}

	// Slow path: compute and cache new decay table.
	newTable := &[decayLookupSize]uint64{}
	for i := 0; i < decayLookupSize; i++ {
		prob := math.Pow(tk.decay, float64(i))
		if prob >= 1.0 {
			newTable[i] = math.MaxUint64
		} else {
			newTable[i] = uint64(prob * float64(math.MaxUint64))
		}
	}
	decayTableCache.Store(decayBits, newTable)
	tk.decayThresholds = newTable
}

// fastDecayCheck returns true if decay should occur using Xorshift RNG.
func (tk *TopK) fastDecayCheck(count uint64) bool {
	// Xorshift64 step
	x := tk.rngState
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	tk.rngState = x

	if count < decayLookupSize {
		return x < tk.decayThresholds[count]
	}

	// Large count fallback - compute threshold on the fly
	prob := math.Pow(tk.decay, float64(count))
	threshold := uint64(prob * float64(math.MaxUint64))
	return x < threshold
}

// hydrateHeap parses the heap from backing slice into Go structs.
func (tk *TopK) hydrateHeap() {
	if tk.heapHydrated {
		return
	}

	bucketsSize := tk.width * tk.depth * 16
	offset := HeaderSize + bucketsSize
	heapN := int(binary.LittleEndian.Uint32(tk.backing[24:28]))

	tk.heap = make(list, 0, tk.k)

	for i := 0; i < heapN; i++ {
		kLen := int(binary.LittleEndian.Uint32(tk.backing[offset:]))
		offset += 4

		key := string(tk.backing[offset : offset+kLen])
		offset += kLen

		count := binary.LittleEndian.Uint64(tk.backing[offset:])
		offset += 8

		fp := binary.LittleEndian.Uint64(tk.backing[offset:])
		offset += 8

		tk.heap = append(tk.heap, Item{Key: key, Count: count, Fingerprint: fp})
	}

	tk.heapHydrated = true
}

// rawHeapSearch scans serialized heap for a key without allocating strings.
// Returns (found, count). This is the key optimization for "mice".
func (tk *TopK) rawHeapSearch(key string) (bool, uint64) {
	bucketsSize := tk.width * tk.depth * 16
	offset := HeaderSize + bucketsSize
	heapN := int(binary.LittleEndian.Uint32(tk.backing[24:28]))
	targetLen := len(key)

	for i := 0; i < heapN; i++ {
		kLen := int(binary.LittleEndian.Uint32(tk.backing[offset:]))
		offset += 4

		if kLen == targetLen {
			// Go 1.20+ optimizes string(slice) == string to zero-alloc memcmp.
			if string(tk.backing[offset:offset+kLen]) == key {
				offset += kLen
				count := binary.LittleEndian.Uint64(tk.backing[offset:])
				return true, count
			}
		}

		// Skip: Key + Count(8) + FP(8)
		offset += kLen + 16
	}

	return false, 0
}

// getMinHeapCount returns the count of the heap root (minimum) from raw bytes.
func (tk *TopK) getMinHeapCount() uint64 {
	bucketsSize := tk.width * tk.depth * 16
	offset := HeaderSize + bucketsSize

	// First item: KeyLen(4) + Key + Count(8)
	kLen := int(binary.LittleEndian.Uint32(tk.backing[offset:]))
	offset += 4 + kLen
	return binary.LittleEndian.Uint64(tk.backing[offset:])
}

// HasValidMagic returns true if data starts with the TopK magic bytes.
func HasValidMagic(data []byte) bool {
	return len(data) >= 4 && binary.LittleEndian.Uint32(data[0:4]) == Magic
}

// Add inserts items using the optimized lazy path where possible.
// For "mice" (items that won't enter the heap), this avoids all heap allocations.
func (tk *TopK) Add(items []string) []AddResult {
	expelled := make([]AddResult, len(items))

	// Cache header values to avoid repeated binary reads.
	heapN := int(binary.LittleEndian.Uint32(tk.backing[24:28]))
	var minHeapCount uint64

	// Gatekeeper optimization: if heap is full and not hydrated, peek at min count.
	// This allows skipping rawHeapSearch entirely for mice.
	canOptimize := !tk.heapHydrated && heapN == tk.k
	if canOptimize {
		minHeapCount = tk.getMinHeapCount()
	}

	// Cache struct fields to stack to avoid pointer chasing in the hot loop.
	depth := uint64(tk.depth)
	width := uint64(tk.width)
	widthMask := tk.widthMask

	// Bounds Check Elimination (BCE) hint: prove the slice is large enough once,
	// so the compiler can remove bounds checks inside the tight inner loop.
	bucketsEnd := HeaderSize + int(width*depth*16)
	_ = tk.backing[bucketsEnd-1]

	for i, key := range items {
		// Use Sum64String to avoid []byte(key) allocation.
		h64 := xxhash.Sum64String(key)
		fingerprint := h64
		var maxCount uint64

		// Step 1: Update HeavyKeeper buckets (inlined for performance)
		for d := uint64(0); d < depth; d++ {
			hashed := mix(h64 ^ d)
			var idx uint64
			if widthMask != math.MaxUint64 {
				idx = hashed & widthMask // Fast path: bitwise AND for power-of-2
			} else {
				idx = hashed % width // Slow path: modulo for non-power-of-2
			}
			off := HeaderSize + int((d*width+idx)<<4)

			fp := binary.LittleEndian.Uint64(tk.backing[off:])
			cnt := binary.LittleEndian.Uint64(tk.backing[off+8:])

			if cnt == 0 {
				binary.LittleEndian.PutUint64(tk.backing[off:], fingerprint)
				binary.LittleEndian.PutUint64(tk.backing[off+8:], 1)
				if 1 > maxCount {
					maxCount = 1
				}
			} else if fp == fingerprint {
				cnt++
				binary.LittleEndian.PutUint64(tk.backing[off+8:], cnt)
				if cnt > maxCount {
					maxCount = cnt
				}
			} else {
				if tk.fastDecayCheck(cnt) {
					cnt--
					if cnt == 0 {
						binary.LittleEndian.PutUint64(tk.backing[off:], fingerprint)
						binary.LittleEndian.PutUint64(tk.backing[off+8:], 1)
						if 1 > maxCount {
							maxCount = 1
						}
					} else {
						binary.LittleEndian.PutUint64(tk.backing[off+8:], cnt)
					}
				}
			}
		}

		// Step 2: Gatekeeper - skip heap search if count is too low
		// This is O(1) and handles 99% of random "mice" inputs.
		if canOptimize && maxCount < minHeapCount {
			expelled[i] = AddResult{Expelled: true, Key: key, Count: maxCount}
			continue
		}

		// Step 3: Heap logic
		if tk.heapHydrated {
			expelled[i] = tk.addToHydratedHeap(key, maxCount, fingerprint)
			if len(tk.heap) == tk.k {
				minHeapCount = tk.heap[0].Count
			}
		} else {
			// Check if item exists in heap
			inHeap, currentHeapCount := tk.rawHeapSearch(key)

			if inHeap {
				if maxCount > currentHeapCount {
					tk.hydrateHeap()
					expelled[i] = tk.addToHydratedHeap(key, maxCount, fingerprint)
					canOptimize = false
				}
				// else: expelled[i] is already zero value (not expelled)
			} else {
				if heapN < tk.k {
					tk.hydrateHeap()
					expelled[i] = tk.addToHydratedHeap(key, maxCount, fingerprint)
					heapN = len(tk.heap)
					canOptimize = false
				} else {
					// Heap full, maxCount >= minHeapCount (passed gatekeeper)
					// Must swap with min
					tk.hydrateHeap()
					expelled[i] = tk.addToHydratedHeap(key, maxCount, fingerprint)
					minHeapCount = tk.heap[0].Count
					canOptimize = false
				}
			}
		}
	}

	return expelled
}

// addToHydratedHeap performs standard heap logic on the Go slice.
func (tk *TopK) addToHydratedHeap(key string, count, fp uint64) AddResult {
	idx, found := tk.heap.linearSearch(key)

	if found {
		if count > tk.heap[idx].Count {
			tk.heap[idx].Count = count
			tk.heap.Fix(idx)
			tk.heapDirty = true
		}
		return AddResult{} // Not expelled
	}

	if len(tk.heap) < tk.k {
		tk.heap.Push(Item{Key: key, Count: count, Fingerprint: fp})
		tk.heapDirty = true
		return AddResult{} // Not expelled
	}

	minItem := tk.heap[0]
	if count > minItem.Count {
		tk.heap[0] = Item{Key: key, Count: count, Fingerprint: fp}
		tk.heap.Fix(0)
		tk.heapDirty = true
		return AddResult{Expelled: true, Key: minItem.Key, Count: minItem.Count}
	}

	return AddResult{Expelled: true, Key: key, Count: count}
}

// Query checks if an item is in the Top-K list.
func (tk *TopK) Query(key string) (bool, uint64) {
	if tk.heapHydrated {
		idx, found := tk.heap.linearSearch(key)
		if found {
			return true, tk.heap[idx].Count
		}
		return false, 0
	}

	// Fast path: scan raw bytes (no allocation since rawHeapSearch takes string)
	return tk.rawHeapSearch(key)
}

// List returns items sorted by count descending.
func (tk *TopK) List() []Item {
	tk.hydrateHeap() // Must parse for user output
	result := make([]Item, len(tk.heap))
	copy(result, tk.heap)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Count > result[j].Count
	})
	return result
}

// K returns the maximum tracked items.
func (tk *TopK) K() int { return tk.k }

// Width returns bucket array width.
func (tk *TopK) Width() int { return tk.width }

// Depth returns bucket array depth.
func (tk *TopK) Depth() int { return tk.depth }

// Decay returns the decay probability base.
func (tk *TopK) Decay() float64 { return tk.decay }

// Bytes returns the serialized TopK. If heap wasn't modified, returns
// backing slice directly. Otherwise rebuilds the heap portion.
func (tk *TopK) Bytes() []byte {
	// Fast path: if heap wasn't hydrated or wasn't modified, backing is correct
	if !tk.heapDirty {
		return tk.backing
	}

	bucketsEnd := HeaderSize + tk.width*tk.depth*16

	// Calculate heap size.
	heapSize := 0
	for _, item := range tk.heap {
		heapSize += 4 + len(item.Key) + 16
	}

	totalSize := bucketsEnd + heapSize

	// Resize backing if needed.
	if cap(tk.backing) < totalSize {
		newBacking := make([]byte, totalSize)
		copy(newBacking, tk.backing[:bucketsEnd])
		tk.backing = newBacking
	} else {
		tk.backing = tk.backing[:totalSize]
	}

	// Update heap count in header.
	binary.LittleEndian.PutUint32(tk.backing[24:28], uint32(len(tk.heap)))

	// Serialize heap.
	offset := bucketsEnd
	for _, item := range tk.heap {
		binary.LittleEndian.PutUint32(tk.backing[offset:], uint32(len(item.Key)))
		offset += 4
		copy(tk.backing[offset:], item.Key)
		offset += len(item.Key)
		binary.LittleEndian.PutUint64(tk.backing[offset:], item.Count)
		offset += 8
		binary.LittleEndian.PutUint64(tk.backing[offset:], item.Fingerprint)
		offset += 8
	}

	tk.heapDirty = false
	return tk.backing
}

// mix applies SplitMix64 to decorrelate hash bits for independent bucket indices.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// getDecayProb returns decay^count (for testing).
func (tk *TopK) getDecayProb(count uint64) float64 {
	if count < decayLookupSize {
		return float64(tk.decayThresholds[count]) / float64(math.MaxUint64)
	}
	return math.Pow(tk.decay, float64(count))
}

// AddWeighted inserts items with specific increment values.
// This is separate from Add() to preserve maximum performance for the common
// "increment by 1" case. Useful for tracking metrics like "top users by bandwidth"
// where each item has a weight (e.g., bytes transferred).
func (tk *TopK) AddWeighted(items []string, increments []uint64) []AddResult {
	results := make([]AddResult, len(items))

	heapN := int(binary.LittleEndian.Uint32(tk.backing[24:28]))
	var minHeapCount uint64

	// Gatekeeper optimization: if heap is full and not hydrated, peek at min count.
	canOptimize := !tk.heapHydrated && heapN == tk.k
	if canOptimize {
		minHeapCount = tk.getMinHeapCount()
	}

	depth := uint64(tk.depth)
	width := uint64(tk.width)
	widthMask := tk.widthMask

	// Bounds Check Elimination (BCE) hint: prove the slice is large enough once,
	// so the compiler can remove bounds checks inside the tight inner loop.
	bucketsEnd := HeaderSize + int(width*depth*16)
	_ = tk.backing[bucketsEnd-1]

	for i, key := range items {
		incr := increments[i]

		if incr == 0 {
			// Zero increment: behaves like Query but returns expelled format
			found, count := tk.Query(key)
			if found {
				results[i] = AddResult{Expelled: false}
			} else {
				results[i] = AddResult{Expelled: true, Key: key, Count: count}
			}
			continue
		}

		h64 := xxhash.Sum64String(key)
		fingerprint := h64
		var maxCount uint64

		// Step 1: HeavyKeeper bucket updates with weighted decay
		for d := uint64(0); d < depth; d++ {
			hashed := mix(h64 ^ d)
			var idx uint64
			if widthMask != math.MaxUint64 {
				idx = hashed & widthMask
			} else {
				idx = hashed % width
			}
			off := HeaderSize + int((d*width+idx)<<4)

			fp := binary.LittleEndian.Uint64(tk.backing[off:])
			cnt := binary.LittleEndian.Uint64(tk.backing[off+8:])

			if cnt == 0 {
				// Empty bucket: claim it with full weight
				binary.LittleEndian.PutUint64(tk.backing[off:], fingerprint)
				binary.LittleEndian.PutUint64(tk.backing[off+8:], incr)
				if incr > maxCount {
					maxCount = incr
				}
			} else if fp == fingerprint {
				// Same fingerprint: add weight
				cnt += incr
				binary.LittleEndian.PutUint64(tk.backing[off+8:], cnt)
				if cnt > maxCount {
					maxCount = cnt
				}
			} else {
				// Different fingerprint: weighted decay
				// We decay 'incr' times. If bucket hits 0, claim it with remaining weight.
				originalCnt := cnt
				claimed := false
				for k := uint64(0); k < incr; k++ {
					if tk.fastDecayCheck(cnt) {
						cnt--
						if cnt == 0 {
							// Bucket emptied: claim with remaining weight
							binary.LittleEndian.PutUint64(tk.backing[off:], fingerprint)
							remaining := incr - k
							binary.LittleEndian.PutUint64(tk.backing[off+8:], remaining)
							if remaining > maxCount {
								maxCount = remaining
							}
							claimed = true
							break
						}
					}
				}
				// Write final count only once if decayed but didn't claim
				if !claimed && cnt != originalCnt {
					binary.LittleEndian.PutUint64(tk.backing[off+8:], cnt)
				}
			}
		}

		// Step 2: Gatekeeper - skip heap search if count is too low
		if canOptimize && maxCount < minHeapCount {
			results[i] = AddResult{Expelled: true, Key: key, Count: maxCount}
			continue
		}

		// Step 3: Heap logic
		if tk.heapHydrated {
			results[i] = tk.addToHydratedHeap(key, maxCount, fingerprint)
			if len(tk.heap) == tk.k {
				minHeapCount = tk.heap[0].Count
			}
		} else {
			// Check if item exists in heap
			inHeap, currentHeapCount := tk.rawHeapSearch(key)

			if inHeap {
				if maxCount > currentHeapCount {
					tk.hydrateHeap()
					results[i] = tk.addToHydratedHeap(key, maxCount, fingerprint)
					canOptimize = false
				}
				// else: results[i] is already zero value (not expelled)
			} else {
				if heapN < tk.k {
					tk.hydrateHeap()
					results[i] = tk.addToHydratedHeap(key, maxCount, fingerprint)
					heapN = len(tk.heap)
					canOptimize = false
				} else {
					// Heap full, maxCount >= minHeapCount (passed gatekeeper)
					tk.hydrateHeap()
					results[i] = tk.addToHydratedHeap(key, maxCount, fingerprint)
					minHeapCount = tk.heap[0].Count
					canOptimize = false
				}
			}
		}
	}

	return results
}
