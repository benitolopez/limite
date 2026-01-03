package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
)

// =============================================================================
// Store Snapshot Benchmarks
// =============================================================================

// BenchmarkSnapshotSave measures the time to serialize the entire store to disk.
// This is the core operation in CompactAOF and determines how long the server
// holds read locks during persistence.
func BenchmarkSnapshotSave(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("keys_%d", size), func(b *testing.B) {
			store := NewStore()

			// Populate store with test data
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("key-%d", i)
				// Simulate HLL-sized values (~16KB for dense)
				val := make([]byte, 100)
				store.Set(key, val)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				buf.Grow(size * 120) // Pre-allocate to avoid resizing
				if err := store.SaveSnapshotToWriter(&buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSnapshotLoad measures the time to restore state from a binary snapshot.
// This determines server startup time after a clean shutdown.
func BenchmarkSnapshotLoad(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("keys_%d", size), func(b *testing.B) {
			// Create and serialize a store
			store := NewStore()
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("key-%d", i)
				val := make([]byte, 100)
				store.Set(key, val)
			}

			var buf bytes.Buffer
			_ = store.SaveSnapshotToWriter(&buf)
			snapshotData := buf.Bytes()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				newStore := NewStore()
				reader := bufio.NewReader(bytes.NewReader(snapshotData))
				if err := newStore.LoadSnapshotFromReader(reader); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSnapshotSaveLargeValues measures snapshot performance with large values
// (simulating dense HLLs which are ~16KB each).
func BenchmarkSnapshotSaveLargeValues(b *testing.B) {
	store := NewStore()

	// 1000 keys with 16KB values = ~16MB of data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("hll-%d", i)
		val := make([]byte, 16*1024) // 16KB per HLL
		store.Set(key, val)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		buf.Grow(17 * 1024 * 1024) // Pre-allocate ~17MB
		if err := store.SaveSnapshotToWriter(&buf); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// AOF Benchmarks
// =============================================================================

// BenchmarkAOFWrite measures the throughput of appending commands to the AOF.
// This is called on every write command and is critical for server throughput.
func BenchmarkAOFWrite(b *testing.B) {
	filename := "bench_aof.aof"
	defer func() { _ = os.Remove(filename) }()

	aof, err := NewAOF(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = aof.Close() }()

	// Pre-encode a typical command
	cmd := encodeCommand("HLL.ADD", []string{"mykey", "value1", "value2"})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := aof.Write(cmd); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAOFFsync measures the cost of fsync operations.
// This is called every second in the background maintenance loop.
func BenchmarkAOFFsync(b *testing.B) {
	filename := "bench_fsync.aof"
	defer func() { _ = os.Remove(filename) }()

	aof, err := NewAOF(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = aof.Close() }()

	// Write some data first
	cmd := encodeCommand("HLL.ADD", []string{"mykey", "value"})
	for i := 0; i < 1000; i++ {
		_ = aof.Write(cmd)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := aof.Fsync(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeCommand measures the cost of encoding commands to RESP format.
func BenchmarkEncodeCommand(b *testing.B) {
	args := []string{"mykey", "value1", "value2", "value3"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = encodeCommand("HLL.ADD", args)
	}
}

// =============================================================================
// Compaction Benchmarks
// =============================================================================

// BenchmarkCompactAOF measures the end-to-end compaction time.
// This is the full CompactAOF operation including snapshot creation and
// atomic file replacement.
func BenchmarkCompactAOF(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("keys_%d", size), func(b *testing.B) {
			filename := fmt.Sprintf("bench_compact_%d.aof", size)
			defer func() { _ = os.Remove(filename) }()
			defer func() { _ = os.Remove(filename + ".tmp") }()

			app := &application{
				store:  NewStore(),
				config: config{aofFilename: filename},
			}

			// Populate store
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("key-%d", i)
				app.store.Set(key, make([]byte, 100))
			}

			// Create initial AOF
			var err error
			app.aof, err = NewAOF(filename)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := app.CompactAOF(); err != nil {
					b.Fatal(err)
				}
			}

			_ = app.aof.Close()
		})
	}
}

// =============================================================================
// AOF Loading Benchmarks
// =============================================================================

// BenchmarkLoadAOFText measures the time to replay a text-only AOF.
// This represents the worst-case startup time (no compaction ever performed).
func BenchmarkLoadAOFText(b *testing.B) {
	filename := "bench_load_text.aof"
	defer func() { _ = os.Remove(filename) }()

	// Create a text AOF with many commands using proper key format
	f, _ := os.Create(filename)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%03d", i%100)
		val := fmt.Sprintf("val%04d", i)
		cmd := fmt.Sprintf("*3\r\n$7\r\nHLL.ADD\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
		_, _ = f.WriteString(cmd)
	}
	_ = f.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		app := newBenchApp()
		app.config.aofFilename = filename
		if err := app.loadAOF(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLoadAOFHybrid measures the time to load a hybrid AOF (binary preamble).
// This represents the best-case startup time (recent compaction).
func BenchmarkLoadAOFHybrid(b *testing.B) {
	filename := "bench_load_hybrid.aof"
	defer func() { _ = os.Remove(filename) }()

	// Create a store with data
	store := NewStore()
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		store.Set(key, make([]byte, 100))
	}

	// Write as hybrid AOF (binary snapshot)
	f, _ := os.Create(filename)
	_ = store.SaveSnapshotToWriter(f)
	_ = f.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		app := newBenchApp()
		app.config.aofFilename = filename
		if err := app.loadAOF(); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Store Operations Benchmarks
// =============================================================================

// BenchmarkStoreSet measures raw store write throughput.
func BenchmarkStoreSet(b *testing.B) {
	store := NewStore()
	value := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		store.Set(key, value)
	}
}

// BenchmarkStoreGet measures raw store read throughput.
func BenchmarkStoreGet(b *testing.B) {
	store := NewStore()
	value := make([]byte, 100)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		store.Set(fmt.Sprintf("key-%d", i), value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		_, _ = store.Get(key)
	}
}

// BenchmarkStoreMutate measures the atomic read-modify-write throughput.
func BenchmarkStoreMutate(b *testing.B) {
	store := NewStore()
	store.Set("counter", []byte{0})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		store.Mutate("counter", func(data []byte) ([]byte, bool) {
			if data == nil {
				return []byte{1}, true
			}
			return []byte{data[0] + 1}, true
		})
	}
}

// BenchmarkStoreDelete measures delete operation throughput.
func BenchmarkStoreDelete(b *testing.B) {
	store := NewStore()
	value := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := fmt.Sprintf("key-%d", i)
		store.Set(key, value)
		b.StartTimer()

		store.Delete(key)
	}
}

// newBenchApp creates a minimal application for benchmark use.
func newBenchApp() *application {
	app := &application{
		store:   NewStore(),
		metrics: NewMetrics(),
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		config: config{
			aofLoadTruncated: true,
		},
	}
	app.router = app.commands()
	return app
}
