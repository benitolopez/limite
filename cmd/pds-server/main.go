// main.go is the entry point for the PDS server. It wires together the storage
// layer, persistence layer, and network server, and manages the operational
// lifecycle including background maintenance tasks.
//
// Startup Sequence
// ================
//
// The server follows a careful initialization order to ensure consistency:
//
// First, we create the empty in-memory Store. Then we call loadAOF(), which
// reads the journal file (if it exists) and populates the store. This happens
// before any network listeners are active, so there's no need for locking
// during the load phase. Only after the state is fully restored do we open
// the AOF for writing and start accepting client connections.
//
// Durability Policy
// =================
//
// The server does not fsync to disk on every write—that would limit throughput
// to a few thousand operations per second. Instead, we buffer writes in memory
// and rely on a background goroutine to call Fsync() every second. This means:
//
//   - Under normal operation, committed data reaches the physical disk within
//     one second of the write.
//   - In the event of a kernel panic or power failure, at most one second of
//     recent writes may be lost.
//
// This trade-off prioritizes throughput over per-write durability.
//
// Background Maintenance
// ======================
//
// A single background goroutine handles two responsibilities:
//
// Fsync Timer: Every second, we flush the AOF buffer to disk. This implements
// the durability guarantee described above.
//
// Auto-Rewrite Trigger: We monitor the journal file size and trigger compaction
// when it exceeds a threshold. The policy is configurable via command-line flags:
//
//   -aof-min-size:        Minimum file size before considering a rewrite.
//   -aof-rewrite-percent: Growth percentage over the base size to trigger.
//
// For example, with defaults of 64MB min and 100% growth: if the base size is
// 64MB, we trigger when the file reaches 128MB. After compaction, the new file
// becomes the base for future calculations.
//
// Graceful Shutdown
// =================
//
// On exit (SIGINT/SIGTERM or clean return), we perform a final CompactAOF to
// ensure the journal is as small as possible for the next startup. This is a
// best-effort operation—if it fails, the journal remains valid (just larger
// than optimal).

package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"pds.lopezb.com/internal/pds/hyperloglog"
)

type config struct {
	port               int
	maxConnections     int
	shutdownTimeout    time.Duration
	idleTimeout        time.Duration
	hllSparseThreshold int
	bfInitialCapacity  uint64
	bfErrorRate        float64
	aofFilename        string
	aofMinSize         int64
	aofRewritePercent  int
	aofLoadTruncated   bool
}

type application struct {
	config          config
	logger          *slog.Logger
	listener        net.Listener
	store           *Store
	router          *Router
	metrics         *Metrics
	readyCh         chan struct{}
	wg              sync.WaitGroup
	connLimiter     chan struct{}
	aof             *AOF
	aofBaseSize     atomic.Int64
	isRewriting     atomic.Bool
	needsCompaction bool
}

func main() {
	var cfg config

	flag.IntVar(&cfg.port, "port", 6479, "TCP server port")
	flag.IntVar(&cfg.maxConnections, "max-conn", 100, "Maximum concurrent connections")
	flag.DurationVar(&cfg.shutdownTimeout, "shutdown-timeout", 5*time.Second, "Graceful shutdown timeout")
	flag.DurationVar(&cfg.idleTimeout, "idle-timeout", 0, "Idle client connection timeout (0 for no timeout)")
	flag.IntVar(&cfg.hllSparseThreshold, "hll-sparse-threshold", hyperloglog.DefaultSparseThreshold, "HyperLogLog sparse-to-dense threshold")
	flag.Uint64Var(&cfg.bfInitialCapacity, "bf-capacity", 1000, "Bloom Filter initial capacity for new filters")
	flag.Float64Var(&cfg.bfErrorRate, "bf-error-rate", 0.01, "Bloom Filter target false positive rate (e.g., 0.01 for 1%)")
	flag.StringVar(&cfg.aofFilename, "aof", "journal.aof", "Append Only File path")
	flag.Int64Var(&cfg.aofMinSize, "aof-min-size", 64*1024*1024, "Min size (bytes) to trigger AOF rewrite")
	flag.IntVar(&cfg.aofRewritePercent, "aof-rewrite-percent", 100, "Percentage growth to trigger AOF rewrite")
	flag.BoolVar(&cfg.aofLoadTruncated, "aof-load-truncated", true, "Auto-recover from truncated AOF (set false for strict mode)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	app := &application{
		config:      cfg,
		logger:      logger,
		store:       NewStore(),
		metrics:     NewMetrics(),
		connLimiter: make(chan struct{}, cfg.maxConnections),
	}

	app.router = app.commands()

	// This replays any commands that happened after the snapshot (or all if no snapshot).
	if err := app.loadAOF(); err != nil {
		logger.Error("failed to load AOF", "error", err)
		os.Exit(1) // Fatal: AOF corruption implies data loss risk
	}

	// Open AOF for writing)
	aof, err := NewAOF(cfg.aofFilename)
	if err != nil {
		logger.Error("failed to open AOF", "error", err)
		os.Exit(1)
	}
	app.aof = aof

	// Initialize base size on startup so we calculate growth correctly.
	if stat, err := aof.file.Stat(); err == nil {
		app.aofBaseSize.Store(stat.Size())
	} else {
		app.aofBaseSize.Store(0)
	}

	// If loadAOF detected truncation, trigger immediate compaction to heal the file.
	// This writes a clean binary snapshot, replacing the corrupted tail.
	if app.needsCompaction {
		logger.Info("AOF was truncated on load, triggering immediate compaction to heal the file...")
		if err := app.CompactAOF(); err != nil {
			logger.Error("failed to compact AOF after truncation recovery", "error", err)
			// Non-fatal: the server can still run, but the file won't be healed until
			// the next automatic or manual compaction.
		} else {
			logger.Info("AOF healed successfully")
		}
	}

	// Background Maintenance Loop
	//
	// This goroutine is the heartbeat of the persistence system. It runs
	// continuously and handles two critical tasks: flushing data to disk
	// and triggering compaction when the journal grows too large.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			// Durability: Force buffered writes to the physical disk.
			// This is what backs our "at most 1 second of data loss" guarantee.
			// The Fsync call is relatively cheap (no data copying, just a syscall)
			// but it does block until the disk confirms the write.
			if err := aof.Fsync(); err != nil {
				logger.Error("background sync failed", "error", err)
			}

			// Compaction Check: Should we rewrite the AOF?
			// We use Stat() to get the current file size. On modern filesystems,
			// this is essentially free (cached in the inode).
			stat, err := aof.file.Stat()
			if err != nil {
				continue
			}

			currentSize := stat.Size()
			baseSize := app.aofBaseSize.Load()

			// Guard: Don't rewrite tiny files. Even if the percentage threshold
			// is technically exceeded (e.g., 1KB -> 2KB is 100% growth), the
			// overhead of compaction isn't worth it for small datasets.
			if currentSize < cfg.aofMinSize {
				continue
			}

			// Growth Policy: Trigger when file size exceeds base + (base * percent / 100).
			// With 100% growth, we rewrite when the file doubles. This balances
			// disk usage against compaction frequency.
			growthTarget := baseSize + (baseSize * int64(cfg.aofRewritePercent) / 100)

			if currentSize > growthTarget {
				// Rewrite Lock: Only one compaction can run at a time.
				// CompareAndSwap returns true only if we successfully flipped
				// false -> true, meaning no other compaction is running.
				if app.isRewriting.CompareAndSwap(false, true) {
					logger.Info("auto-rewrite triggered",
						"current_bytes", currentSize,
						"base_bytes", baseSize,
						"threshold_percent", cfg.aofRewritePercent)

					// Run compaction in a separate goroutine so we don't block
					// the maintenance loop (and miss fsync ticks).
					go func() {
						defer app.isRewriting.Store(false)

						start := time.Now()
						if err := app.CompactAOF(); err != nil {
							logger.Error("auto-rewrite failed", "error", err)
						} else {
							logger.Info("auto-rewrite completed", "duration", time.Since(start))
						}
					}()
				}
			}
		}
	}()

	// Graceful Shutdown Handler
	//
	// When the server exits (whether from a signal or a clean return), we
	// perform a final compaction. This ensures the journal is as small as
	// possible for the next startup, minimizing AOF replay time.
	defer func() {
		logger.Info("shutting down, compacting AOF...")
		if err := app.CompactAOF(); err != nil {
			// This is best-effort. The journal is still valid if compaction
			// fails; it just won't be as compact. Log the error and continue
			// with the close.
			logger.Error("failed to compact AOF on exit", "error", err)
		}
		_ = aof.Close()
	}()

	err = app.serve()
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}
