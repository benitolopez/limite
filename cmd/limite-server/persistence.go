// persistence.go implements the Persistence Controller, which orchestrates the
// interaction between the in-memory Store and the on-disk journal. This file is
// responsible for loading data at startup and logging writes during operation.
//
// The Limite server uses a Hybrid Persistence Model inspired by Redis 4.0+. This
// model combines the best properties of two approaches:
//
//   - Binary Snapshots: Fast to load, compact on disk, but represent a
//     point-in-time state and lose recent writes if the server crashes.
//
//   - Append-Only Files (AOF): Durable (every write is logged), but slow to
//     replay on startup and grow indefinitely without compaction.
//
// The Hybrid approach stores both in a single file (journal.aof):
//
//	+-----------------------+---------------------------+
//	| Binary Preamble       | Text Tail                 |
//	| (LIM1 Snapshot)       | (RESP Commands)           |
//	+-----------------------+---------------------------+
//
// On startup, the loader reads the binary preamble to restore the bulk of the
// data almost instantly, then replays only the text tail (recent commands that
// arrived after the last compaction). This gives us fast startup times AND
// strong durability guarantees.
//
// Command Logging
// ===============
//
// Every successful write command (HLL.ADD, BF.ADD, CMS.INCRBY, etc.) is logged
// to the AOF as a RESP-formatted text string. This happens after the in-memory
// mutation succeeds. If the disk write fails, we log an error but do not panic;
// we prioritize service availability over strict consistency, trusting that the
// admin will notice the error and take action.
//
// The logged commands are human-readable and compatible with standard Redis
// parsers, making debugging and manual recovery straightforward.
//
// Hybrid Compaction
// =================
//
// Over time, the text tail grows as commands accumulate. CompactAOF rewrites
// the journal by taking a fresh binary snapshot and atomically replacing the
// old file. The new file contains only the binary preamble (no text tail yet),
// dramatically reducing file size and future startup time.
//
// The compaction process uses a temporary file and an atomic rename to ensure
// we never leave a corrupted journal on disk. Even if the server crashes mid-
// compaction, the original journal remains intact.

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

// logCommand appends a write command to the AOF in RESP format. This should be
// called by every handler that successfully modifies in-memory state.
//
// The function is intentionally fire-and-forget: if the disk write fails, we
// log an error but do not return it to the caller. This design prioritizes
// service availability—the in-memory state is correct and clients can continue
// working. The admin is expected to monitor logs for persistence failures.
func (app *application) logCommand(command string, args []string) {
	if app.aof == nil {
		return
	}

	// Convert the command into RESP wire format. This uses the same encoding
	// that clients send, ensuring the log is human-readable and compatible
	// with standard Redis parsers.
	data := encodeCommand(command, args)

	// The AOF handles its own locking, so concurrent calls from different
	// request goroutines are safe. We absorb errors here rather than bubbling
	// them up because the in-memory mutation already succeeded; failing the
	// request now would confuse clients.
	if err := app.aof.Write(data); err != nil {
		app.logger.Error("CRITICAL: failed to append to AOF", "error", err, "command", command)
	}
}

// loadAOF restores the server state from the journal file at startup. It
// handles both pure-text AOF files and Hybrid files (binary preamble + text
// tail) transparently.
func (app *application) loadAOF() error {
	//
	// DESIGN
	// ------
	//
	// This loader implements a "smart" detection scheme. It peeks at the first
	// 4 bytes of the file to determine the format:
	//
	// If the magic bytes are "PDS1", we have a Hybrid file. The binary loader
	// (store.LoadSnapshotFromReader) is called first; it consumes exactly the
	// binary section and stops at the checksum. Critically, we pass a *bufio.Reader*
	// so that buffered-but-unconsumed bytes remain available. The same reader is
	// then passed to the RESP parser to replay any text commands that follow.
	//
	// If the magic bytes are anything else, we assume a pure-text AOF (or an
	// empty file) and skip directly to RESP parsing.
	//
	// This unified streaming approach avoids reading the file twice or seeking
	// back and forth. The buffered reader acts as a "cursor" that advances
	// monotonically through the file.
	//

	f, err := os.Open(app.config.aofFilename)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	reader := bufio.NewReader(f)

	// 1. Header Peek
	// We check the first 4 bytes to determine the file format.
	magic, _ := reader.Peek(4)

	// 2. Binary Preamble Loading
	if string(magic) == "LIM1" {
		app.logger.Info("loading hybrid AOF preamble...")
		// This consumes the binary section and stops exactly after the checksum.
		if err := app.store.LoadSnapshotFromReader(reader); err != nil {
			return fmt.Errorf("corrupt hybrid preamble: %w", err)
		}
	}

	// 3. Text Replay (The Tail)
	// The reader is now positioned at the start of the text log (or EOF).
	parser := NewParser(reader)

	for {
		parts, err := parser.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			// In case of power failure, the last text command might be half-written.
			// This is normal and expected—we can recover by ignoring the partial command.
			//
			// io.ErrUnexpectedEOF means we started reading a command but hit EOF
			// before completing it. This is the classic "crash during write" scenario.
			//
			// The behavior is controlled by the -aof-load-truncated flag (default: true):
			//   - true:  Log warning, set needsCompaction flag, continue with loaded data
			//   - false: Return error, require manual intervention (strict mode)
			//
			// Other errors (e.g., ErrInvalidSyntax in the middle of the file) remain
			// fatal because they indicate real corruption, not just truncation.
			if err == io.ErrUnexpectedEOF {
				if app.config.aofLoadTruncated {
					app.logger.Warn("AOF truncated at end - ignoring partial last command (this is normal after a crash)")
					app.needsCompaction = true // Signal main.go to compact immediately after AOF opens
					return nil
				}
				return errors.New("AOF truncated (run with -aof-load-truncated=true to auto-recover, or use limite-check to inspect)")
			}
			return err
		}

		app.router.Dispatch(app, io.Discard, parts)
	}

	return nil
}

// CompactAOF rewrites the journal file by replacing the accumulated command
// history with a fresh binary snapshot. This dramatically reduces file size
// and speeds up future startups.
func (app *application) CompactAOF() error {
	//
	// DESIGN
	// ------
	//
	// Compaction is the key to keeping the Hybrid model efficient. Without it,
	// the text tail would grow forever, and restarts would slow down as more
	// commands need replaying. Compaction "collapses" all that history into a
	// single point-in-time snapshot.
	//
	// The process has two phases with different concurrency characteristics:
	//
	// Phase 1 (Snapshot to Temp File): We create a temporary file and stream
	// the binary snapshot to it. This can take significant time for large
	// datasets, but it happens without blocking the main event loop. The
	// SaveSnapshotToWriter method uses per-shard read-locks, so the server
	// remains responsive throughout.
	//
	// Phase 2 (Atomic Swap): We acquire the exclusive AOF lock to pause all
	// incoming logCommand calls. This critical section is extremely brief—just
	// a flush, close, rename, and reopen. Clients might experience a few
	// milliseconds of added latency, but no requests are dropped.
	//
	// Resource Lifecycle Safety:
	// To prevent undefined behavior (double-closing files) and ensure garbage
	// collection of failed attempts, we use explicit state flags ('closed' and
	// 'renamed'). The deferred cleanup function checks these flags to determine
	// exactly which resources are still pending release.
	//
	// If the server crashes during Phase 1, the original journal is untouched
	// (the temp file is garbage). If it crashes during Phase 2, the rename is
	// atomic on POSIX systems, so we either have the old file or the new one,
	// never a corrupted hybrid.
	//

	// 1. Create Temporary File
	// We write to a temp file first to ensure the original log remains
	// intact if the snapshot process crashes or runs out of disk space.
	tmpName := app.config.aofFilename + ".tmp"
	f, err := os.Create(tmpName)
	if err != nil {
		return err
	}

	// State flags for deferred cleanup.
	// We use these to prevent double-closing the file or deleting the
	// file after a successful rename.
	var (
		fileClosed    bool // Explicit close succeeded; don't double-close in defer
		renameSuccess bool // Atomic swap succeeded; don't delete the new journal
	)

	// Safety cleanup: checks the flags to perform only necessary actions.
	defer func() {
		// Only close if the happy path hasn't done so yet.
		if !fileClosed {
			_ = f.Close()
		}
		// If the rename didn't happen (error occurred early), remove garbage.
		// If the rename succeeded, the file is live and must stay.
		if !renameSuccess {
			_ = os.Remove(tmpName)
		}
	}()

	// 2. Write Binary Snapshot (Preamble)
	// We wrap in a block to ensure the buffered writer is flushed locally.
	{
		bw := bufio.NewWriter(f)
		if err := app.store.SaveSnapshotToWriter(bw); err != nil {
			return err
		}
		if err := bw.Flush(); err != nil {
			return err
		}
	}

	// Ensure data is physically on disk before we swap.
	if err := f.Sync(); err != nil {
		return err
	}

	// We must close the temp file handle before we can rename it (Windows requirement,
	// and good practice generally).
	if err := f.Close(); err != nil {
		return err
	}
	// Mark the file as closed so the deferred function doesn't touch it again.
	fileClosed = true

	// 3. Critical Section: The Swap
	// We must stop all incoming writes (logCommand) to perform the file switch.
	app.aof.mu.Lock()
	defer app.aof.mu.Unlock()

	// A. Flush and Close the ACTIVE AOF
	// We try to save any pending text commands in the buffer.
	if err := app.aof.writer.Flush(); err != nil {
		// Even if flush fails, we proceed to close/swap because we are strictly
		// replacing history with a newer snapshot state.
		app.logger.Error("warning: failed to flush old AOF before rewrite", "error", err)
	}
	_ = app.aof.file.Close()

	// B. Atomic Rename
	// This replaces 'journal.aof' with our new binary-packed file.
	// From this point on, the file on disk is the new Hybrid version.
	if err := os.Rename(tmpName, app.config.aofFilename); err != nil {
		return err
	}
	// Mark rename as successful so the deferred function doesn't delete the new AOF.
	renameSuccess = true

	// C. Re-open the File
	// We open the new file in Append mode. Any subsequent 'logCommand' calls
	// will append text to the end of this binary blob.
	newFile, err := os.OpenFile(app.config.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return err
	}

	// D. Update AOF Internals
	// Point the struct to the new file handle and reset the buffer.
	app.aof.file = newFile
	app.aof.writer.Reset(newFile)

	// Update metrics for the auto-rewrite logic.
	// We get the size of the *new* compact file.
	if stat, err := newFile.Stat(); err == nil {
		app.aofBaseSize.Store(stat.Size())
	}

	return nil
}
