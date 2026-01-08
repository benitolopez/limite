// handlers.go implements general utility commands.
//
// This file provides server-level commands that are not specific to any
// particular data structure: PING, INFO, COMPACT, and DEL.

package main

import (
	"fmt"
	"io"
	"strings"
)

// handlePing handles the PING command.
// Syntax: PING
//
// This is a standard liveness check used by clients to verify that the
// server connection is active and responsive.
func (app *application) handlePing(w io.Writer, args []string) {
	if len(args) != 0 {
		app.wrongNumberOfArgsResponse(w, "PING")
		return
	}

	// Respond with the standard "PONG" Simple String.
	// This confirms the server is reachable and processing commands.
	_ = app.writeSimpleStringResponse(w, "PONG")
}

// handleInfo handles the INFO command.
// Syntax: INFO
//
// This command provides a text-based report of the server's internal state,
// statistics, and metrics. It is primarily used for monitoring and
// debugging purposes.
func (app *application) handleInfo(w io.Writer, args []string) {
	if len(args) > 0 {
		// Verify arguments. Currently, we do not support specific sections
		// (e.g., "INFO CPU"), so we strictly require zero arguments.
		app.wrongNumberOfArgsResponse(w, "INFO")
		return
	}

	// Retrieve a snapshot of the server's metrics.
	// We use atomic loads for counters to ensure thread safety without locks.
	// The active connection count is derived from the current length of the
	// semaphore channel, providing an instantaneous view of concurrency.
	totalConns := app.metrics.TotalConnections.Load()
	totalCmds := app.metrics.TotalCommands.Load()
	activeConns := len(app.connLimiter)

	// Construct the report using the standard Redis INFO format.
	// The response is a collection of CRLF-terminated lines divided into
	// sections (denoted by #), where each line follows the "key:value" pattern.
	// We use a Builder to minimize allocations during string concatenation.
	var infoBuilder strings.Builder

	infoBuilder.WriteString("# Server\r\n")
	infoBuilder.WriteString(fmt.Sprintf("connections_total:%d\r\n", totalConns))
	infoBuilder.WriteString(fmt.Sprintf("connections_active:%d\r\n", activeConns))
	infoBuilder.WriteString(fmt.Sprintf("commands_processed_total:%d\r\n", totalCmds))

	infoContent := infoBuilder.String()

	if err := app.writeBulkStringResponse(w, infoContent); err != nil {
		return
	}
}

// handleCompact handles the COMPACT command.
// Syntax: COMPACT
func (app *application) handleCompact(w io.Writer, args []string) {
	//
	// DESIGN
	// ------
	//
	// This command allows an administrator to manually trigger the Hybrid AOF
	// rewrite process. This is useful for freeing up disk space immediately
	// or preparing a compact dataset before a backup/restart.
	//
	// Concurrency Control:
	// We share the 'isRewriting' atomic flag with the automatic background
	// maintenance loop (in main.go). This ensures that we never run two
	// compaction processes simultaneously, which would waste CPU/IO and
	// potentially conflict on temporary files.
	//
	// Asynchronous Execution:
	// The rewrite happens in a goroutine. The client receives an immediate
	// confirmation, identical to Redis behavior. Success/Failure is reported
	// only in the server logs.
	//

	if len(args) != 0 {
		app.wrongNumberOfArgsResponse(w, "COMPACT")
		return
	}

	// Cannot compact when running in memory-only mode.
	if app.aof == nil {
		_ = app.writeErrorResponse(w, "ERR persistence is disabled, nothing to compact")
		return
	}

	// Attempt to acquire the rewrite lock.
	// If CompareAndSwap returns false, a rewrite (auto or manual) is already running.
	if !app.isRewriting.CompareAndSwap(false, true) {
		_ = app.writeErrorResponse(w, "ERR Background append only file rewriting already in progress")
		return
	}

	go func() {
		defer app.isRewriting.Store(false)

		app.logger.Info("user requested background AOF rewrite started")

		if err := app.CompactAOF(); err != nil {
			app.logger.Error("background rewrite failed", "error", err)
		} else {
			app.logger.Info("background AOF rewrite finished successfully")
		}
	}()

	_ = app.writeSimpleStringResponse(w, "Background append only file rewriting started")
}

// handleDel handles the DEL command.
// Syntax: DEL key [key ...]
//
// Removes the specified keys from the store. Keys that do not exist are ignored.
// Returns the number of keys that were actually deleted.
func (app *application) handleDel(w io.Writer, args []string) {
	if len(args) == 0 {
		app.wrongNumberOfArgsResponse(w, "DEL")
		return
	}

	var deletedKeys []string

	// Iterate and delete
	for _, key := range args {
		if app.store.Delete(key) {
			deletedKeys = append(deletedKeys, key)
		}
	}

	count := len(deletedKeys)

	// Optimization: Only log the keys that were actually removed.
	// If the user sent "DEL a b" but only "a" existed, we log "DEL a".
	if count > 0 {
		app.logCommand("DEL", deletedKeys)
	}

	_ = app.writeIntegerResponse(w, count)
}
