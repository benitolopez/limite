package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	writeTimeout              = 5 * time.Second
	rejectionTimeout          = 500 * time.Millisecond
	errMaxConnectionsResponse = "ERR max number of clients reached\n"
)

// serve starts the TCP server and blocks until shutdown.
func (app *application) serve() error {
	//
	// DESIGN
	// ------
	//
	// This function implements a production-ready TCP server with graceful shutdown.
	// The main challenge is coordinating between new connections, in-flight requests,
	// and the shutdown signal without losing data or hanging indefinitely.
	//
	// 1. CONNECTION LIMITING
	//    We use a buffered channel (`connLimiter`) as a semaphore to cap concurrent
	//    connections. A non-blocking send to this channel acts as a "try-acquire":
	//    if the buffer is full, we reject the connection immediately. This protects
	//    the server from resource exhaustion under load.
	//
	// 2. GRACEFUL SHUTDOWN
	//    A dedicated goroutine listens for OS signals (SIGINT, SIGTERM). Upon receiving
	//    one, it closes the listener to stop accepting new connections, then waits for
	//    all in-flight handlers to finish (tracked by a WaitGroup). A context timeout
	//    ensures the shutdown doesn't hang forever if a client is stuck.
	//
	// 3. ERROR PROPAGATION
	//    The shutdown goroutine communicates its result back to the main loop via a
	//    channel. This allows the main function to return an appropriate error code.
	//
	addr := fmt.Sprintf(":%d", app.config.port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	app.listener = ln

	serverAddr := ln.Addr().String()

	if app.readyCh != nil {
		close(app.readyCh)
	}

	shutdownError := make(chan error)
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		s := <-quit

		app.logger.Info("caught signal", "signal", s.String(), "address", serverAddr)
		app.logger.Info("shutting down server", "address", serverAddr)

		// Create a context with a timeout for graceful shutdown.
		ctx, cancel := context.WithTimeout(context.Background(), app.config.shutdownTimeout)
		defer cancel()

		// Stop accepting new connections.
		if err := ln.Close(); err != nil {
			shutdownError <- err
		}

		// Use a channel to signal when the WaitGroup is done.
		wgDone := make(chan struct{})
		go func() {
			app.wg.Wait()
			close(wgDone)
		}()

		// Wait for either all connections to finish or for the timeout.
		select {
		case <-wgDone:
			shutdownError <- nil // Clean shutdown
		case <-ctx.Done():
			shutdownError <- ctx.Err() // Timeout
		}
	}()

	app.logger.Info("server starting", "address", serverAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break // Normal shutdown path
			}
			app.logger.Error("failed to accept connection", "error", err, "address", serverAddr)
			continue
		}

		select {
		case app.connLimiter <- struct{}{}:
			// A slot was available. Launch the handler.
			app.wg.Add(1)
			go app.handleConnection(conn)
		default:
			// No slot was available. Reject the connection.
			app.logger.Info("rejecting connection, limit reached", "remote_addr", conn.RemoteAddr().String())

			// Security: Set strict deadline to prevent slowloris-style DoS.
			// A malicious client could block the accept loop by not reading.
			_ = conn.SetWriteDeadline(time.Now().Add(rejectionTimeout))

			_ = app.writeResponse(conn, []byte(errMaxConnectionsResponse))
			_ = conn.Close()
		}
	}

	err = <-shutdownError
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		app.logger.Error("server stopped with error", "error", err, "address", serverAddr)
		return err
	}

	app.logger.Info("server stopped gracefully", "address", serverAddr)
	return nil
}

// handleConnection manages the lifecycle of a single client connection.
func (app *application) handleConnection(conn net.Conn) {
	//
	// DESIGN
	// ------
	//
	// This handler implements a high-performance request/response loop with
	// buffered I/O and an optimization called "Smart Flush" for pipelining.
	//
	// 1. BUFFERED WRITES
	//    Instead of writing each response directly to the socket (one syscall per
	//    response), we wrap the connection in a `bufio.Writer`. Responses accumulate
	//    in a 4KB user-space buffer and are sent in larger batches, reducing the
	//    number of expensive kernel transitions.
	//
	// 2. SMART FLUSH (PIPELINING OPTIMIZATION)
	//    When a client pipelines commands (sends multiple commands without waiting
	//    for responses), the TCP stack delivers them in a single read. After
	//    processing a command, we check if the parser's buffer still has data. If
	//    so, we skip the flush and immediately process the next command. This
	//    batches multiple responses into one write syscall. If the buffer is empty,
	//    we flush to ensure the client isn't left waiting.
	//
	// 3. RESOURCE CLEANUP
	//    The deferred operations ensure that, regardless of how the loop exits
	//    (clean disconnect, parse error, timeout), we always:
	//    - Release the semaphore slot (`connLimiter`)
	//    - Decrement the WaitGroup (for graceful shutdown)
	//    - Close the connection
	//    - Flush any buffered responses
	//
	defer func() { <-app.connLimiter }()
	defer app.wg.Done()
	defer func() { _ = conn.Close() }()

	app.metrics.TotalConnections.Add(1)

	remoteAddr := conn.RemoteAddr().String()
	app.logger.Info("new connection", "remote_addr", remoteAddr)

	parser := NewParser(conn)
	writer := bufio.NewWriterSize(conn, 4096)

	// Ensure buffered responses are flushed before the connection closes.
	// This handles the case where a parse error occurs mid-pipeline:
	// responses to successfully processed commands must still be sent.
	defer func() { _ = writer.Flush() }()

	if app.config.idleTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(app.config.idleTimeout)); err != nil {
			app.logger.Error("failed to set initial read deadline", "error", err, "remote_addr", remoteAddr)
			return
		}
	}

	for {
		if app.config.idleTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(app.config.idleTimeout)); err != nil {
				app.logger.Error("failed to set read deadline", "error", err, "remote_addr", remoteAddr)
				return
			}
		}

		parts, err := parser.Parse()
		if err != nil {
			if err == io.EOF {
				app.logger.Info("client disconnected", "remote_addr", remoteAddr)
			} else {
				app.logger.Error("parser error", "error", err, "remote_addr", remoteAddr)
			}
			return
		}

		app.router.Dispatch(app, writer, parts)

		// Smart Flush: Only flush when the read buffer is empty.
		//
		// If the client sent multiple commands in a single TCP packet (pipelining),
		// the parser's buffer will still have data. We skip the flush and immediately
		// process the next command, batching multiple responses into a single write
		// syscall. This reduces I/O overhead significantly for pipelined workloads.
		if parser.Buffered() == 0 {
			if err := writer.Flush(); err != nil {
				app.logger.Error("failed to flush response", "error", err, "remote_addr", remoteAddr)
				return
			}
		}
	}
}
