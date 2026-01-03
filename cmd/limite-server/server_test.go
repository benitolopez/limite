package main

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestApp is a helper function that creates a new, valid application instance
// for use in tests. This centralizes the setup logic.
func newTestApp(t *testing.T) *application {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	const maxConnections = 10

	cfg := config{
		port:           0, // Use a random free port
		maxConnections: maxConnections,
	}

	app := &application{
		config:      cfg,
		logger:      logger,
		store:       NewStore(),
		metrics:     NewMetrics(),
		readyCh:     make(chan struct{}),
		connLimiter: make(chan struct{}, cfg.maxConnections),
	}
	app.router = app.commands()

	return app
}

// TestPingServer ensures the PING command works as expected.
func TestPingServer(t *testing.T) {
	app := newTestApp(t)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	// --- Client ---
	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// --- Execution & Verification ---
	reader := bufio.NewReader(conn)

	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("failed to write PING: %v", err)
	}
	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	expected := "+PONG\r\n"
	if response != expected {
		t.Errorf("unexpected response: got %q, want %q", response, expected)
	}
}

// TestConnectionLimiter verifies that the server correctly limits the number
// of concurrent connections.
func TestConnectionLimiter(t *testing.T) {
	const maxConnections = 1
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config{port: 0, maxConnections: maxConnections}
	app := &application{
		config:      cfg,
		logger:      logger,
		store:       NewStore(),
		metrics:     NewMetrics(),
		readyCh:     make(chan struct{}),
		connLimiter: make(chan struct{}, cfg.maxConnections),
	}
	app.router = app.commands()

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()
	serverAddr := app.listener.Addr().String()

	// --- Step 1: Use up the single connection slot ---
	hogConn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("failed to make the first connection: %v", err)
	}
	defer func() { _ = hogConn.Close() }()

	// Give the server a moment to process the connection.
	time.Sleep(50 * time.Millisecond)

	// --- Step 2: Test that the next connection is REJECTED ---
	secondConn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("second connection dial failed unexpectedly: %v", err)
	}
	defer func() { _ = secondConn.Close() }()

	// Read from the rejected connection. We expect to get the error message.
	reader := bufio.NewReader(secondConn)
	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read from second connection: %v", err)
	}

	expected := "ERR max number of clients reached\n"
	if response != expected {
		t.Errorf("unexpected response from rejected connection: got %q, want %q", response, expected)
	}

	// --- Step 3: Verify the first connection is still alive ---
	// This proves that rejecting the second connection didn't kill the server.
	if _, err := hogConn.Write([]byte("PING\r\n")); err != nil {
		t.Fatal("first connection is dead after second was rejected")
	}

	hogReader := bufio.NewReader(hogConn)
	_, err = hogReader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read PONG from first connection: %v", err)
	}
}

// =============================================================================
// COMPACT Tests
// =============================================================================

func TestCompact(t *testing.T) {
	app := newTestApp(t)

	// Setup AOF for compaction
	tmpFile := "test_compact_cmd.aof"
	defer func() {
		_ = removeTestFile(tmpFile)
		_ = removeTestFile(tmpFile + ".tmp")
	}()

	app.config.aofFilename = tmpFile
	var err error
	app.aof, err = NewAOF(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.aof.Close() }()

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)

	sendCommand := func(cmd string) string {
		_, err := conn.Write([]byte(cmd + "\r\n"))
		if err != nil {
			t.Fatalf("failed to write command %q: %v", cmd, err)
		}
		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("failed to read response for %q: %v", cmd, err)
		}
		return response
	}

	t.Run("basic compact", func(t *testing.T) {
		// Add some data first
		sendCommand("HLL.ADD compact_key value1 value2")

		resp := sendCommand("COMPACT")
		expected := "+Background append only file rewriting started\r\n"
		if resp != expected {
			t.Errorf("expected %q, got %q", expected, resp)
		}

		// Wait for compaction to finish
		time.Sleep(100 * time.Millisecond)

		// Verify isRewriting is released
		if app.isRewriting.Load() {
			t.Error("isRewriting should be false after compaction completes")
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("COMPACT extraarg")
		if resp != "-ERR wrong number of arguments for 'COMPACT' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

func TestCompactConcurrentBlocking(t *testing.T) {
	app := newTestApp(t)

	// Setup AOF
	tmpFile := "test_compact_concurrent.aof"
	defer func() {
		_ = removeTestFile(tmpFile)
		_ = removeTestFile(tmpFile + ".tmp")
	}()

	app.config.aofFilename = tmpFile
	var err error
	app.aof, err = NewAOF(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.aof.Close() }()

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	// Add some data to make compaction take a bit longer
	conn1, _ := net.Dial("tcp", app.listener.Addr().String())
	defer func() { _ = conn1.Close() }()
	reader1 := bufio.NewReader(conn1)

	for i := 0; i < 100; i++ {
		_, _ = fmt.Fprintf(conn1, "HLL.ADD key%d value\r\n", i)
		_, _ = reader1.ReadString('\n')
	}

	// Manually set isRewriting to simulate an ongoing compaction
	app.isRewriting.Store(true)

	// Try to start another compaction - should fail
	_, _ = conn1.Write([]byte("COMPACT\r\n"))
	response, _ := reader1.ReadString('\n')

	expected := "-ERR Background append only file rewriting already in progress\r\n"
	if response != expected {
		t.Errorf("expected %q, got %q", expected, response)
	}

	// Release the lock
	app.isRewriting.Store(false)

	// Now it should work
	_, _ = conn1.Write([]byte("COMPACT\r\n"))
	response, _ = reader1.ReadString('\n')

	expected = "+Background append only file rewriting started\r\n"
	if response != expected {
		t.Errorf("expected %q, got %q", expected, response)
	}

	// Wait for completion
	time.Sleep(100 * time.Millisecond)
}

func TestCompactRaceCondition(t *testing.T) {
	app := newTestApp(t)

	// Setup AOF
	tmpFile := "test_compact_race.aof"
	defer func() {
		_ = removeTestFile(tmpFile)
		_ = removeTestFile(tmpFile + ".tmp")
	}()

	app.config.aofFilename = tmpFile
	var err error
	app.aof, err = NewAOF(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.aof.Close() }()

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	// Try to start multiple compactions simultaneously
	const clients = 10
	var wg sync.WaitGroup
	var started, blocked int32

	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)
			_, _ = conn.Write([]byte("COMPACT\r\n"))
			response, _ := reader.ReadString('\n')

			switch response {
			case "+Background append only file rewriting started\r\n":
				atomic.AddInt32(&started, 1)
			case "-ERR Background append only file rewriting already in progress\r\n":
				atomic.AddInt32(&blocked, 1)
			}
		}()
	}

	wg.Wait()

	// Exactly one should have started, the rest should be blocked
	if started != 1 {
		t.Errorf("expected exactly 1 compaction to start, got %d", started)
	}
	if blocked != int32(clients-1) {
		t.Errorf("expected %d blocked, got %d", clients-1, blocked)
	}

	t.Logf("Concurrent COMPACT: started=%d, blocked=%d", started, blocked)

	// Wait for the one that started to finish
	time.Sleep(200 * time.Millisecond)

	// Verify lock is released
	if app.isRewriting.Load() {
		t.Error("isRewriting should be false after all compactions complete")
	}
}

func removeTestFile(filename string) error {
	return os.Remove(filename)
}
