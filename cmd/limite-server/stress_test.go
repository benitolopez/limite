package main

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Connection Stress Tests
// =============================================================================

// TestStressMaxConnections verifies the server handles connection limits gracefully
// under heavy concurrent connection attempts.
func TestStressMaxConnections(t *testing.T) {
	const maxConn = 10
	const attemptedConns = 100

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	app := &application{
		config:      config{port: 0, maxConnections: maxConn},
		logger:      logger,
		store:       NewStore(),
		metrics:     NewMetrics(),
		readyCh:     make(chan struct{}),
		connLimiter: make(chan struct{}, maxConn),
	}
	app.router = app.commands()

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	var wg sync.WaitGroup
	var accepted, rejected atomic.Int32

	wg.Add(attemptedConns)
	for i := 0; i < attemptedConns; i++ {
		go func() {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", app.listener.Addr().String(), 5*time.Second)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()

			// Try to read - rejected connections get error message
			_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			reader := bufio.NewReader(conn)
			line, err := reader.ReadString('\n')

			if err == nil && line == "ERR max number of clients reached\n" {
				rejected.Add(1)
			} else {
				accepted.Add(1)
				// Keep connection alive briefly to maintain pressure
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	t.Logf("Connections: accepted=%d, rejected=%d, max=%d",
		accepted.Load(), rejected.Load(), maxConn)

	// We should have accepted at most maxConn connections
	if accepted.Load() > int32(maxConn) {
		t.Errorf("Accepted more connections than limit: %d > %d", accepted.Load(), maxConn)
	}
}

// TestStressRapidConnectDisconnect verifies the server handles rapid connection
// cycling without leaking resources.
func TestStressRapidConnectDisconnect(t *testing.T) {
	app := newStressTestApp(t, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const cycles = 500
	const concurrency = 20

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < cycles/concurrency; i++ {
				conn, err := net.Dial("tcp", app.listener.Addr().String())
				if err != nil {
					continue
				}

				// Send PING, get PONG, close
				_, _ = conn.Write([]byte("PING\r\n"))
				reader := bufio.NewReader(conn)
				_, _ = reader.ReadString('\n')
				_ = conn.Close()
			}
		}()
	}

	wg.Wait()

	// Verify server is still healthy
	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatalf("Server unresponsive after stress test: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_, _ = conn.Write([]byte("PING\r\n"))
	reader := bufio.NewReader(conn)
	response, _ := reader.ReadString('\n')
	if response != "+PONG\r\n" {
		t.Errorf("Unexpected response after stress: %q", response)
	}

	t.Logf("Completed %d rapid connect/disconnect cycles", cycles)
}

// =============================================================================
// Pipeline Stress Tests
// =============================================================================

// TestStressLargePipeline verifies the server handles large command pipelines
// without blocking or running out of memory.
func TestStressLargePipeline(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const pipelineSize = 10000

	// Send all commands without waiting for responses
	for i := 0; i < pipelineSize; i++ {
		cmd := fmt.Sprintf("HLL.ADD pipeline_key elem%d\r\n", i)
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			t.Fatalf("Failed to send command %d: %v", i, err)
		}
	}

	// Now read all responses
	reader := bufio.NewReader(conn)
	for i := 0; i < pipelineSize; i++ {
		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read response %d: %v", i, err)
		}
		if response != ":0\r\n" && response != ":1\r\n" {
			t.Errorf("Unexpected response %d: %q", i, response)
		}
	}

	// Verify final count
	_, _ = conn.Write([]byte("HLL.COUNT pipeline_key\r\n"))
	response, _ := reader.ReadString('\n')
	t.Logf("Pipeline test: sent %d commands, HLL.COUNT response: %s", pipelineSize, response)
}

// TestStressMultiClientPipeline verifies multiple clients can pipeline
// commands simultaneously without interference.
func TestStressMultiClientPipeline(t *testing.T) {
	app := newStressTestApp(t, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 10
	const commandsPerClient = 1000

	var wg sync.WaitGroup
	var errors atomic.Int32

	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors.Add(1)
				return
			}
			defer func() { _ = conn.Close() }()

			key := fmt.Sprintf("client_%d_key", clientID)

			// Send all commands
			for i := 0; i < commandsPerClient; i++ {
				cmd := fmt.Sprintf("HLL.ADD %s elem%d\r\n", key, i)
				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors.Add(1)
					return
				}
			}

			// Read all responses
			reader := bufio.NewReader(conn)
			for i := 0; i < commandsPerClient; i++ {
				if _, err := reader.ReadString('\n'); err != nil {
					errors.Add(1)
					return
				}
			}
		}(c)
	}

	wg.Wait()

	if e := errors.Load(); e > 0 {
		t.Errorf("Encountered %d errors during multi-client pipeline", e)
	}

	t.Logf("Multi-client pipeline: %d clients × %d commands = %d total",
		clients, commandsPerClient, clients*commandsPerClient)
}

// =============================================================================
// Memory Pressure Tests
// =============================================================================

// TestStressManyKeys verifies the server handles a large number of distinct keys.
func TestStressManyKeys(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const numKeys = 10000

	reader := bufio.NewReader(conn)

	// Create many distinct HLLs
	for i := 0; i < numKeys; i++ {
		cmd := fmt.Sprintf("HLL.ADD stress_key_%d value\r\n", i)
		_, _ = conn.Write([]byte(cmd))
		_, _ = reader.ReadString('\n')
	}

	// Verify we can still access them
	_, _ = conn.Write([]byte("HLL.COUNT stress_key_0\r\n"))
	response, _ := reader.ReadString('\n')

	if response != ":1\r\n" {
		t.Errorf("Unexpected count for first key: %s", response)
	}

	t.Logf("Created and verified %d distinct HLL keys", numKeys)
}

// TestStressLargeValues verifies the server handles large HLL values
// (forcing dense mode with many elements).
func TestStressLargeValues(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// Add enough elements to force dense mode (~16KB per HLL)
	const elementsPerKey = 2000
	const numKeys = 100

	reader := bufio.NewReader(conn)

	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("large_hll_%d", k)
		for i := 0; i < elementsPerKey; i++ {
			cmd := fmt.Sprintf("HLL.ADD %s elem%d\r\n", key, i)
			_, _ = conn.Write([]byte(cmd))
			_, _ = reader.ReadString('\n')
		}
	}

	// Verify counts are reasonable
	_, _ = conn.Write([]byte("HLL.COUNT large_hll_0\r\n"))
	response, _ := reader.ReadString('\n')

	t.Logf("Created %d dense HLLs with ~%d elements each. Sample count: %s",
		numKeys, elementsPerKey, response)
}

// =============================================================================
// Sustained Load Tests
// =============================================================================

// TestStressSustainedLoad runs a sustained workload for a period of time.
func TestStressSustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping sustained load test in short mode")
	}

	app := newStressTestApp(t, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const duration = 2 * time.Second
	const workers = 10

	var totalOps atomic.Int64
	var errors atomic.Int64

	ctx := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors.Add(1)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)
			key := fmt.Sprintf("sustained_key_%d", workerID)

			for {
				select {
				case <-ctx:
					return
				default:
					// Mix of operations
					cmd := fmt.Sprintf("HLL.ADD %s elem%d\r\n", key, totalOps.Load())
					if _, err := conn.Write([]byte(cmd)); err != nil {
						errors.Add(1)
						return
					}
					if _, err := reader.ReadString('\n'); err != nil {
						errors.Add(1)
						return
					}
					totalOps.Add(1)
				}
			}
		}(w)
	}

	time.Sleep(duration)
	close(ctx)
	wg.Wait()

	opsPerSec := float64(totalOps.Load()) / duration.Seconds()
	t.Logf("Sustained load: %d ops in %v (%.0f ops/sec), errors: %d",
		totalOps.Load(), duration, opsPerSec, errors.Load())

	if errors.Load() > 0 {
		t.Errorf("Encountered %d errors during sustained load", errors.Load())
	}
}

// =============================================================================
// Bloom Filter Stress Tests
// =============================================================================

// TestStressBloomFilterPipeline verifies the server handles large Bloom Filter
// command pipelines without blocking or running out of memory.
func TestStressBloomFilterPipeline(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const pipelineSize = 10000

	// Send all BF.ADD commands without waiting for responses
	for i := 0; i < pipelineSize; i++ {
		cmd := fmt.Sprintf("BF.ADD bf_pipeline_key elem%d\r\n", i)
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			t.Fatalf("Failed to send command %d: %v", i, err)
		}
	}

	// Now read all responses
	reader := bufio.NewReader(conn)
	for i := 0; i < pipelineSize; i++ {
		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read response %d: %v", i, err)
		}
		if response != ":0\r\n" && response != ":1\r\n" {
			t.Errorf("Unexpected response %d: %q", i, response)
		}
	}

	// Verify a sample of elements exist (no false negatives)
	for i := 0; i < 10; i++ {
		cmd := fmt.Sprintf("BF.EXISTS bf_pipeline_key elem%d\r\n", i*1000)
		_, _ = conn.Write([]byte(cmd))
		response, _ := reader.ReadString('\n')
		if response != ":1\r\n" {
			t.Errorf("False negative for elem%d: got %s", i*1000, response)
		}
	}

	t.Logf("Bloom Filter pipeline test: sent %d BF.ADD commands", pipelineSize)
}

// TestStressManyBloomFilters verifies the server handles a large number of
// distinct Bloom Filter keys.
func TestStressManyBloomFilters(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const numFilters = 1000
	const elementsPerFilter = 10

	reader := bufio.NewReader(conn)

	// Create many distinct Bloom Filters
	for f := 0; f < numFilters; f++ {
		for e := 0; e < elementsPerFilter; e++ {
			cmd := fmt.Sprintf("BF.ADD bf_stress_%d elem%d\r\n", f, e)
			_, _ = conn.Write([]byte(cmd))
			_, _ = reader.ReadString('\n')
		}
	}

	// Verify we can still access them
	_, _ = conn.Write([]byte("BF.EXISTS bf_stress_0 elem0\r\n"))
	response, _ := reader.ReadString('\n')

	if response != ":1\r\n" {
		t.Errorf("Unexpected response for first filter: %s", response)
	}

	// Verify last filter too
	_, _ = fmt.Fprintf(conn, "BF.EXISTS bf_stress_%d elem0\r\n", numFilters-1)
	response, _ = reader.ReadString('\n')

	if response != ":1\r\n" {
		t.Errorf("Unexpected response for last filter: %s", response)
	}

	t.Logf("Created and verified %d distinct Bloom Filters with %d elements each",
		numFilters, elementsPerFilter)
}

// TestStressScaledBloomFilter verifies the server handles Bloom Filters that
// grow beyond their initial capacity, triggering multiple layer allocations.
func TestStressScaledBloomFilter(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// Add many more elements than the default capacity (1000) to force scaling
	const numElements = 10000

	reader := bufio.NewReader(conn)

	// Add elements to force the filter to scale
	for i := 0; i < numElements; i++ {
		cmd := fmt.Sprintf("BF.ADD bf_scaled elem%d\r\n", i)
		_, _ = conn.Write([]byte(cmd))
		_, _ = reader.ReadString('\n')
	}

	// Verify no false negatives by checking all elements
	falseNegatives := 0
	for i := 0; i < numElements; i++ {
		cmd := fmt.Sprintf("BF.EXISTS bf_scaled elem%d\r\n", i)
		_, _ = conn.Write([]byte(cmd))
		response, _ := reader.ReadString('\n')
		if response != ":1\r\n" {
			falseNegatives++
		}
	}

	if falseNegatives > 0 {
		t.Errorf("Scaled Bloom Filter has %d false negatives (should be 0)", falseNegatives)
	}

	// Count false positives for elements we never added
	falsePositives := 0
	const checkCount = 10000
	for i := 0; i < checkCount; i++ {
		cmd := fmt.Sprintf("BF.EXISTS bf_scaled notexist%d\r\n", i)
		_, _ = conn.Write([]byte(cmd))
		response, _ := reader.ReadString('\n')
		if response == ":1\r\n" {
			falsePositives++
		}
	}

	fpr := float64(falsePositives) / float64(checkCount) * 100
	t.Logf("Scaled Bloom Filter: %d elements added, %d false negatives, %.2f%% FPR",
		numElements, falseNegatives, fpr)

	// FPR should be reasonable (scaled filters have higher FPR due to multiple layers)
	if fpr > 10.0 {
		t.Errorf("FPR too high: %.2f%% (expected < 10%%)", fpr)
	}
}

// TestStressBloomFilterMultiClient verifies multiple clients can pipeline
// Bloom Filter commands simultaneously without interference.
func TestStressBloomFilterMultiClient(t *testing.T) {
	app := newStressTestApp(t, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 10
	const commandsPerClient = 1000

	var wg sync.WaitGroup
	var errors atomic.Int32

	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors.Add(1)
				return
			}
			defer func() { _ = conn.Close() }()

			key := fmt.Sprintf("bf_client_%d", clientID)

			// Send all commands
			for i := 0; i < commandsPerClient; i++ {
				cmd := fmt.Sprintf("BF.ADD %s elem%d\r\n", key, i)
				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors.Add(1)
					return
				}
			}

			// Read all responses
			reader := bufio.NewReader(conn)
			for i := 0; i < commandsPerClient; i++ {
				if _, err := reader.ReadString('\n'); err != nil {
					errors.Add(1)
					return
				}
			}
		}(c)
	}

	wg.Wait()

	if e := errors.Load(); e > 0 {
		t.Errorf("Encountered %d errors during multi-client Bloom Filter pipeline", e)
	}

	t.Logf("Multi-client Bloom Filter pipeline: %d clients × %d commands = %d total",
		clients, commandsPerClient, clients*commandsPerClient)
}

// =============================================================================
// Count-Min Sketch Stress Tests
// =============================================================================

// TestStressCMSPipeline verifies the server handles large CMS command pipelines
// without blocking or running out of memory.
func TestStressCMSPipeline(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)

	// First, create the CMS
	_, _ = conn.Write([]byte("CMS.INIT cms_pipeline_key 10000 7\r\n"))
	_, _ = reader.ReadString('\n')

	const pipelineSize = 10000

	// Send all CMS.INCRBY commands without waiting for responses
	for i := 0; i < pipelineSize; i++ {
		cmd := fmt.Sprintf("CMS.INCRBY cms_pipeline_key elem%d 1\r\n", i)
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			t.Fatalf("Failed to send command %d: %v", i, err)
		}
	}

	// Now read all responses
	for i := 0; i < pipelineSize; i++ {
		// Read array header
		header, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read header %d: %v", i, err)
		}
		if header != "*1\r\n" {
			t.Errorf("Unexpected header %d: %q", i, header)
		}
		// Read count value
		_, err = reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read count %d: %v", i, err)
		}
	}

	// Verify a sample of elements
	for i := 0; i < 10; i++ {
		cmd := fmt.Sprintf("CMS.QUERY cms_pipeline_key elem%d\r\n", i*1000)
		_, _ = conn.Write([]byte(cmd))
		header, _ := reader.ReadString('\n')
		if header != "*1\r\n" {
			t.Errorf("Query header for elem%d: %q", i*1000, header)
		}
		count, _ := reader.ReadString('\n')
		if count != ":1\r\n" {
			t.Errorf("Expected :1 for elem%d, got %s", i*1000, count)
		}
	}

	t.Logf("CMS pipeline test: sent %d CMS.INCRBY commands", pipelineSize)
}

// TestStressManyCMS verifies the server handles a large number of distinct CMS keys.
func TestStressManyCMS(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const numCMS = 100
	const elementsPerCMS = 100

	reader := bufio.NewReader(conn)

	// Create many distinct CMS structures
	for c := 0; c < numCMS; c++ {
		cmd := fmt.Sprintf("CMS.INIT cms_stress_%d 1000 5\r\n", c)
		_, _ = conn.Write([]byte(cmd))
		_, _ = reader.ReadString('\n')

		for e := 0; e < elementsPerCMS; e++ {
			cmd := fmt.Sprintf("CMS.INCRBY cms_stress_%d elem%d 1\r\n", c, e)
			_, _ = conn.Write([]byte(cmd))
			_, _ = reader.ReadString('\n') // header
			_, _ = reader.ReadString('\n') // count
		}
	}

	// Verify we can still access them
	_, _ = conn.Write([]byte("CMS.QUERY cms_stress_0 elem0\r\n"))
	header, _ := reader.ReadString('\n')
	if header != "*1\r\n" {
		t.Errorf("Unexpected header for first CMS: %s", header)
	}
	count, _ := reader.ReadString('\n')
	if count != ":1\r\n" {
		t.Errorf("Unexpected count for first CMS: %s", count)
	}

	// Verify last CMS too
	_, _ = fmt.Fprintf(conn, "CMS.QUERY cms_stress_%d elem0\r\n", numCMS-1)
	header, _ = reader.ReadString('\n')
	if header != "*1\r\n" {
		t.Errorf("Unexpected header for last CMS: %s", header)
	}
	count, _ = reader.ReadString('\n')
	if count != ":1\r\n" {
		t.Errorf("Unexpected count for last CMS: %s", count)
	}

	t.Logf("Created and verified %d distinct CMS structures with %d elements each",
		numCMS, elementsPerCMS)
}

// TestStressCMSMultiClient verifies multiple clients can pipeline
// CMS commands simultaneously without interference.
func TestStressCMSMultiClient(t *testing.T) {
	app := newStressTestApp(t, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 10
	const commandsPerClient = 1000

	// Pre-create CMS for each client
	setupConn, _ := net.Dial("tcp", app.listener.Addr().String())
	setupReader := bufio.NewReader(setupConn)
	for c := 0; c < clients; c++ {
		cmd := fmt.Sprintf("CMS.INIT cms_client_%d 1000 5\r\n", c)
		_, _ = setupConn.Write([]byte(cmd))
		_, _ = setupReader.ReadString('\n')
	}
	_ = setupConn.Close()

	var wg sync.WaitGroup
	var errors atomic.Int32

	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors.Add(1)
				return
			}
			defer func() { _ = conn.Close() }()

			key := fmt.Sprintf("cms_client_%d", clientID)

			// Send all commands
			for i := 0; i < commandsPerClient; i++ {
				cmd := fmt.Sprintf("CMS.INCRBY %s elem%d 1\r\n", key, i%100)
				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors.Add(1)
					return
				}
			}

			// Read all responses
			reader := bufio.NewReader(conn)
			for i := 0; i < commandsPerClient; i++ {
				if _, err := reader.ReadString('\n'); err != nil { // header
					errors.Add(1)
					return
				}
				if _, err := reader.ReadString('\n'); err != nil { // count
					errors.Add(1)
					return
				}
			}
		}(c)
	}

	wg.Wait()

	if e := errors.Load(); e > 0 {
		t.Errorf("Encountered %d errors during multi-client CMS pipeline", e)
	}

	t.Logf("Multi-client CMS pipeline: %d clients × %d commands = %d total",
		clients, commandsPerClient, clients*commandsPerClient)
}

// TestStressCMSHeavyHitter tests CMS behavior with heavy hitter items
// (items that are incremented much more frequently than others).
func TestStressCMSHeavyHitter(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)

	// Create CMS
	_, _ = conn.Write([]byte("CMS.INIT cms_heavyhitter 1000 5\r\n"))
	_, _ = reader.ReadString('\n')

	// Add a heavy hitter with count 10000
	_, _ = conn.Write([]byte("CMS.INCRBY cms_heavyhitter heavy 10000\r\n"))
	_, _ = reader.ReadString('\n') // header
	_, _ = reader.ReadString('\n') // count

	// Add many light items with count 1 each
	const lightItems = 1000
	for i := 0; i < lightItems; i++ {
		cmd := fmt.Sprintf("CMS.INCRBY cms_heavyhitter light%d 1\r\n", i)
		_, _ = conn.Write([]byte(cmd))
		_, _ = reader.ReadString('\n') // header
		_, _ = reader.ReadString('\n') // count
	}

	// Query heavy hitter - should be close to 10000
	_, _ = conn.Write([]byte("CMS.QUERY cms_heavyhitter heavy\r\n"))
	_, _ = reader.ReadString('\n') // header
	heavyCount, _ := reader.ReadString('\n')

	// Query a light item - should be 1 (or slightly more due to collisions)
	_, _ = conn.Write([]byte("CMS.QUERY cms_heavyhitter light0\r\n"))
	_, _ = reader.ReadString('\n') // header
	lightCount, _ := reader.ReadString('\n')

	t.Logf("Heavy hitter test: heavy=%s light=%s", heavyCount, lightCount)

	// With Conservative Update, light items should not be heavily polluted
	// by the heavy hitter. The light count should be small (ideally 1).
}

// =============================================================================
// String Command Stress Tests
// =============================================================================

// TestStressStringPipeline verifies the server handles large SET/GET pipelines.
func TestStressStringPipeline(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const pipelineSize = 5000

	// Send all SET commands without waiting for responses
	for i := 0; i < pipelineSize; i++ {
		cmd := fmt.Sprintf("SET string_key_%d value_%d\r\n", i, i)
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			t.Fatalf("Failed to send command %d: %v", i, err)
		}
	}

	// Read all SET responses
	reader := bufio.NewReader(conn)
	for i := 0; i < pipelineSize; i++ {
		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read SET response %d: %v", i, err)
		}
		if response != "+OK\r\n" {
			t.Errorf("Unexpected SET response %d: %q", i, response)
		}
	}

	// Now GET all keys
	for i := 0; i < pipelineSize; i++ {
		cmd := fmt.Sprintf("GET string_key_%d\r\n", i)
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			t.Fatalf("Failed to send GET command %d: %v", i, err)
		}
	}

	// Read all GET responses
	for i := 0; i < pipelineSize; i++ {
		// Read length line
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read GET length %d: %v", i, err)
		}
		if line[0] != '$' {
			t.Errorf("Unexpected GET response type %d: %q", i, line)
			continue
		}
		// Read value
		_, err = reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read GET value %d: %v", i, err)
		}
	}

	t.Logf("String pipeline test: sent %d SET + %d GET commands", pipelineSize, pipelineSize)
}

// TestStressIncrPipeline verifies the server handles large INCR pipelines.
func TestStressIncrPipeline(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const pipelineSize = 10000

	// Send all INCR commands to the same key
	for i := 0; i < pipelineSize; i++ {
		_, err := conn.Write([]byte("INCR stress_counter\r\n"))
		if err != nil {
			t.Fatalf("Failed to send command %d: %v", i, err)
		}
	}

	// Read all responses
	reader := bufio.NewReader(conn)
	for i := 0; i < pipelineSize; i++ {
		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read response %d: %v", i, err)
		}
		// Response should be :N where N is i+1
		expected := fmt.Sprintf(":%d\r\n", i+1)
		if response != expected {
			t.Errorf("INCR response %d: got %q, want %q", i, response, expected)
		}
	}

	// Verify final value via GET
	_, _ = conn.Write([]byte("GET stress_counter\r\n"))
	line, _ := reader.ReadString('\n')
	value, _ := reader.ReadString('\n')
	t.Logf("INCR pipeline test: %d commands, final value: %s", pipelineSize, value[:len(value)-2])

	_ = line // suppress unused
}

// TestStressStringMultiClient verifies multiple clients can SET/GET simultaneously.
func TestStressStringMultiClient(t *testing.T) {
	app := newStressTestApp(t, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 10
	const commandsPerClient = 1000

	var wg sync.WaitGroup
	var errors atomic.Int32

	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors.Add(1)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			// Each client works on its own keys
			for i := 0; i < commandsPerClient; i++ {
				key := fmt.Sprintf("client_%d_key_%d", clientID, i%100)
				value := fmt.Sprintf("value_%d_%d", clientID, i)
				cmd := fmt.Sprintf("SET %s %s\r\n", key, value)

				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors.Add(1)
					return
				}

				response, err := reader.ReadString('\n')
				if err != nil {
					errors.Add(1)
					return
				}

				if response != "+OK\r\n" {
					errors.Add(1)
				}
			}
		}(c)
	}

	wg.Wait()

	if e := errors.Load(); e > 0 {
		t.Errorf("Encountered %d errors during multi-client String operations", e)
	}

	t.Logf("Multi-client String pipeline: %d clients × %d commands = %d total",
		clients, commandsPerClient, clients*commandsPerClient)
}

// TestStressManyStringKeys verifies the server handles many distinct string keys.
func TestStressManyStringKeys(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const numKeys = 10000
	reader := bufio.NewReader(conn)

	// Create many unique keys
	for i := 0; i < numKeys; i++ {
		cmd := fmt.Sprintf("SET unique_string_%d value_%d\r\n", i, i)
		_, _ = conn.Write([]byte(cmd))
	}

	// Read all responses
	for i := 0; i < numKeys; i++ {
		if _, err := reader.ReadString('\n'); err != nil {
			t.Fatalf("Failed at key %d: %v", i, err)
		}
	}

	// Verify a sample of keys
	sampleSize := 100
	for i := 0; i < sampleSize; i++ {
		keyIdx := i * (numKeys / sampleSize)
		cmd := fmt.Sprintf("GET unique_string_%d\r\n", keyIdx)
		_, _ = conn.Write([]byte(cmd))
	}

	// Read and verify sample
	for i := 0; i < sampleSize; i++ {
		line, _ := reader.ReadString('\n')
		if line == "$-1\r\n" {
			t.Errorf("Key unique_string_%d missing", i*(numKeys/sampleSize))
			continue
		}
		_, _ = reader.ReadString('\n') // read value
	}

	t.Logf("Created and verified %d distinct String keys", numKeys)
}

// =============================================================================
// Top-K Stress Tests
// =============================================================================

// TestStressTopKPipeline verifies the server handles large Top-K command pipelines
// without blocking or running out of memory.
func TestStressTopKPipeline(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)

	// First, create the TopK
	_, _ = conn.Write([]byte("TOPK.RESERVE topk_pipeline_key 100\r\n"))
	_, _ = reader.ReadString('\n')

	const pipelineSize = 10000

	// Send all TOPK.ADD commands without waiting for responses
	for i := 0; i < pipelineSize; i++ {
		cmd := fmt.Sprintf("TOPK.ADD topk_pipeline_key elem%d\r\n", i)
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			t.Fatalf("Failed to send command %d: %v", i, err)
		}
	}

	// Now read all responses (each response is an array with 1 element)
	for i := 0; i < pipelineSize; i++ {
		// Read array header
		header, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read header %d: %v", i, err)
		}
		if header != "*1\r\n" {
			t.Errorf("Unexpected header %d: %q", i, header)
		}
		// Read the element (either $-1\r\n for nil or $N\r\n + key + \r\n)
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read element %d: %v", i, err)
		}
		// If not nil, read the actual value
		if line != "$-1\r\n" {
			_, _ = reader.ReadString('\n') // read the expelled key
		}
	}

	// Verify TopK list
	_, _ = conn.Write([]byte("TOPK.LIST topk_pipeline_key\r\n"))
	header, _ := reader.ReadString('\n')
	t.Logf("TopK pipeline test: sent %d TOPK.ADD commands, LIST header: %s", pipelineSize, header)
}

// TestStressManyTopK verifies the server handles a large number of distinct TopK keys.
func TestStressManyTopK(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	const numTopK = 100
	const elementsPerTopK = 50

	reader := bufio.NewReader(conn)

	// Create many distinct TopK structures (auto-create via TOPK.ADD)
	for tk := 0; tk < numTopK; tk++ {
		for e := 0; e < elementsPerTopK; e++ {
			cmd := fmt.Sprintf("TOPK.ADD topk_stress_%d elem%d\r\n", tk, e)
			_, _ = conn.Write([]byte(cmd))
			// Read response
			header, _ := reader.ReadString('\n')
			if header != "*1\r\n" {
				t.Errorf("Unexpected header for topk %d elem %d: %s", tk, e, header)
			}
			line, _ := reader.ReadString('\n')
			if line != "$-1\r\n" {
				_, _ = reader.ReadString('\n') // read expelled key
			}
		}
	}

	// Verify we can still access them
	_, _ = conn.Write([]byte("TOPK.QUERY topk_stress_0 elem0\r\n"))
	header, _ := reader.ReadString('\n')
	if header != "*1\r\n" {
		t.Errorf("Unexpected header for first TopK: %s", header)
	}
	result, _ := reader.ReadString('\n')
	if result != ":1\r\n" {
		t.Errorf("Unexpected result for first TopK elem0: %s", result)
	}

	// Verify last TopK too
	_, _ = fmt.Fprintf(conn, "TOPK.QUERY topk_stress_%d elem0\r\n", numTopK-1)
	header, _ = reader.ReadString('\n')
	if header != "*1\r\n" {
		t.Errorf("Unexpected header for last TopK: %s", header)
	}
	result, _ = reader.ReadString('\n')
	if result != ":1\r\n" {
		t.Errorf("Unexpected result for last TopK elem0: %s", result)
	}

	t.Logf("Created and verified %d distinct TopK structures with %d elements each",
		numTopK, elementsPerTopK)
}

// TestStressTopKMultiClient verifies multiple clients can pipeline
// TopK commands simultaneously without interference.
func TestStressTopKMultiClient(t *testing.T) {
	app := newStressTestApp(t, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 10
	const commandsPerClient = 500

	var wg sync.WaitGroup
	var errors atomic.Int32

	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors.Add(1)
				return
			}
			defer func() { _ = conn.Close() }()

			key := fmt.Sprintf("topk_client_%d", clientID)

			// Send all commands
			for i := 0; i < commandsPerClient; i++ {
				cmd := fmt.Sprintf("TOPK.ADD %s elem%d\r\n", key, i%100)
				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors.Add(1)
					return
				}
			}

			// Read all responses
			reader := bufio.NewReader(conn)
			for i := 0; i < commandsPerClient; i++ {
				// Read array header
				header, err := reader.ReadString('\n')
				if err != nil {
					errors.Add(1)
					return
				}
				if header != "*1\r\n" {
					errors.Add(1)
					return
				}
				// Read element
				line, err := reader.ReadString('\n')
				if err != nil {
					errors.Add(1)
					return
				}
				if line != "$-1\r\n" {
					// Read expelled key
					if _, err := reader.ReadString('\n'); err != nil {
						errors.Add(1)
						return
					}
				}
			}
		}(c)
	}

	wg.Wait()

	if e := errors.Load(); e > 0 {
		t.Errorf("Encountered %d errors during multi-client TopK pipeline", e)
	}

	t.Logf("Multi-client TopK pipeline: %d clients × %d commands = %d total",
		clients, commandsPerClient, clients*commandsPerClient)
}

// TestStressTopKHeavyHitter tests TopK behavior with heavy hitter items
// (items that are added much more frequently than others).
func TestStressTopKHeavyHitter(t *testing.T) {
	app := newStressTestApp(t, 10)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	conn, err := net.Dial("tcp", app.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)

	// Create TopK with small K
	_, _ = conn.Write([]byte("TOPK.RESERVE topk_heavyhitter 10\r\n"))
	_, _ = reader.ReadString('\n')

	// Add a heavy hitter 1000 times
	for i := 0; i < 1000; i++ {
		_, _ = conn.Write([]byte("TOPK.ADD topk_heavyhitter heavy\r\n"))
		header, _ := reader.ReadString('\n')
		if header != "*1\r\n" {
			t.Errorf("Unexpected header: %s", header)
		}
		line, _ := reader.ReadString('\n')
		if line != "$-1\r\n" {
			_, _ = reader.ReadString('\n')
		}
	}

	// Add many light items with count 1 each
	const lightItems = 500
	for i := 0; i < lightItems; i++ {
		cmd := fmt.Sprintf("TOPK.ADD topk_heavyhitter light%d\r\n", i)
		_, _ = conn.Write([]byte(cmd))
		header, _ := reader.ReadString('\n')
		if header != "*1\r\n" {
			t.Errorf("Unexpected header: %s", header)
		}
		line, _ := reader.ReadString('\n')
		if line != "$-1\r\n" {
			_, _ = reader.ReadString('\n')
		}
	}

	// Query heavy hitter - should be in TopK
	_, _ = conn.Write([]byte("TOPK.QUERY topk_heavyhitter heavy\r\n"))
	header, _ := reader.ReadString('\n')
	if header != "*1\r\n" {
		t.Errorf("Unexpected query header: %s", header)
	}
	result, _ := reader.ReadString('\n')

	// List TopK with counts
	_, _ = conn.Write([]byte("TOPK.LIST topk_heavyhitter WITHCOUNT\r\n"))
	listHeader, _ := reader.ReadString('\n')

	t.Logf("Heavy hitter test: heavy query result=%s, list header=%s", result, listHeader)

	// Heavy should be in TopK (result should be :1)
	if result != ":1\r\n" {
		t.Errorf("Heavy hitter not in TopK! Got %s", result)
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

func newStressTestApp(t *testing.T, maxConn int) *application {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	app := &application{
		config:      config{port: 0, maxConnections: maxConn},
		logger:      logger,
		store:       NewStore(),
		metrics:     NewMetrics(),
		readyCh:     make(chan struct{}),
		connLimiter: make(chan struct{}, maxConn),
	}
	app.router = app.commands()
	return app
}
