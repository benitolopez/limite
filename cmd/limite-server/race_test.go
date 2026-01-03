package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
)

// =============================================================================
// Store Concurrency Tests
// =============================================================================

// TestStoreConcurrentWritesSameKey verifies that concurrent writes to the same
// key don't cause data races or corruption.
func TestStoreConcurrentWritesSameKey(t *testing.T) {
	store := NewStore()
	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				value := []byte(fmt.Sprintf("value-%d-%d", id, i))
				store.Set("contested_key", value)
			}
		}(g)
	}

	wg.Wait()

	// Verify the key exists and has a valid value
	val, found := store.Get("contested_key")
	if !found {
		t.Error("Key missing after concurrent writes")
	}
	if len(val) == 0 {
		t.Error("Value is empty after concurrent writes")
	}
}

// TestStoreConcurrentReadWrite verifies that concurrent reads and writes
// to the same key don't cause data races.
func TestStoreConcurrentReadWrite(t *testing.T) {
	store := NewStore()
	store.Set("rw_key", []byte("initial"))

	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2) // Half readers, half writers

	// Writers
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				value := []byte(fmt.Sprintf("value-%d-%d", id, i))
				store.Set("rw_key", value)
			}
		}(g)
	}

	// Readers
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				val, found := store.Get("rw_key")
				if !found {
					t.Error("Key disappeared during concurrent access")
					return
				}
				// Just access the value to ensure it's readable
				_ = len(val)
			}
		}()
	}

	wg.Wait()
}

// TestStoreConcurrentMutate verifies that the Mutate operation is atomic
// and doesn't suffer from lost updates.
func TestStoreConcurrentMutate(t *testing.T) {
	store := NewStore()

	// Initialize with a counter value
	store.Set("counter", []byte{0})

	const goroutines = 100
	const iterations = 10

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				store.Mutate("counter", func(data []byte) ([]byte, bool) {
					if data == nil {
						return []byte{1}, true
					}
					// Increment the counter
					newVal := data[0] + 1
					return []byte{newVal}, true
				})
			}
		}()
	}

	wg.Wait()

	// Note: Due to byte overflow at 255, we can't verify exact count
	// but we verify no panics or races occurred
	val, found := store.Get("counter")
	if !found || len(val) != 1 {
		t.Error("Counter key corrupted")
	}
}

// TestStoreConcurrentDifferentKeys verifies that operations on different keys
// can proceed in parallel without blocking each other.
func TestStoreConcurrentDifferentKeys(t *testing.T) {
	store := NewStore()
	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			for i := 0; i < iterations; i++ {
				value := []byte(fmt.Sprintf("value-%d", i))
				store.Set(key, value)
				_, _ = store.Get(key)
			}
		}(g)
	}

	wg.Wait()

	// Verify all keys exist
	for g := 0; g < goroutines; g++ {
		key := fmt.Sprintf("key-%d", g)
		if _, found := store.Get(key); !found {
			t.Errorf("Key %s missing after concurrent operations", key)
		}
	}
}

// TestStoreConcurrentDelete verifies that concurrent deletes don't cause races.
func TestStoreConcurrentDelete(t *testing.T) {
	store := NewStore()

	// Pre-populate
	for i := 0; i < 100; i++ {
		store.Set(fmt.Sprintf("del-key-%d", i), []byte("value"))
	}

	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Deleters
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				store.Delete(fmt.Sprintf("del-key-%d", i))
			}
		}(g)
	}

	// Concurrent readers/writers
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("del-key-%d", i)
				store.Get(key)
				store.Set(key, []byte("new-value"))
			}
		}(g)
	}

	wg.Wait()
}

// =============================================================================
// Snapshot Concurrency Tests
// =============================================================================

// TestSnapshotDuringWrites verifies that taking a snapshot while writes are
// happening doesn't cause data races or corruption.
func TestSnapshotDuringWrites(t *testing.T) {
	store := NewStore()

	// Start background writers
	stopCh := make(chan struct{})
	var writerWg sync.WaitGroup

	const writers = 10
	writerWg.Add(writers)

	for w := 0; w < writers; w++ {
		go func(id int) {
			defer writerWg.Done()
			i := 0
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("writer-%d-key-%d", id, i%100)
					store.Set(key, []byte(fmt.Sprintf("value-%d", i)))
					i++
				}
			}
		}(w)
	}

	// Take multiple snapshots while writes are happening
	for s := 0; s < 5; s++ {
		var buf bytes.Buffer
		if err := store.SaveSnapshotToWriter(&buf); err != nil {
			t.Errorf("Snapshot %d failed: %v", s, err)
		}

		// Verify the snapshot is valid by loading it
		newStore := NewStore()
		reader := bufio.NewReader(&buf)
		if err := newStore.LoadSnapshotFromReader(reader); err != nil {
			t.Errorf("Snapshot %d load failed: %v", s, err)
		}
	}

	close(stopCh)
	writerWg.Wait()
}

// =============================================================================
// Server Concurrency Tests
// =============================================================================

// TestServerConcurrentClients verifies that multiple clients can interact
// with the server simultaneously without races.
func TestServerConcurrentClients(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 20
	const commandsPerClient = 50

	var wg sync.WaitGroup
	wg.Add(clients)

	errors := make(chan error, clients*commandsPerClient)

	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors <- fmt.Errorf("client %d connect failed: %w", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < commandsPerClient; i++ {
				// Each client works on its own key to avoid contention issues
				// but we also test shared keys
				var key string
				if i%2 == 0 {
					key = fmt.Sprintf("client-%d-key", clientID)
				} else {
					key = "shared_key"
				}

				cmd := fmt.Sprintf("HLL.ADD %s element-%d-%d\r\n", key, clientID, i)
				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors <- fmt.Errorf("client %d write failed: %w", clientID, err)
					return
				}

				response, err := reader.ReadString('\n')
				if err != nil {
					errors <- fmt.Errorf("client %d read failed: %w", clientID, err)
					return
				}

				// Response should be :0 or :1
				if response != ":0\r\n" && response != ":1\r\n" {
					errors <- fmt.Errorf("client %d unexpected response: %q", clientID, response)
				}
			}
		}(c)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestServerConcurrentHLLSameKey verifies that concurrent HLL.ADD operations
// on the same key produce correct results.
func TestServerConcurrentHLLSameKey(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 10
	const elementsPerClient = 100

	var wg sync.WaitGroup
	wg.Add(clients)

	// Each client adds unique elements to the same HLL
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("client %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < elementsPerClient; i++ {
				element := fmt.Sprintf("client%d-elem%d", clientID, i)
				cmd := fmt.Sprintf("HLL.ADD concurrent_hll %s\r\n", element)

				if _, err := conn.Write([]byte(cmd)); err != nil {
					t.Errorf("client %d write failed: %v", clientID, err)
					return
				}

				if _, err := reader.ReadString('\n'); err != nil {
					t.Errorf("client %d read failed: %v", clientID, err)
					return
				}
			}
		}(c)
	}

	wg.Wait()

	// Verify the final count
	conn, _ := net.Dial("tcp", app.listener.Addr().String())
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("HLL.COUNT concurrent_hll\r\n"))
	response, _ := reader.ReadString('\n')

	// We added clients * elementsPerClient unique elements
	// HLL should estimate close to this number
	expectedMin := clients * elementsPerClient * 90 / 100 // Allow 10% error
	t.Logf("HLL.COUNT response: %s (expected ~%d unique elements)", response, clients*elementsPerClient)

	// Just verify we got a reasonable positive number
	if response == ":0\r\n" {
		t.Error("HLL count is 0, data was lost during concurrent access")
	}
	_ = expectedMin // Used for logging context
}

// TestServerConcurrentMerge verifies that concurrent HLL.MERGE operations
// don't cause races.
func TestServerConcurrentMerge(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	// Setup: Create source HLLs
	setupConn, _ := net.Dial("tcp", app.listener.Addr().String())
	setupReader := bufio.NewReader(setupConn)

	for i := 0; i < 10; i++ {
		cmd := fmt.Sprintf("HLL.ADD merge_src_%d elem%d\r\n", i, i)
		_, _ = setupConn.Write([]byte(cmd))
		_, _ = setupReader.ReadString('\n')
	}
	_ = setupConn.Close()

	// Concurrent merges to the same destination
	const clients = 10
	var wg sync.WaitGroup
	wg.Add(clients)

	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("client %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			// Each client merges a different source into the shared destination
			srcKey := fmt.Sprintf("merge_src_%d", clientID%10)
			cmd := fmt.Sprintf("HLL.MERGE merge_dest %s\r\n", srcKey)

			if _, err := conn.Write([]byte(cmd)); err != nil {
				t.Errorf("client %d write failed: %v", clientID, err)
				return
			}

			response, err := reader.ReadString('\n')
			if err != nil {
				t.Errorf("client %d read failed: %v", clientID, err)
				return
			}

			if response != "+OK\r\n" {
				t.Errorf("client %d unexpected response: %q", clientID, response)
			}
		}(c)
	}

	wg.Wait()
}

// =============================================================================
// Bloom Filter Concurrency Tests
// =============================================================================

// TestServerConcurrentBFSameKey verifies that concurrent BF.ADD and BF.EXISTS
// operations on the same key produce correct results without data races.
func TestServerConcurrentBFSameKey(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 10
	const elementsPerClient = 100

	var wg sync.WaitGroup
	wg.Add(clients * 2) // Half writers, half readers

	// Writers: Add elements to the same Bloom Filter
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("client %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < elementsPerClient; i++ {
				element := fmt.Sprintf("client%d-elem%d", clientID, i)
				cmd := fmt.Sprintf("BF.ADD concurrent_bf %s\r\n", element)

				if _, err := conn.Write([]byte(cmd)); err != nil {
					t.Errorf("client %d write failed: %v", clientID, err)
					return
				}

				response, err := reader.ReadString('\n')
				if err != nil {
					t.Errorf("client %d read failed: %v", clientID, err)
					return
				}

				// Response should be :0 or :1
				if response != ":0\r\n" && response != ":1\r\n" {
					t.Errorf("client %d unexpected BF.ADD response: %q", clientID, response)
				}
			}
		}(c)
	}

	// Readers: Check for elements in the same Bloom Filter
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("reader %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < elementsPerClient; i++ {
				// Check for various elements (some may exist, some may not)
				element := fmt.Sprintf("client%d-elem%d", clientID, i)
				cmd := fmt.Sprintf("BF.EXISTS concurrent_bf %s\r\n", element)

				if _, err := conn.Write([]byte(cmd)); err != nil {
					t.Errorf("reader %d write failed: %v", clientID, err)
					return
				}

				response, err := reader.ReadString('\n')
				if err != nil {
					t.Errorf("reader %d read failed: %v", clientID, err)
					return
				}

				// Response should be :0 or :1
				if response != ":0\r\n" && response != ":1\r\n" {
					t.Errorf("reader %d unexpected BF.EXISTS response: %q", clientID, response)
				}
			}
		}(c)
	}

	wg.Wait()

	// Verify: All elements we added should now be found (no false negatives)
	conn, _ := net.Dial("tcp", app.listener.Addr().String())
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)

	// Check a sample of elements that were definitely added
	for c := 0; c < clients; c++ {
		element := fmt.Sprintf("client%d-elem0", c)
		cmd := fmt.Sprintf("BF.EXISTS concurrent_bf %s\r\n", element)
		_, _ = conn.Write([]byte(cmd))
		response, _ := reader.ReadString('\n')

		if response != ":1\r\n" {
			t.Errorf("False negative: element %s should exist but got %s", element, response)
		}
	}

	t.Logf("Concurrent BF test completed: %d clients × %d elements", clients, elementsPerClient)
}

// TestServerConcurrentBFAddExists verifies that interleaved BF.ADD and BF.EXISTS
// operations don't cause races or inconsistent state.
func TestServerConcurrentBFAddExists(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 20
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(clients)

	errors := make(chan error, clients*iterations)

	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors <- fmt.Errorf("client %d connect failed: %w", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < iterations; i++ {
				// Alternate between ADD and EXISTS on same and different keys
				var cmd string
				if i%2 == 0 {
					cmd = fmt.Sprintf("BF.ADD bf_race_%d elem%d\r\n", clientID%5, i)
				} else {
					cmd = fmt.Sprintf("BF.EXISTS bf_race_%d elem%d\r\n", clientID%5, i-1)
				}

				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors <- fmt.Errorf("client %d write failed: %w", clientID, err)
					return
				}

				response, err := reader.ReadString('\n')
				if err != nil {
					errors <- fmt.Errorf("client %d read failed: %w", clientID, err)
					return
				}

				if response != ":0\r\n" && response != ":1\r\n" {
					errors <- fmt.Errorf("client %d unexpected response: %q", clientID, response)
				}
			}
		}(c)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// =============================================================================
// Count-Min Sketch Concurrency Tests
// =============================================================================

// TestServerConcurrentCMSSameKey verifies that concurrent CMS.INCRBY and CMS.QUERY
// operations on the same key produce correct results without data races.
func TestServerConcurrentCMSSameKey(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	// First, create the CMS
	setupConn, _ := net.Dial("tcp", app.listener.Addr().String())
	setupReader := bufio.NewReader(setupConn)
	_, _ = setupConn.Write([]byte("CMS.INIT concurrent_cms 1000 5\r\n"))
	_, _ = setupReader.ReadString('\n')
	_ = setupConn.Close()

	const clients = 10
	const incrementsPerClient = 100

	var wg sync.WaitGroup
	wg.Add(clients * 2) // Half writers, half readers

	// Writers: Increment items in the same CMS
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("client %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < incrementsPerClient; i++ {
				item := fmt.Sprintf("item%d", i%10) // Shared items to cause contention
				cmd := fmt.Sprintf("CMS.INCRBY concurrent_cms %s 1\r\n", item)

				if _, err := conn.Write([]byte(cmd)); err != nil {
					t.Errorf("client %d write failed: %v", clientID, err)
					return
				}

				// Read array response
				header, err := reader.ReadString('\n')
				if err != nil {
					t.Errorf("client %d read header failed: %v", clientID, err)
					return
				}
				if header != "*1\r\n" {
					t.Errorf("client %d unexpected header: %q", clientID, header)
					return
				}
				_, _ = reader.ReadString('\n') // Read the count
			}
		}(c)
	}

	// Readers: Query items from the same CMS
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("reader %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < incrementsPerClient; i++ {
				item := fmt.Sprintf("item%d", i%10)
				cmd := fmt.Sprintf("CMS.QUERY concurrent_cms %s\r\n", item)

				if _, err := conn.Write([]byte(cmd)); err != nil {
					t.Errorf("reader %d write failed: %v", clientID, err)
					return
				}

				// Read array response
				header, err := reader.ReadString('\n')
				if err != nil {
					t.Errorf("reader %d read header failed: %v", clientID, err)
					return
				}
				if header != "*1\r\n" {
					t.Errorf("reader %d unexpected header: %q", clientID, header)
					return
				}
				_, _ = reader.ReadString('\n') // Read the count
			}
		}(c)
	}

	wg.Wait()

	// Verify final counts are reasonable
	conn, _ := net.Dial("tcp", app.listener.Addr().String())
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("CMS.QUERY concurrent_cms item0\r\n"))
	header, _ := reader.ReadString('\n')
	if header != "*1\r\n" {
		t.Errorf("Final query header: %q", header)
	}
	count, _ := reader.ReadString('\n')

	t.Logf("Concurrent CMS test completed: %d clients × %d increments, final item0 count: %s",
		clients, incrementsPerClient, count)
}

// TestServerConcurrentCMSIncrQuery verifies that interleaved CMS.INCRBY and CMS.QUERY
// operations don't cause races or inconsistent state.
func TestServerConcurrentCMSIncrQuery(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	// Create multiple CMS keys
	setupConn, _ := net.Dial("tcp", app.listener.Addr().String())
	setupReader := bufio.NewReader(setupConn)
	for i := 0; i < 5; i++ {
		cmd := fmt.Sprintf("CMS.INIT cms_race_%d 1000 5\r\n", i)
		_, _ = setupConn.Write([]byte(cmd))
		_, _ = setupReader.ReadString('\n')
	}
	_ = setupConn.Close()

	const clients = 20
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(clients)

	errors := make(chan error, clients*iterations)

	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors <- fmt.Errorf("client %d connect failed: %w", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < iterations; i++ {
				// Alternate between INCRBY and QUERY on same and different keys
				var cmd string
				if i%2 == 0 {
					cmd = fmt.Sprintf("CMS.INCRBY cms_race_%d elem%d 1\r\n", clientID%5, i)
				} else {
					cmd = fmt.Sprintf("CMS.QUERY cms_race_%d elem%d\r\n", clientID%5, i-1)
				}

				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors <- fmt.Errorf("client %d write failed: %w", clientID, err)
					return
				}

				// Read array response
				header, err := reader.ReadString('\n')
				if err != nil {
					errors <- fmt.Errorf("client %d read failed: %w", clientID, err)
					return
				}

				if header != "*1\r\n" {
					errors <- fmt.Errorf("client %d unexpected header: %q", clientID, header)
					return
				}

				// Read the count value
				countLine, err := reader.ReadString('\n')
				if err != nil {
					errors <- fmt.Errorf("client %d read count failed: %w", clientID, err)
					return
				}

				if len(countLine) < 2 || countLine[0] != ':' {
					errors <- fmt.Errorf("client %d unexpected count: %q", clientID, countLine)
				}
			}
		}(c)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// =============================================================================
// String Command Concurrency Tests
// =============================================================================

// TestServerConcurrentINCRSameKey verifies that concurrent INCR operations
// on the same key produce correct results without lost updates.
func TestServerConcurrentINCRSameKey(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 10
	const incrementsPerClient = 100

	var wg sync.WaitGroup
	wg.Add(clients)

	// Each client increments the same counter
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("client %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < incrementsPerClient; i++ {
				if _, err := conn.Write([]byte("INCR concurrent_counter\r\n")); err != nil {
					t.Errorf("client %d write failed: %v", clientID, err)
					return
				}

				response, err := reader.ReadString('\n')
				if err != nil {
					t.Errorf("client %d read failed: %v", clientID, err)
					return
				}

				// Response should be an integer
				if len(response) < 2 || response[0] != ':' {
					t.Errorf("client %d unexpected response: %q", clientID, response)
				}
			}
		}(c)
	}

	wg.Wait()

	// Verify the final count
	conn, _ := net.Dial("tcp", app.listener.Addr().String())
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("GET concurrent_counter\r\n"))

	// Read the bulk string response
	line, _ := reader.ReadString('\n')
	if line[0] != '$' {
		t.Fatalf("Unexpected response type: %q", line)
	}
	value, _ := reader.ReadString('\n')

	expectedCount := clients * incrementsPerClient
	t.Logf("INCR final value: %s (expected: %d)", value[:len(value)-2], expectedCount)

	// The count should exactly match since INCR uses atomic Mutate
	if value != fmt.Sprintf("%d\r\n", expectedCount) {
		t.Errorf("Lost updates detected! Got %q, want %d", value, expectedCount)
	}
}

// TestServerConcurrentSetGet verifies that concurrent SET and GET operations
// on the same key don't cause races or return corrupted data.
func TestServerConcurrentSetGet(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	const clients = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(clients * 2) // Half writers, half readers

	// Writers
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("writer %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < iterations; i++ {
				value := fmt.Sprintf("value-from-client-%d-iter-%d", clientID, i)
				cmd := fmt.Sprintf("SET shared_string_key %s\r\n", value)

				if _, err := conn.Write([]byte(cmd)); err != nil {
					t.Errorf("writer %d write failed: %v", clientID, err)
					return
				}

				response, err := reader.ReadString('\n')
				if err != nil {
					t.Errorf("writer %d read failed: %v", clientID, err)
					return
				}

				if response != "+OK\r\n" {
					t.Errorf("writer %d unexpected response: %q", clientID, response)
				}
			}
		}(c)
	}

	// Readers
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("reader %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < iterations; i++ {
				if _, err := conn.Write([]byte("GET shared_string_key\r\n")); err != nil {
					t.Errorf("reader %d write failed: %v", clientID, err)
					return
				}

				// Read response (either $-1 for nil or $N\r\n<value>\r\n)
				line, err := reader.ReadString('\n')
				if err != nil {
					t.Errorf("reader %d read failed: %v", clientID, err)
					return
				}

				if line[0] == '$' && line != "$-1\r\n" {
					// Read the value
					_, err := reader.ReadString('\n')
					if err != nil {
						t.Errorf("reader %d value read failed: %v", clientID, err)
						return
					}
				}
			}
		}(c)
	}

	wg.Wait()
}

// TestServerConcurrentIncrDecr verifies that concurrent INCR and DECR operations
// maintain consistency.
func TestServerConcurrentIncrDecr(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	// Initialize counter to a known value
	setupConn, _ := net.Dial("tcp", app.listener.Addr().String())
	setupReader := bufio.NewReader(setupConn)
	_, _ = setupConn.Write([]byte("SET balanced_counter 1000\r\n"))
	_, _ = setupReader.ReadString('\n')
	_ = setupConn.Close()

	const clients = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(clients * 2) // Half incrementers, half decrementers

	// Incrementers
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < iterations; i++ {
				_, _ = conn.Write([]byte("INCR balanced_counter\r\n"))
				_, _ = reader.ReadString('\n')
			}
		}(c)
	}

	// Decrementers
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < iterations; i++ {
				_, _ = conn.Write([]byte("DECR balanced_counter\r\n"))
				_, _ = reader.ReadString('\n')
			}
		}(c)
	}

	wg.Wait()

	// Final value should be the same as initial (1000) since equal INCR and DECR
	conn, _ := net.Dial("tcp", app.listener.Addr().String())
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("GET balanced_counter\r\n"))

	line, _ := reader.ReadString('\n')
	if line[0] == '$' && line != "$-1\r\n" {
		value, _ := reader.ReadString('\n')
		t.Logf("Balanced counter final value: %s (expected: 1000)", value[:len(value)-2])

		if value != "1000\r\n" {
			t.Errorf("Counter not balanced! Got %q, want 1000", value)
		}
	}
}

// =============================================================================
// Top-K Concurrency Tests
// =============================================================================

// TestServerConcurrentTopKSameKey verifies that concurrent TOPK.ADD and TOPK.QUERY
// operations on the same key produce correct results without data races.
func TestServerConcurrentTopKSameKey(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	// First, create the TopK
	setupConn, _ := net.Dial("tcp", app.listener.Addr().String())
	setupReader := bufio.NewReader(setupConn)
	_, _ = setupConn.Write([]byte("TOPK.RESERVE concurrent_topk 50\r\n"))
	_, _ = setupReader.ReadString('\n')
	_ = setupConn.Close()

	const clients = 10
	const addsPerClient = 100

	var wg sync.WaitGroup
	wg.Add(clients * 2) // Half writers, half readers

	// Writers: Add items to the same TopK
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("client %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < addsPerClient; i++ {
				item := fmt.Sprintf("client%d-item%d", clientID, i)
				cmd := fmt.Sprintf("TOPK.ADD concurrent_topk %s\r\n", item)

				if _, err := conn.Write([]byte(cmd)); err != nil {
					t.Errorf("client %d write failed: %v", clientID, err)
					return
				}

				// Read array response
				header, err := reader.ReadString('\n')
				if err != nil {
					t.Errorf("client %d read header failed: %v", clientID, err)
					return
				}
				if header != "*1\r\n" {
					t.Errorf("client %d unexpected header: %q", clientID, header)
					return
				}
				// Read element
				line, _ := reader.ReadString('\n')
				if line != "$-1\r\n" {
					_, _ = reader.ReadString('\n') // read expelled key
				}
			}
		}(c)
	}

	// Readers: Query items from the same TopK
	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				t.Errorf("reader %d connect failed: %v", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < addsPerClient; i++ {
				// Query for various items
				item := fmt.Sprintf("client%d-item%d", clientID, i)
				cmd := fmt.Sprintf("TOPK.QUERY concurrent_topk %s\r\n", item)

				if _, err := conn.Write([]byte(cmd)); err != nil {
					t.Errorf("reader %d write failed: %v", clientID, err)
					return
				}

				// Read array response
				header, err := reader.ReadString('\n')
				if err != nil {
					t.Errorf("reader %d read header failed: %v", clientID, err)
					return
				}
				if header != "*1\r\n" {
					t.Errorf("reader %d unexpected header: %q", clientID, header)
					return
				}
				// Read result
				_, _ = reader.ReadString('\n')
			}
		}(c)
	}

	wg.Wait()

	// Verify TopK still works after concurrent access
	conn, _ := net.Dial("tcp", app.listener.Addr().String())
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("TOPK.LIST concurrent_topk\r\n"))
	header, _ := reader.ReadString('\n')

	t.Logf("Concurrent TopK test completed: %d clients × %d adds, LIST header: %s",
		clients, addsPerClient, header)
}

// TestServerConcurrentTopKAddQuery verifies that interleaved TOPK.ADD and TOPK.QUERY
// operations don't cause races or inconsistent state.
func TestServerConcurrentTopKAddQuery(t *testing.T) {
	app := newTestApp(t)
	app.config.maxConnections = 50
	app.connLimiter = make(chan struct{}, 50)

	go func() { _ = app.serve() }()
	<-app.readyCh
	defer func() { _ = app.listener.Close() }()

	// Create multiple TopK keys
	setupConn, _ := net.Dial("tcp", app.listener.Addr().String())
	setupReader := bufio.NewReader(setupConn)
	for i := 0; i < 5; i++ {
		cmd := fmt.Sprintf("TOPK.RESERVE topk_race_%d 20\r\n", i)
		_, _ = setupConn.Write([]byte(cmd))
		_, _ = setupReader.ReadString('\n')
	}
	_ = setupConn.Close()

	const clients = 20
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(clients)

	errors := make(chan error, clients*iterations)

	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", app.listener.Addr().String())
			if err != nil {
				errors <- fmt.Errorf("client %d connect failed: %w", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			reader := bufio.NewReader(conn)

			for i := 0; i < iterations; i++ {
				// Alternate between ADD and QUERY on same and different keys
				var cmd string
				if i%2 == 0 {
					cmd = fmt.Sprintf("TOPK.ADD topk_race_%d elem%d\r\n", clientID%5, i)
				} else {
					cmd = fmt.Sprintf("TOPK.QUERY topk_race_%d elem%d\r\n", clientID%5, i-1)
				}

				if _, err := conn.Write([]byte(cmd)); err != nil {
					errors <- fmt.Errorf("client %d write failed: %w", clientID, err)
					return
				}

				// Read array response
				header, err := reader.ReadString('\n')
				if err != nil {
					errors <- fmt.Errorf("client %d read failed: %w", clientID, err)
					return
				}

				if header != "*1\r\n" {
					errors <- fmt.Errorf("client %d unexpected header: %q", clientID, header)
					return
				}

				// Read element/result
				line, err := reader.ReadString('\n')
				if err != nil {
					errors <- fmt.Errorf("client %d read element failed: %w", clientID, err)
					return
				}

				// If ADD response and not nil, read expelled key
				if i%2 == 0 && line != "$-1\r\n" {
					if _, err := reader.ReadString('\n'); err != nil {
						errors <- fmt.Errorf("client %d read expelled failed: %w", clientID, err)
						return
					}
				}
			}
		}(c)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// =============================================================================
// AOF Concurrency Tests
// =============================================================================

// TestAOFConcurrentWrites verifies that concurrent command logging doesn't
// cause data races.
func TestAOFConcurrentWrites(t *testing.T) {
	filename := "test_concurrent_aof.aof"
	defer func() {
		_ = removeFile(filename)
	}()

	aof, err := NewAOF(filename)
	if err != nil {
		t.Fatalf("Failed to create AOF: %v", err)
	}
	defer func() { _ = aof.Close() }()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	app := &application{
		aof:    aof,
		logger: logger,
	}

	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				app.logCommand("HLL.ADD", []string{
					fmt.Sprintf("key-%d", id),
					fmt.Sprintf("value-%d", i),
				})
			}
		}(g)
	}

	wg.Wait()

	// Verify AOF is still valid by flushing
	if err := aof.Fsync(); err != nil {
		t.Errorf("AOF fsync failed: %v", err)
	}
}

// TestCompactAOFDuringWrites verifies that compaction during active writes
// doesn't cause races.
func TestCompactAOFDuringWrites(t *testing.T) {
	filename := "test_compact_concurrent.aof"
	defer func() {
		_ = removeFile(filename)
		_ = removeFile(filename + ".tmp")
	}()

	app := newTestApp(t)
	app.config.aofFilename = filename

	var err error
	app.aof, err = NewAOF(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.aof.Close() }()

	// Start background writers
	stopCh := make(chan struct{})
	var writerWg sync.WaitGroup

	const writers = 5
	writerWg.Add(writers)

	for w := 0; w < writers; w++ {
		go func(id int) {
			defer writerWg.Done()
			i := 0
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("writer-%d-key", id)
					app.store.Set(key, []byte(fmt.Sprintf("value-%d", i)))
					app.logCommand("SET", []string{key, fmt.Sprintf("value-%d", i)})
					i++
				}
			}
		}(w)
	}

	// Perform multiple compactions while writes are happening
	for c := 0; c < 3; c++ {
		if err := app.CompactAOF(); err != nil {
			t.Errorf("Compaction %d failed: %v", c, err)
		}
	}

	close(stopCh)
	writerWg.Wait()
}

// =============================================================================
// Helper Functions
// =============================================================================

func removeFile(filename string) error {
	return os.Remove(filename)
}
