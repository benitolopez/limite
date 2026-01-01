package main

import (
	"bufio"
	"net"
	"testing"
)

func TestHLLCountMultiKey(t *testing.T) {
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

	reader := bufio.NewReader(conn)

	// Helper to send command and get raw response string
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

	// 1. Setup Key A: {apple, banana}
	// HLL.ADD returns ":1" (registers changed)
	if resp := sendCommand("HLL.ADD key_a apple banana"); resp != ":1\r\n" {
		t.Fatalf("setup key_a failed, got %q", resp)
	}

	// 2. Setup Key B: {banana, cherry}
	// Overlapping 'banana'
	if resp := sendCommand("HLL.ADD key_b banana cherry"); resp != ":1\r\n" {
		t.Fatalf("setup key_b failed, got %q", resp)
	}

	// 3. Test HLL.COUNT with multiple keys
	// Union: {apple, banana, cherry} -> 3 unique items.
	// HLL is exact for very small sets, so we expect exactly 3.
	got := sendCommand("HLL.COUNT key_a key_b")
	expected := ":3\r\n"

	if got != expected {
		t.Errorf("HLL.COUNT key_a key_b: got %q, want %q", got, expected)
	}
}

// =============================================================================
// HLL.ADD Tests
// =============================================================================

func TestHLLAdd(t *testing.T) {
	app := newTestApp(t)

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

	t.Run("basic add single element", func(t *testing.T) {
		resp := sendCommand("HLL.ADD add_test_1 element1")
		if resp != ":1\r\n" {
			t.Errorf("expected :1, got %q", resp)
		}
	})

	t.Run("add multiple elements", func(t *testing.T) {
		resp := sendCommand("HLL.ADD add_test_2 elem1 elem2 elem3")
		if resp != ":1\r\n" {
			t.Errorf("expected :1, got %q", resp)
		}
	})

	t.Run("add duplicate returns 0", func(t *testing.T) {
		// First add
		sendCommand("HLL.ADD add_test_3 duplicate_elem")
		// Second add of same element should return 0 (no change)
		resp := sendCommand("HLL.ADD add_test_3 duplicate_elem")
		if resp != ":0\r\n" {
			t.Errorf("expected :0 for duplicate, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("HLL.ADD")
		if resp != "-ERR wrong number of arguments for 'HLL.ADD' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("HLL.ADD keyonly")
		if resp != "-ERR wrong number of arguments for 'HLL.ADD' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// HLL.COUNT Tests
// =============================================================================

func TestHLLCount(t *testing.T) {
	app := newTestApp(t)

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

	t.Run("single key count", func(t *testing.T) {
		sendCommand("HLL.ADD count_test_1 a b c")
		resp := sendCommand("HLL.COUNT count_test_1")
		if resp != ":3\r\n" {
			t.Errorf("expected :3, got %q", resp)
		}
	})

	t.Run("non-existent key returns 0", func(t *testing.T) {
		resp := sendCommand("HLL.COUNT nonexistent_key")
		if resp != ":0\r\n" {
			t.Errorf("expected :0 for non-existent key, got %q", resp)
		}
	})

	t.Run("multi-key with some missing", func(t *testing.T) {
		sendCommand("HLL.ADD count_test_2 x y z")
		// count_test_2 has 3 elements, missing_key treated as empty
		resp := sendCommand("HLL.COUNT count_test_2 missing_key_123")
		if resp != ":3\r\n" {
			t.Errorf("expected :3, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("HLL.COUNT")
		if resp != "-ERR wrong number of arguments for 'HLL.COUNT' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// HLL.MERGE Tests
// =============================================================================

func TestHLLMerge(t *testing.T) {
	app := newTestApp(t)

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

	t.Run("basic merge two sources into new destination", func(t *testing.T) {
		// Setup: src1 = {a, b}, src2 = {b, c}
		sendCommand("HLL.ADD merge_src1 a b")
		sendCommand("HLL.ADD merge_src2 b c")

		// Merge into new destination
		resp := sendCommand("HLL.MERGE merge_dest merge_src1 merge_src2")
		if resp != "+OK\r\n" {
			t.Errorf("expected +OK, got %q", resp)
		}

		// Verify: union = {a, b, c} = 3 elements
		resp = sendCommand("HLL.COUNT merge_dest")
		if resp != ":3\r\n" {
			t.Errorf("expected :3, got %q", resp)
		}
	})

	t.Run("merge into existing destination", func(t *testing.T) {
		// Setup: dest has {x}, src has {y, z}
		sendCommand("HLL.ADD merge_exist_dest x")
		sendCommand("HLL.ADD merge_exist_src y z")

		// Merge - should combine dest + src
		resp := sendCommand("HLL.MERGE merge_exist_dest merge_exist_src")
		if resp != "+OK\r\n" {
			t.Errorf("expected +OK, got %q", resp)
		}

		// Verify: union = {x, y, z} = 3 elements
		resp = sendCommand("HLL.COUNT merge_exist_dest")
		if resp != ":3\r\n" {
			t.Errorf("expected :3, got %q", resp)
		}
	})

	t.Run("merge with missing source keys", func(t *testing.T) {
		// Setup: only one source exists
		sendCommand("HLL.ADD merge_partial_src p q")

		// Merge with a non-existent source (treated as empty)
		resp := sendCommand("HLL.MERGE merge_partial_dest merge_partial_src nonexistent_src_key")
		if resp != "+OK\r\n" {
			t.Errorf("expected +OK, got %q", resp)
		}

		// Verify: only {p, q} from existing source
		resp = sendCommand("HLL.COUNT merge_partial_dest")
		if resp != ":2\r\n" {
			t.Errorf("expected :2, got %q", resp)
		}
	})

	t.Run("self-merge destination is also a source", func(t *testing.T) {
		// Setup: dest = {1, 2}, src = {2, 3}
		sendCommand("HLL.ADD self_merge_dest 1 2")
		sendCommand("HLL.ADD self_merge_src 2 3")

		// Merge where destination is also listed as a source
		resp := sendCommand("HLL.MERGE self_merge_dest self_merge_dest self_merge_src")
		if resp != "+OK\r\n" {
			t.Errorf("expected +OK, got %q", resp)
		}

		// Verify: union = {1, 2, 3} = 3 elements
		resp = sendCommand("HLL.COUNT self_merge_dest")
		if resp != ":3\r\n" {
			t.Errorf("expected :3, got %q", resp)
		}
	})

	t.Run("merge multiple sources", func(t *testing.T) {
		// Setup: 3 sources with overlapping elements
		sendCommand("HLL.ADD multi_src1 a b")
		sendCommand("HLL.ADD multi_src2 b c")
		sendCommand("HLL.ADD multi_src3 c d e")

		resp := sendCommand("HLL.MERGE multi_dest multi_src1 multi_src2 multi_src3")
		if resp != "+OK\r\n" {
			t.Errorf("expected +OK, got %q", resp)
		}

		// Verify: union = {a, b, c, d, e} = 5 elements
		resp = sendCommand("HLL.COUNT multi_dest")
		if resp != ":5\r\n" {
			t.Errorf("expected :5, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("HLL.MERGE")
		if resp != "-ERR wrong number of arguments for 'HLL.MERGE' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("HLL.MERGE destonly")
		if resp != "-ERR wrong number of arguments for 'HLL.MERGE' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}
