package main

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// =============================================================================
// BF.ADD Tests
// =============================================================================

func TestBFAdd(t *testing.T) {
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
		resp := sendCommand("BF.ADD bf_test_1 element1")
		if resp != ":1\r\n" {
			t.Errorf("expected :1, got %q", resp)
		}
	})

	t.Run("add duplicate returns 0", func(t *testing.T) {
		// First add
		sendCommand("BF.ADD bf_test_2 duplicate_elem")
		// Second add of same element should return 0 (already present)
		resp := sendCommand("BF.ADD bf_test_2 duplicate_elem")
		if resp != ":0\r\n" {
			t.Errorf("expected :0 for duplicate, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("BF.ADD")
		if resp != "-ERR wrong number of arguments for 'BF.ADD' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("BF.ADD keyonly")
		if resp != "-ERR wrong number of arguments for 'BF.ADD' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// BF.EXISTS Tests
// =============================================================================

func TestBFExists(t *testing.T) {
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

	t.Run("element exists", func(t *testing.T) {
		sendCommand("BF.ADD bf_exists_test item1")
		resp := sendCommand("BF.EXISTS bf_exists_test item1")
		if resp != ":1\r\n" {
			t.Errorf("expected :1, got %q", resp)
		}
	})

	t.Run("element does not exist", func(t *testing.T) {
		sendCommand("BF.ADD bf_exists_test_2 item1")
		resp := sendCommand("BF.EXISTS bf_exists_test_2 nonexistent_item")
		if resp != ":0\r\n" {
			t.Errorf("expected :0 for non-existent item, got %q", resp)
		}
	})

	t.Run("non-existent key returns 0", func(t *testing.T) {
		resp := sendCommand("BF.EXISTS nonexistent_bf_key someitem")
		if resp != ":0\r\n" {
			t.Errorf("expected :0 for non-existent key, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("BF.EXISTS")
		if resp != "-ERR wrong number of arguments for 'BF.EXISTS' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("BF.EXISTS keyonly")
		if resp != "-ERR wrong number of arguments for 'BF.EXISTS' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// BF.MADD Tests
// =============================================================================

func TestBFMAdd(t *testing.T) {
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

	// Helper to send command and read first response line
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

	// Helper to read remaining lines of a RESP array
	readArrayBody := func(count int) string {
		var result strings.Builder
		for i := 0; i < count; i++ {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("failed to read array element %d: %v", i, err)
			}
			result.WriteString(line)
		}
		return result.String()
	}

	t.Run("basic madd multiple elements", func(t *testing.T) {
		header := sendCommand("BF.MADD bf_madd_1 elem1 elem2 elem3")
		if header != "*3\r\n" {
			t.Errorf("expected array header *3, got %q", header)
		}
		body := readArrayBody(3)
		expected := ":1\r\n:1\r\n:1\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}
	})

	t.Run("madd with duplicates in same call", func(t *testing.T) {
		header := sendCommand("BF.MADD bf_madd_2 dup dup dup")
		if header != "*3\r\n" {
			t.Errorf("expected array header *3, got %q", header)
		}
		body := readArrayBody(3)
		// First occurrence is 1, subsequent are 0
		expected := ":1\r\n:0\r\n:0\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}
	})

	t.Run("madd with previously existing elements", func(t *testing.T) {
		sendCommand("BF.ADD bf_madd_3 existing1")
		sendCommand("BF.ADD bf_madd_3 existing2")

		header := sendCommand("BF.MADD bf_madd_3 existing1 newelem existing2")
		if header != "*3\r\n" {
			t.Errorf("expected array header *3, got %q", header)
		}
		body := readArrayBody(3)
		// existing1=0, newelem=1, existing2=0
		expected := ":0\r\n:1\r\n:0\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}
	})

	t.Run("madd single element", func(t *testing.T) {
		header := sendCommand("BF.MADD bf_madd_4 singleelem")
		if header != "*1\r\n" {
			t.Errorf("expected array header *1, got %q", header)
		}
		body := readArrayBody(1)
		if body != ":1\r\n" {
			t.Errorf("expected :1, got %q", body)
		}
	})

	t.Run("madd creates new filter", func(t *testing.T) {
		header := sendCommand("BF.MADD bf_madd_new a b c d e")
		if header != "*5\r\n" {
			t.Errorf("expected array header *5, got %q", header)
		}
		body := readArrayBody(5)
		expected := ":1\r\n:1\r\n:1\r\n:1\r\n:1\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}

		// Verify with BF.EXISTS
		resp := sendCommand("BF.EXISTS bf_madd_new c")
		if resp != ":1\r\n" {
			t.Errorf("expected :1, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("BF.MADD")
		if resp != "-ERR wrong number of arguments for 'BF.MADD' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("BF.MADD keyonly")
		if resp != "-ERR wrong number of arguments for 'BF.MADD' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// BF.MEXISTS Tests
// =============================================================================

func TestBFMExists(t *testing.T) {
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

	// Helper to send command and read first response line
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

	// Helper to read remaining lines of a RESP array
	readArrayBody := func(count int) string {
		var result strings.Builder
		for i := 0; i < count; i++ {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("failed to read array element %d: %v", i, err)
			}
			result.WriteString(line)
		}
		return result.String()
	}

	t.Run("mexists mixed results", func(t *testing.T) {
		// Setup: Add "apple" and "banana"
		sendCommand("BF.MADD fruits apple banana")
		readArrayBody(2) // Consume MADD response

		// Check: apple(yes), cherry(no), banana(yes) -> [1, 0, 1]
		header := sendCommand("BF.MEXISTS fruits apple cherry banana")
		if header != "*3\r\n" {
			t.Errorf("expected *3 header, got %q", header)
		}
		body := readArrayBody(3)
		expected := ":1\r\n:0\r\n:1\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}
	})

	t.Run("mexists non-existent key", func(t *testing.T) {
		// Should return all zeros
		header := sendCommand("BF.MEXISTS ghost_key a b")
		if header != "*2\r\n" {
			t.Errorf("expected *2 header, got %q", header)
		}
		body := readArrayBody(2)
		expected := ":0\r\n:0\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}
	})

	t.Run("mexists single element", func(t *testing.T) {
		sendCommand("BF.ADD bf_mexists_single item1")
		header := sendCommand("BF.MEXISTS bf_mexists_single item1")
		if header != "*1\r\n" {
			t.Errorf("expected *1 header, got %q", header)
		}
		body := readArrayBody(1)
		if body != ":1\r\n" {
			t.Errorf("expected :1, got %q", body)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("BF.MEXISTS")
		if resp != "-ERR wrong number of arguments for 'BF.MEXISTS' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("BF.MEXISTS keyonly")
		if resp != "-ERR wrong number of arguments for 'BF.MEXISTS' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}
