package main

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// =============================================================================
// TOPK.RESERVE Tests
// =============================================================================

func TestTopKReserve(t *testing.T) {
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

	t.Run("basic reserve", func(t *testing.T) {
		resp := sendCommand("TOPK.RESERVE topk_reserve_1 50")
		if resp != "+OK\r\n" {
			t.Errorf("expected +OK, got %q", resp)
		}
	})

	t.Run("reserve with options", func(t *testing.T) {
		resp := sendCommand("TOPK.RESERVE topk_reserve_2 50 2000 7 0.9")
		if resp != "+OK\r\n" {
			t.Errorf("expected +OK, got %q", resp)
		}
	})

	t.Run("reserve duplicate key fails", func(t *testing.T) {
		sendCommand("TOPK.RESERVE topk_reserve_3 50")
		resp := sendCommand("TOPK.RESERVE topk_reserve_3 50")
		if resp != "-ERR key already exists\r\n" {
			t.Errorf("expected key exists error, got %q", resp)
		}
	})

	t.Run("invalid k", func(t *testing.T) {
		resp := sendCommand("TOPK.RESERVE topk_reserve_4 0")
		if resp != "-ERR k must be a positive integer\r\n" {
			t.Errorf("expected invalid k error, got %q", resp)
		}

		resp = sendCommand("TOPK.RESERVE topk_reserve_4 notanumber")
		if resp != "-ERR k must be a positive integer\r\n" {
			t.Errorf("expected invalid k error, got %q", resp)
		}
	})

	t.Run("invalid decay", func(t *testing.T) {
		resp := sendCommand("TOPK.RESERVE topk_reserve_5 50 2048 5 1.5")
		if !strings.HasPrefix(resp, "-ERR decay must be between 0 and 1") {
			t.Errorf("expected invalid decay error, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("TOPK.RESERVE")
		if resp != "-ERR wrong number of arguments for 'TOPK.RESERVE' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("TOPK.RESERVE keyonly")
		if resp != "-ERR wrong number of arguments for 'TOPK.RESERVE' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		// Partial optional args
		resp = sendCommand("TOPK.RESERVE key 50 2048")
		if resp != "-ERR wrong number of arguments for 'TOPK.RESERVE' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// TOPK.ADD Tests
// =============================================================================

func TestTopKAdd(t *testing.T) {
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

	readBulkString := func() string {
		// Read $len\r\n or $-1\r\n
		header, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("failed to read bulk string header: %v", err)
		}

		if header == "$-1\r\n" {
			return "(nil)"
		}

		// Read the actual string value
		value, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("failed to read bulk string value: %v", err)
		}
		return strings.TrimSuffix(value, "\r\n")
	}

	t.Run("add auto-creates key", func(t *testing.T) {
		header := sendCommand("TOPK.ADD topk_add_1 item1")
		if header != "*1\r\n" {
			t.Errorf("expected *1 header, got %q", header)
		}

		// Should return nil (nothing expelled)
		val := readBulkString()
		if val != "(nil)" {
			t.Errorf("expected (nil), got %q", val)
		}
	})

	t.Run("add multiple items", func(t *testing.T) {
		header := sendCommand("TOPK.ADD topk_add_2 a b c")
		if header != "*3\r\n" {
			t.Errorf("expected *3 header, got %q", header)
		}

		// All should return nil (nothing expelled, heap not full)
		for i := 0; i < 3; i++ {
			val := readBulkString()
			if val != "(nil)" {
				t.Errorf("item %d: expected (nil), got %q", i, val)
			}
		}
	})

	t.Run("add existing item returns nil", func(t *testing.T) {
		sendCommand("TOPK.RESERVE topk_add_3 10")

		// Add item
		sendCommand("TOPK.ADD topk_add_3 existing")
		readBulkString()

		// Add same item again
		header := sendCommand("TOPK.ADD topk_add_3 existing")
		if header != "*1\r\n" {
			t.Errorf("expected *1 header, got %q", header)
		}

		val := readBulkString()
		if val != "(nil)" {
			t.Errorf("expected (nil) for existing item, got %q", val)
		}
	})

	t.Run("add wrong type", func(t *testing.T) {
		sendCommand("SET string_topk value")
		resp := sendCommand("TOPK.ADD string_topk item")
		if !strings.HasPrefix(resp, "-WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("TOPK.ADD")
		if resp != "-ERR wrong number of arguments for 'TOPK.ADD' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("TOPK.ADD keyonly")
		if resp != "-ERR wrong number of arguments for 'TOPK.ADD' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// TOPK.QUERY Tests
// =============================================================================

func TestTopKQuery(t *testing.T) {
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

	readBulkString := func() string {
		header, _ := reader.ReadString('\n')
		if header == "$-1\r\n" {
			return "(nil)"
		}
		value, _ := reader.ReadString('\n')
		return strings.TrimSuffix(value, "\r\n")
	}

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

	t.Run("query existing items", func(t *testing.T) {
		sendCommand("TOPK.RESERVE topk_query_1 10")
		sendCommand("TOPK.ADD topk_query_1 apple banana cherry")
		for i := 0; i < 3; i++ {
			readBulkString()
		}

		header := sendCommand("TOPK.QUERY topk_query_1 apple banana")
		if header != "*2\r\n" {
			t.Errorf("expected *2 header, got %q", header)
		}
		body := readArrayBody(2)
		// Both should be in TopK (return 1)
		if body != ":1\r\n:1\r\n" {
			t.Errorf("expected :1 for both, got %q", body)
		}
	})

	t.Run("query non-existent items returns zero", func(t *testing.T) {
		sendCommand("TOPK.RESERVE topk_query_2 10")
		sendCommand("TOPK.ADD topk_query_2 exists")
		readBulkString()

		header := sendCommand("TOPK.QUERY topk_query_2 exists missing")
		if header != "*2\r\n" {
			t.Errorf("expected *2 header, got %q", header)
		}
		body := readArrayBody(2)
		// exists=1, missing=0
		if body != ":1\r\n:0\r\n" {
			t.Errorf("expected :1 :0, got %q", body)
		}
	})

	t.Run("query non-existent key returns zeros", func(t *testing.T) {
		header := sendCommand("TOPK.QUERY ghost_topk item1 item2")
		if header != "*2\r\n" {
			t.Errorf("expected *2 header, got %q", header)
		}
		body := readArrayBody(2)
		if body != ":0\r\n:0\r\n" {
			t.Errorf("expected :0 :0, got %q", body)
		}
	})

	t.Run("query wrong type", func(t *testing.T) {
		sendCommand("SET string_query value")
		resp := sendCommand("TOPK.QUERY string_query item")
		if !strings.HasPrefix(resp, "-WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("TOPK.QUERY")
		if resp != "-ERR wrong number of arguments for 'TOPK.QUERY' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("TOPK.QUERY keyonly")
		if resp != "-ERR wrong number of arguments for 'TOPK.QUERY' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// TOPK.LIST Tests
// =============================================================================

func TestTopKList(t *testing.T) {
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

	readBulkString := func() string {
		header, _ := reader.ReadString('\n')
		if header == "$-1\r\n" {
			return "(nil)"
		}
		value, _ := reader.ReadString('\n')
		return strings.TrimSuffix(value, "\r\n")
	}

	t.Run("list empty topk", func(t *testing.T) {
		sendCommand("TOPK.RESERVE topk_list_1 10")
		resp := sendCommand("TOPK.LIST topk_list_1")
		if resp != "*0\r\n" {
			t.Errorf("expected *0 for empty list, got %q", resp)
		}
	})

	t.Run("list non-existent key", func(t *testing.T) {
		resp := sendCommand("TOPK.LIST ghost_list")
		if resp != "*0\r\n" {
			t.Errorf("expected *0 for missing key, got %q", resp)
		}
	})

	t.Run("list with items", func(t *testing.T) {
		sendCommand("TOPK.RESERVE topk_list_2 10")

		// Add items with different frequencies
		for i := 0; i < 100; i++ {
			sendCommand("TOPK.ADD topk_list_2 high")
			readBulkString()
		}
		for i := 0; i < 50; i++ {
			sendCommand("TOPK.ADD topk_list_2 medium")
			readBulkString()
		}
		for i := 0; i < 10; i++ {
			sendCommand("TOPK.ADD topk_list_2 low")
			readBulkString()
		}

		header := sendCommand("TOPK.LIST topk_list_2")
		if header != "*3\r\n" {
			t.Errorf("expected *3 header, got %q", header)
		}

		// Read 3 bulk strings (sorted descending)
		items := make([]string, 3)
		for i := 0; i < 3; i++ {
			items[i] = readBulkString()
		}

		// First should be "high"
		if items[0] != "high" {
			t.Errorf("expected first item to be 'high', got %q", items[0])
		}
	})

	t.Run("list withcount", func(t *testing.T) {
		sendCommand("TOPK.RESERVE topk_list_3 10")

		for i := 0; i < 20; i++ {
			sendCommand("TOPK.ADD topk_list_3 item")
			readBulkString()
		}

		header := sendCommand("TOPK.LIST topk_list_3 WITHCOUNT")
		if header != "*2\r\n" { // 1 item * 2 (key + count)
			t.Errorf("expected *2 header, got %q", header)
		}

		// Read key
		key := readBulkString()
		if key != "item" {
			t.Errorf("expected key 'item', got %q", key)
		}

		// Read count (integer format :N\r\n)
		countLine, _ := reader.ReadString('\n')
		if !strings.HasPrefix(countLine, ":") {
			t.Errorf("expected integer count, got %q", countLine)
		}
	})

	t.Run("list wrong type", func(t *testing.T) {
		sendCommand("SET string_list value")
		resp := sendCommand("TOPK.LIST string_list")
		if !strings.HasPrefix(resp, "-WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("TOPK.LIST")
		if resp != "-ERR wrong number of arguments for 'TOPK.LIST' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})

	t.Run("invalid option", func(t *testing.T) {
		sendCommand("TOPK.RESERVE topk_list_4 10")
		resp := sendCommand("TOPK.LIST topk_list_4 INVALID")
		if resp != "-ERR syntax error\r\n" {
			t.Errorf("expected syntax error, got %q", resp)
		}
	})
}

// =============================================================================
// TOPK Eviction Tests
// =============================================================================

func TestTopKEviction(t *testing.T) {
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

	readBulkString := func() string {
		header, _ := reader.ReadString('\n')
		if header == "$-1\r\n" {
			return "(nil)"
		}
		value, _ := reader.ReadString('\n')
		return strings.TrimSuffix(value, "\r\n")
	}

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

	t.Run("item fails to enter returns itself", func(t *testing.T) {
		// K=1 so only one item fits
		sendCommand("TOPK.RESERVE topk_evict_1 1")

		// Add "heavy" many times to build count
		for i := 0; i < 50; i++ {
			sendCommand("TOPK.ADD topk_evict_1 heavy")
			readBulkString()
		}

		// Add "light" once - should fail to enter and return itself
		header := sendCommand("TOPK.ADD topk_evict_1 light")
		if header != "*1\r\n" {
			t.Fatalf("expected *1 header, got %q", header)
		}

		expelled := readBulkString()
		if expelled != "light" {
			t.Errorf("expected 'light' to be expelled (failed to enter), got %q", expelled)
		}
	})

	t.Run("heavy item evicts light item", func(t *testing.T) {
		// K=1
		sendCommand("TOPK.RESERVE topk_evict_2 1")

		// Add "light" (count = 1)
		sendCommand("TOPK.ADD topk_evict_2 light")
		readBulkString()

		// Verify "light" is in TopK
		header := sendCommand("TOPK.QUERY topk_evict_2 light")
		if header != "*1\r\n" {
			t.Fatalf("expected *1 header, got %q", header)
		}
		body := readArrayBody(1)
		if body != ":1\r\n" {
			t.Errorf("expected 'light' in TopK initially, got %q", body)
		}

		// Add "heavy" many times until it evicts "light"
		var lightEvicted bool
		for i := 0; i < 100; i++ {
			sendCommand("TOPK.ADD topk_evict_2 heavy")
			expelled := readBulkString()
			if expelled == "light" {
				lightEvicted = true
				break
			}
		}

		if !lightEvicted {
			t.Error("expected 'light' to be evicted after adding 'heavy' many times")
		}

		// Verify "heavy" is now in TopK
		header = sendCommand("TOPK.QUERY topk_evict_2 heavy")
		if header != "*1\r\n" {
			t.Fatalf("expected *1 header, got %q", header)
		}
		body = readArrayBody(1)
		if body != ":1\r\n" {
			t.Errorf("expected 'heavy' in TopK, got %q", body)
		}
	})
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestTopKIntegration(t *testing.T) {
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

	readBulkString := func() string {
		header, _ := reader.ReadString('\n')
		if header == "$-1\r\n" {
			return "(nil)"
		}
		value, _ := reader.ReadString('\n')
		return strings.TrimSuffix(value, "\r\n")
	}

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

	t.Run("full workflow", func(t *testing.T) {
		// 1. Create TopK
		resp := sendCommand("TOPK.RESERVE trending 5")
		if resp != "+OK\r\n" {
			t.Fatalf("RESERVE failed: %q", resp)
		}

		// 2. Simulate user activity
		items := []string{"page_home", "page_about", "page_product", "page_home", "page_home"}
		for _, item := range items {
			sendCommand("TOPK.ADD trending " + item)
			readBulkString()
		}

		// 3. Query specific items
		header := sendCommand("TOPK.QUERY trending page_home page_about page_missing")
		if header != "*3\r\n" {
			t.Fatalf("QUERY header failed: %q", header)
		}
		body := readArrayBody(3)
		// page_home=1, page_about=1, page_missing=0
		if body != ":1\r\n:1\r\n:0\r\n" {
			t.Fatalf("QUERY body failed: %q", body)
		}

		// 4. List top items
		header = sendCommand("TOPK.LIST trending")
		if header != "*3\r\n" {
			t.Fatalf("LIST header failed: %q", header)
		}

		// First should be page_home (highest count)
		first := readBulkString()
		if first != "page_home" {
			t.Errorf("expected first item to be 'page_home', got %q", first)
		}

		// Consume rest
		readBulkString()
		readBulkString()
	})

	t.Run("auto-create workflow", func(t *testing.T) {
		// Use TOPK.ADD without RESERVE (auto-creates)
		header := sendCommand("TOPK.ADD autocreate_topk event1 event2 event3")
		if header != "*3\r\n" {
			t.Fatalf("ADD header failed: %q", header)
		}
		for i := 0; i < 3; i++ {
			val := readBulkString()
			if val != "(nil)" {
				t.Errorf("expected (nil) for auto-create add, got %q", val)
			}
		}

		// Verify items exist
		header = sendCommand("TOPK.QUERY autocreate_topk event1 event2 event3")
		if header != "*3\r\n" {
			t.Fatalf("QUERY header failed: %q", header)
		}
		body := readArrayBody(3)
		if body != ":1\r\n:1\r\n:1\r\n" {
			t.Fatalf("QUERY body failed: %q", body)
		}
	})
}
