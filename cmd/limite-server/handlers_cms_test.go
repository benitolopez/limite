package main

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// =============================================================================
// CMS.INIT Tests
// =============================================================================

func TestCMSInit(t *testing.T) {
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

	t.Run("basic init", func(t *testing.T) {
		resp := sendCommand("CMS.INIT cms_init_1 1000 5")
		if resp != "+OK\r\n" {
			t.Errorf("expected +OK, got %q", resp)
		}
	})

	t.Run("init duplicate key fails", func(t *testing.T) {
		sendCommand("CMS.INIT cms_init_2 1000 5")
		resp := sendCommand("CMS.INIT cms_init_2 1000 5")
		if resp != "-ERR key already exists\r\n" {
			t.Errorf("expected key exists error, got %q", resp)
		}
	})

	t.Run("invalid width", func(t *testing.T) {
		resp := sendCommand("CMS.INIT cms_init_3 0 5")
		if resp != "-ERR invalid width\r\n" {
			t.Errorf("expected invalid width error, got %q", resp)
		}

		resp = sendCommand("CMS.INIT cms_init_3 notanumber 5")
		if resp != "-ERR invalid width\r\n" {
			t.Errorf("expected invalid width error, got %q", resp)
		}
	})

	t.Run("invalid depth", func(t *testing.T) {
		resp := sendCommand("CMS.INIT cms_init_4 1000 0")
		if resp != "-ERR invalid depth\r\n" {
			t.Errorf("expected invalid depth error, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("CMS.INIT")
		if resp != "-ERR wrong number of arguments for 'CMS.INIT' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("CMS.INIT keyonly")
		if resp != "-ERR wrong number of arguments for 'CMS.INIT' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("CMS.INIT key 100")
		if resp != "-ERR wrong number of arguments for 'CMS.INIT' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// CMS.INITBYPROB Tests
// =============================================================================

func TestCMSInitByProb(t *testing.T) {
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

	t.Run("basic initbyprob", func(t *testing.T) {
		resp := sendCommand("CMS.INITBYPROB cms_prob_1 0.01 0.01")
		if resp != "+OK\r\n" {
			t.Errorf("expected +OK, got %q", resp)
		}
	})

	t.Run("initbyprob duplicate key fails", func(t *testing.T) {
		sendCommand("CMS.INITBYPROB cms_prob_2 0.01 0.01")
		resp := sendCommand("CMS.INITBYPROB cms_prob_2 0.01 0.01")
		if resp != "-ERR key already exists\r\n" {
			t.Errorf("expected key exists error, got %q", resp)
		}
	})

	t.Run("invalid epsilon", func(t *testing.T) {
		resp := sendCommand("CMS.INITBYPROB cms_prob_3 0 0.01")
		if !strings.HasPrefix(resp, "-ERR invalid epsilon") {
			t.Errorf("expected invalid epsilon error, got %q", resp)
		}

		resp = sendCommand("CMS.INITBYPROB cms_prob_3 1.5 0.01")
		if !strings.HasPrefix(resp, "-ERR invalid epsilon") {
			t.Errorf("expected invalid epsilon error, got %q", resp)
		}
	})

	t.Run("invalid delta", func(t *testing.T) {
		resp := sendCommand("CMS.INITBYPROB cms_prob_4 0.01 0")
		if !strings.HasPrefix(resp, "-ERR invalid delta") {
			t.Errorf("expected invalid delta error, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("CMS.INITBYPROB")
		if resp != "-ERR wrong number of arguments for 'CMS.INITBYPROB' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// CMS.INCRBY Tests
// =============================================================================

func TestCMSIncrBy(t *testing.T) {
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

	t.Run("basic incrby single item", func(t *testing.T) {
		sendCommand("CMS.INIT cms_incr_1 1000 5")

		header := sendCommand("CMS.INCRBY cms_incr_1 item1 10")
		if header != "*1\r\n" {
			t.Errorf("expected *1 header, got %q", header)
		}
		body := readArrayBody(1)
		if body != ":10\r\n" {
			t.Errorf("expected :10, got %q", body)
		}
	})

	t.Run("incrby multiple items", func(t *testing.T) {
		sendCommand("CMS.INIT cms_incr_2 1000 5")

		header := sendCommand("CMS.INCRBY cms_incr_2 apple 5 banana 10 cherry 3")
		if header != "*3\r\n" {
			t.Errorf("expected *3 header, got %q", header)
		}
		body := readArrayBody(3)
		expected := ":5\r\n:10\r\n:3\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}
	})

	t.Run("incrby accumulates", func(t *testing.T) {
		sendCommand("CMS.INIT cms_incr_3 1000 5")

		// First increment
		sendCommand("CMS.INCRBY cms_incr_3 item 5")
		readArrayBody(1)

		// Second increment
		header := sendCommand("CMS.INCRBY cms_incr_3 item 3")
		if header != "*1\r\n" {
			t.Errorf("expected *1 header, got %q", header)
		}
		body := readArrayBody(1)
		if body != ":8\r\n" {
			t.Errorf("expected :8 after accumulation, got %q", body)
		}
	})

	t.Run("incrby key not found", func(t *testing.T) {
		resp := sendCommand("CMS.INCRBY nonexistent_cms item 5")
		if resp != "-ERR key not found\r\n" {
			t.Errorf("expected key not found error, got %q", resp)
		}
	})

	t.Run("incrby invalid increment", func(t *testing.T) {
		sendCommand("CMS.INIT cms_incr_4 1000 5")
		resp := sendCommand("CMS.INCRBY cms_incr_4 item notanumber")
		if resp != "-ERR invalid increment value\r\n" {
			t.Errorf("expected invalid increment error, got %q", resp)
		}
	})

	t.Run("incrby wrong type", func(t *testing.T) {
		// Create a string key
		sendCommand("SET string_key value")
		resp := sendCommand("CMS.INCRBY string_key item 5")
		if !strings.HasPrefix(resp, "-WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("CMS.INCRBY")
		if resp != "-ERR wrong number of arguments for 'CMS.INCRBY' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("CMS.INCRBY keyonly")
		if resp != "-ERR wrong number of arguments for 'CMS.INCRBY' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		// Odd number of item/increment pairs
		sendCommand("CMS.INIT cms_incr_args 1000 5")
		resp = sendCommand("CMS.INCRBY cms_incr_args item")
		if resp != "-ERR wrong number of arguments for 'CMS.INCRBY' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// CMS.QUERY Tests
// =============================================================================

func TestCMSQuery(t *testing.T) {
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
		sendCommand("CMS.INIT cms_query_1 1000 5")
		sendCommand("CMS.INCRBY cms_query_1 apple 5 banana 10")
		readArrayBody(2)

		header := sendCommand("CMS.QUERY cms_query_1 apple banana")
		if header != "*2\r\n" {
			t.Errorf("expected *2 header, got %q", header)
		}
		body := readArrayBody(2)
		expected := ":5\r\n:10\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}
	})

	t.Run("query non-existent items returns zero", func(t *testing.T) {
		sendCommand("CMS.INIT cms_query_2 1000 5")
		sendCommand("CMS.INCRBY cms_query_2 apple 5")
		readArrayBody(1)

		header := sendCommand("CMS.QUERY cms_query_2 apple nonexistent banana")
		if header != "*3\r\n" {
			t.Errorf("expected *3 header, got %q", header)
		}
		body := readArrayBody(3)
		// apple=5, nonexistent=0, banana=0
		expected := ":5\r\n:0\r\n:0\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}
	})

	t.Run("query non-existent key returns zeros", func(t *testing.T) {
		header := sendCommand("CMS.QUERY ghost_cms item1 item2")
		if header != "*2\r\n" {
			t.Errorf("expected *2 header, got %q", header)
		}
		body := readArrayBody(2)
		expected := ":0\r\n:0\r\n"
		if body != expected {
			t.Errorf("expected %q, got %q", expected, body)
		}
	})

	t.Run("query wrong type", func(t *testing.T) {
		sendCommand("SET string_key2 value")
		resp := sendCommand("CMS.QUERY string_key2 item")
		if !strings.HasPrefix(resp, "-WRONGTYPE") {
			t.Errorf("expected WRONGTYPE error, got %q", resp)
		}
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		resp := sendCommand("CMS.QUERY")
		if resp != "-ERR wrong number of arguments for 'CMS.QUERY' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}

		resp = sendCommand("CMS.QUERY keyonly")
		if resp != "-ERR wrong number of arguments for 'CMS.QUERY' command\r\n" {
			t.Errorf("expected wrong args error, got %q", resp)
		}
	})
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestCMSIntegration(t *testing.T) {
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
		// 1. Create CMS
		resp := sendCommand("CMS.INIT myfreq 1000 5")
		if resp != "+OK\r\n" {
			t.Fatalf("INIT failed: %q", resp)
		}

		// 2. Add some items with counts
		header := sendCommand("CMS.INCRBY myfreq pageA 100 pageB 50 pageC 25")
		if header != "*3\r\n" {
			t.Fatalf("INCRBY header failed: %q", header)
		}
		body := readArrayBody(3)
		if body != ":100\r\n:50\r\n:25\r\n" {
			t.Fatalf("INCRBY body failed: %q", body)
		}

		// 3. Increment again
		header = sendCommand("CMS.INCRBY myfreq pageA 50 pageB 25")
		if header != "*2\r\n" {
			t.Fatalf("INCRBY2 header failed: %q", header)
		}
		body = readArrayBody(2)
		if body != ":150\r\n:75\r\n" {
			t.Fatalf("INCRBY2 body failed: %q", body)
		}

		// 4. Query final counts
		header = sendCommand("CMS.QUERY myfreq pageA pageB pageC pageD")
		if header != "*4\r\n" {
			t.Fatalf("QUERY header failed: %q", header)
		}
		body = readArrayBody(4)
		// pageA=150, pageB=75, pageC=25, pageD=0
		if body != ":150\r\n:75\r\n:25\r\n:0\r\n" {
			t.Fatalf("QUERY body failed: %q", body)
		}
	})

	t.Run("initbyprob then use", func(t *testing.T) {
		// Create with probability parameters
		resp := sendCommand("CMS.INITBYPROB freq2 0.001 0.01")
		if resp != "+OK\r\n" {
			t.Fatalf("INITBYPROB failed: %q", resp)
		}

		// Use it
		header := sendCommand("CMS.INCRBY freq2 event 1000")
		if header != "*1\r\n" {
			t.Fatalf("INCRBY header failed: %q", header)
		}
		body := readArrayBody(1)
		if body != ":1000\r\n" {
			t.Fatalf("INCRBY body failed: %q", body)
		}

		// Query
		header = sendCommand("CMS.QUERY freq2 event")
		if header != "*1\r\n" {
			t.Fatalf("QUERY header failed: %q", header)
		}
		body = readArrayBody(1)
		if body != ":1000\r\n" {
			t.Fatalf("QUERY body failed: %q", body)
		}
	})
}
