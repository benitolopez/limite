package main

import (
	"bytes"
	"math"
	"strconv"
	"testing"
)

// TestSetGetBasic tests the basic SET and GET workflow.
func TestSetGetBasic(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// SET key value
	app.handleSet(&buf, []string{"mykey", "hello"})
	if buf.String() != "+OK\r\n" {
		t.Errorf("SET response: got %q, want %q", buf.String(), "+OK\r\n")
	}

	// Verify internal storage has DATA header
	stored, found := app.store.Get("mykey")
	if !found {
		t.Fatal("key not found in store after SET")
	}
	if string(stored[:4]) != "DATA" {
		t.Errorf("SET did not prepend DATA magic header, got %q", stored[:4])
	}
	if string(stored[4:]) != "hello" {
		t.Errorf("SET did not store correct payload, got %q", stored[4:])
	}

	// GET key (should strip header)
	buf.Reset()
	app.handleGet(&buf, []string{"mykey"})
	if buf.String() != "$5\r\nhello\r\n" {
		t.Errorf("GET response: got %q, want %q", buf.String(), "$5\r\nhello\r\n")
	}
}

// TestSetOverwrites tests that SET overwrites existing values.
func TestSetOverwrites(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleSet(&buf, []string{"key", "first"})
	buf.Reset()
	app.handleSet(&buf, []string{"key", "second"})
	if buf.String() != "+OK\r\n" {
		t.Errorf("SET response: got %q, want %q", buf.String(), "+OK\r\n")
	}

	buf.Reset()
	app.handleGet(&buf, []string{"key"})
	if buf.String() != "$6\r\nsecond\r\n" {
		t.Errorf("GET after overwrite: got %q, want %q", buf.String(), "$6\r\nsecond\r\n")
	}
}

// TestSetEmptyValue tests SET with an empty value.
func TestSetEmptyValue(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleSet(&buf, []string{"empty", ""})
	if buf.String() != "+OK\r\n" {
		t.Errorf("SET empty: got %q, want %q", buf.String(), "+OK\r\n")
	}

	buf.Reset()
	app.handleGet(&buf, []string{"empty"})
	// Empty bulk string: $0\r\n\r\n
	if buf.String() != "$0\r\n\r\n" {
		t.Errorf("GET empty: got %q, want %q", buf.String(), "$0\r\n\r\n")
	}
}

// TestGetNonExistent tests GET on a non-existent key.
func TestGetNonExistent(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleGet(&buf, []string{"nonexistent"})
	// Nil response: $-1\r\n
	if buf.String() != "$-1\r\n" {
		t.Errorf("GET nonexistent: got %q, want %q", buf.String(), "$-1\r\n")
	}
}

// TestGetWrongType tests GET on a key holding a different data type (e.g., HLL).
func TestGetWrongType(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Manually inject HLL-like data (with HYLL magic header)
	app.store.Set("hll_key", []byte{'H', 'Y', 'L', 'L', 0x01, 0x02, 0x03})

	app.handleGet(&buf, []string{"hll_key"})
	// Should return WRONGTYPE error
	if !bytes.HasPrefix(buf.Bytes(), []byte("-WRONGTYPE")) {
		t.Errorf("GET on wrong type: got %q, want WRONGTYPE error", buf.String())
	}
}

// TestIncrNewKey tests INCR on a new key (should become 1).
func TestIncrNewKey(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleIncr(&buf, []string{"counter"})
	if buf.String() != ":1\r\n" {
		t.Errorf("INCR new key: got %q, want %q", buf.String(), ":1\r\n")
	}

	// Verify stored value
	stored, _ := app.store.Get("counter")
	if string(stored) != "DATA1" {
		t.Errorf("INCR stored: got %q, want %q", string(stored), "DATA1")
	}
}

// TestIncrExistingKey tests INCR on an existing integer.
func TestIncrExistingKey(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set initial value
	app.handleSet(&buf, []string{"counter", "10"})
	buf.Reset()

	// INCR
	app.handleIncr(&buf, []string{"counter"})
	if buf.String() != ":11\r\n" {
		t.Errorf("INCR existing: got %q, want %q", buf.String(), ":11\r\n")
	}
}

// TestIncrMultiple tests multiple INCR operations.
func TestIncrMultiple(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	for i := 1; i <= 5; i++ {
		buf.Reset()
		app.handleIncr(&buf, []string{"counter"})
		expected := ":" + strconv.Itoa(i) + "\r\n"
		if buf.String() != expected {
			t.Errorf("INCR iteration %d: got %q, want %q", i, buf.String(), expected)
		}
	}
}

// TestIncrNonInteger tests INCR on a non-integer value.
func TestIncrNonInteger(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleSet(&buf, []string{"str", "hello"})
	buf.Reset()

	app.handleIncr(&buf, []string{"str"})
	if !bytes.HasPrefix(buf.Bytes(), []byte("-ERR value is not an integer")) {
		t.Errorf("INCR on string: got %q, want ERR response", buf.String())
	}
}

// TestIncrOverflow tests INCR overflow detection.
func TestIncrOverflow(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set to MaxInt64
	maxInt := strconv.FormatInt(math.MaxInt64, 10)
	app.handleSet(&buf, []string{"big", maxInt})
	buf.Reset()

	app.handleIncr(&buf, []string{"big"})
	if !bytes.HasPrefix(buf.Bytes(), []byte("-ERR increment or decrement would overflow")) {
		t.Errorf("INCR overflow: got %q, want overflow error", buf.String())
	}
}

// TestIncrWrongType tests INCR on a wrong type.
func TestIncrWrongType(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Manually inject HLL-like data
	app.store.Set("hll_key", []byte{'H', 'Y', 'L', 'L', 0x01})

	app.handleIncr(&buf, []string{"hll_key"})
	if !bytes.HasPrefix(buf.Bytes(), []byte("-WRONGTYPE")) {
		t.Errorf("INCR on wrong type: got %q, want WRONGTYPE error", buf.String())
	}
}

// TestDecrNewKey tests DECR on a new key (should become -1).
func TestDecrNewKey(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleDecr(&buf, []string{"counter"})
	if buf.String() != ":-1\r\n" {
		t.Errorf("DECR new key: got %q, want %q", buf.String(), ":-1\r\n")
	}
}

// TestDecrToNegative tests DECR resulting in negative numbers.
func TestDecrToNegative(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleSet(&buf, []string{"counter", "2"})
	buf.Reset()

	// DECR 3 times: 2 -> 1 -> 0 -> -1
	for i := 0; i < 3; i++ {
		buf.Reset()
		app.handleDecr(&buf, []string{"counter"})
	}

	if buf.String() != ":-1\r\n" {
		t.Errorf("DECR to negative: got %q, want %q", buf.String(), ":-1\r\n")
	}
}

// TestDecrUnderflow tests DECR underflow detection.
func TestDecrUnderflow(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set to MinInt64
	minInt := strconv.FormatInt(math.MinInt64, 10)
	app.handleSet(&buf, []string{"small", minInt})
	buf.Reset()

	app.handleDecr(&buf, []string{"small"})
	if !bytes.HasPrefix(buf.Bytes(), []byte("-ERR increment or decrement would overflow")) {
		t.Errorf("DECR underflow: got %q, want overflow error", buf.String())
	}
}

// TestIncrByPositive tests INCRBY with a positive value.
func TestIncrByPositive(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleSet(&buf, []string{"counter", "10"})
	buf.Reset()

	app.handleIncrBy(&buf, []string{"counter", "5"})
	if buf.String() != ":15\r\n" {
		t.Errorf("INCRBY positive: got %q, want %q", buf.String(), ":15\r\n")
	}
}

// TestIncrByNegative tests INCRBY with a negative value.
func TestIncrByNegative(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleSet(&buf, []string{"counter", "10"})
	buf.Reset()

	app.handleIncrBy(&buf, []string{"counter", "-3"})
	if buf.String() != ":7\r\n" {
		t.Errorf("INCRBY negative: got %q, want %q", buf.String(), ":7\r\n")
	}
}

// TestIncrByNewKey tests INCRBY on a new key.
func TestIncrByNewKey(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleIncrBy(&buf, []string{"counter", "100"})
	if buf.String() != ":100\r\n" {
		t.Errorf("INCRBY new key: got %q, want %q", buf.String(), ":100\r\n")
	}
}

// TestIncrByInvalidIncrement tests INCRBY with an invalid increment.
func TestIncrByInvalidIncrement(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleIncrBy(&buf, []string{"counter", "notanumber"})
	if !bytes.HasPrefix(buf.Bytes(), []byte("-ERR value is not an integer")) {
		t.Errorf("INCRBY invalid: got %q, want ERR response", buf.String())
	}
}

// TestDecrByPositive tests DECRBY with a positive value.
func TestDecrByPositive(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleSet(&buf, []string{"counter", "10"})
	buf.Reset()

	app.handleDecrBy(&buf, []string{"counter", "3"})
	if buf.String() != ":7\r\n" {
		t.Errorf("DECRBY positive: got %q, want %q", buf.String(), ":7\r\n")
	}
}

// TestDecrByNegative tests DECRBY with a negative value (effectively adding).
func TestDecrByNegative(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleSet(&buf, []string{"counter", "10"})
	buf.Reset()

	// DECRBY -5 is equivalent to INCRBY 5
	app.handleDecrBy(&buf, []string{"counter", "-5"})
	if buf.String() != ":15\r\n" {
		t.Errorf("DECRBY negative: got %q, want %q", buf.String(), ":15\r\n")
	}
}

// TestWrongNumberOfArgs tests error handling for wrong number of arguments.
func TestWrongNumberOfArgs(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	tests := []struct {
		name    string
		handler func(buf *bytes.Buffer)
		cmd     string
	}{
		{"SET no args", func(buf *bytes.Buffer) { app.handleSet(buf, []string{}) }, "SET"},
		{"SET one arg", func(buf *bytes.Buffer) { app.handleSet(buf, []string{"key"}) }, "SET"},
		{"SET three args", func(buf *bytes.Buffer) { app.handleSet(buf, []string{"a", "b", "c"}) }, "SET"},
		{"GET no args", func(buf *bytes.Buffer) { app.handleGet(buf, []string{}) }, "GET"},
		{"GET two args", func(buf *bytes.Buffer) { app.handleGet(buf, []string{"a", "b"}) }, "GET"},
		{"INCR no args", func(buf *bytes.Buffer) { app.handleIncr(buf, []string{}) }, "INCR"},
		{"INCR two args", func(buf *bytes.Buffer) { app.handleIncr(buf, []string{"a", "b"}) }, "INCR"},
		{"DECR no args", func(buf *bytes.Buffer) { app.handleDecr(buf, []string{}) }, "DECR"},
		{"INCRBY one arg", func(buf *bytes.Buffer) { app.handleIncrBy(buf, []string{"key"}) }, "INCRBY"},
		{"DECRBY no args", func(buf *bytes.Buffer) { app.handleDecrBy(buf, []string{}) }, "DECRBY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			tt.handler(&buf)
			if !bytes.HasPrefix(buf.Bytes(), []byte("-ERR wrong number of arguments")) {
				t.Errorf("%s: got %q, want wrong number of arguments error", tt.name, buf.String())
			}
		})
	}
}

// TestSetOverwritesWrongType tests that SET overwrites a key of a different type.
func TestSetOverwritesWrongType(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Manually inject HLL-like data
	app.store.Set("typed_key", []byte{'H', 'Y', 'L', 'L', 0x01})

	// SET should overwrite it
	app.handleSet(&buf, []string{"typed_key", "string_value"})
	if buf.String() != "+OK\r\n" {
		t.Errorf("SET over wrong type: got %q, want %q", buf.String(), "+OK\r\n")
	}

	// GET should now work
	buf.Reset()
	app.handleGet(&buf, []string{"typed_key"})
	if buf.String() != "$12\r\nstring_value\r\n" {
		t.Errorf("GET after overwrite: got %q, want %q", buf.String(), "$12\r\nstring_value\r\n")
	}
}
