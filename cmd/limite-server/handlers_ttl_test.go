package main

import (
	"bufio"
	"bytes"
	"strconv"
	"testing"
	"time"
)

// TestExpireBasic tests the basic EXPIRE and TTL workflow.
func TestExpireBasic(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()

	// Set expiry to 5000ms from now
	app.handleExpire(&buf, []string{"mykey", "5000"})
	if buf.String() != ":1\r\n" {
		t.Errorf("EXPIRE response: got %q, want %q", buf.String(), ":1\r\n")
	}

	// Check TTL (should be around 5000ms, allow some margin)
	buf.Reset()
	app.handleTTL(&buf, []string{"mykey"})
	resp := buf.String()
	// Parse the integer response
	if len(resp) < 3 || resp[0] != ':' {
		t.Fatalf("TTL response format invalid: %q", resp)
	}
	ttl, err := strconv.ParseInt(resp[1:len(resp)-2], 10, 64)
	if err != nil {
		t.Fatalf("TTL response parse error: %v", err)
	}
	if ttl < 4900 || ttl > 5100 {
		t.Errorf("TTL response: got %d, want ~5000", ttl)
	}
}

// TestExpireNonExistent tests EXPIRE on a non-existent key.
func TestExpireNonExistent(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleExpire(&buf, []string{"nonexistent", "1000"})
	if buf.String() != ":0\r\n" {
		t.Errorf("EXPIRE nonexistent: got %q, want %q", buf.String(), ":0\r\n")
	}
}

// TestExpireNegative tests EXPIRE with negative TTL (should delete key).
func TestExpireNegative(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()

	// Set negative expiry (should delete immediately)
	app.handleExpire(&buf, []string{"mykey", "-1000"})
	if buf.String() != ":1\r\n" {
		t.Errorf("EXPIRE negative response: got %q, want %q", buf.String(), ":1\r\n")
	}

	// Key should no longer exist
	buf.Reset()
	app.handleGet(&buf, []string{"mykey"})
	if buf.String() != "$-1\r\n" {
		t.Errorf("GET after negative EXPIRE: got %q, want %q", buf.String(), "$-1\r\n")
	}
}

// TestExpireZero tests EXPIRE with zero TTL (should delete key).
func TestExpireZero(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()

	// Set zero expiry (should delete immediately)
	app.handleExpire(&buf, []string{"mykey", "0"})
	if buf.String() != ":1\r\n" {
		t.Errorf("EXPIRE zero response: got %q, want %q", buf.String(), ":1\r\n")
	}

	// Key should no longer exist
	buf.Reset()
	app.handleGet(&buf, []string{"mykey"})
	if buf.String() != "$-1\r\n" {
		t.Errorf("GET after zero EXPIRE: got %q, want %q", buf.String(), "$-1\r\n")
	}
}

// TestExpireAtBasic tests the EXPIREAT command.
func TestExpireAtBasic(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()

	// Set expiry to 5 seconds from now
	futureMs := time.Now().UnixMilli() + 5000
	app.handleExpireAt(&buf, []string{"mykey", strconv.FormatInt(futureMs, 10)})
	if buf.String() != ":1\r\n" {
		t.Errorf("EXPIREAT response: got %q, want %q", buf.String(), ":1\r\n")
	}

	// Check TTL (should be around 5000ms)
	buf.Reset()
	app.handleTTL(&buf, []string{"mykey"})
	resp := buf.String()
	if len(resp) < 3 || resp[0] != ':' {
		t.Fatalf("TTL response format invalid: %q", resp)
	}
	ttl, _ := strconv.ParseInt(resp[1:len(resp)-2], 10, 64)
	if ttl < 4900 || ttl > 5100 {
		t.Errorf("TTL response: got %d, want ~5000", ttl)
	}
}

// TestExpireAtPast tests EXPIREAT with a timestamp in the past (should delete key).
func TestExpireAtPast(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()

	// Set expiry to 1 second ago
	pastMs := time.Now().UnixMilli() - 1000
	app.handleExpireAt(&buf, []string{"mykey", strconv.FormatInt(pastMs, 10)})
	if buf.String() != ":1\r\n" {
		t.Errorf("EXPIREAT past response: got %q, want %q", buf.String(), ":1\r\n")
	}

	// Key should no longer exist
	buf.Reset()
	app.handleGet(&buf, []string{"mykey"})
	if buf.String() != "$-1\r\n" {
		t.Errorf("GET after EXPIREAT past: got %q, want %q", buf.String(), "$-1\r\n")
	}
}

// TestTTLNonExistent tests TTL on a non-existent key.
func TestTTLNonExistent(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleTTL(&buf, []string{"nonexistent"})
	if buf.String() != ":-2\r\n" {
		t.Errorf("TTL nonexistent: got %q, want %q", buf.String(), ":-2\r\n")
	}
}

// TestTTLNoExpiry tests TTL on a key without expiry.
func TestTTLNoExpiry(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key without expiry
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()

	app.handleTTL(&buf, []string{"mykey"})
	if buf.String() != ":-1\r\n" {
		t.Errorf("TTL no expiry: got %q, want %q", buf.String(), ":-1\r\n")
	}
}

// TestPersistBasic tests the PERSIST command.
func TestPersistBasic(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key with expiry
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()
	app.handleExpire(&buf, []string{"mykey", "5000"})
	buf.Reset()

	// Verify it has TTL
	app.handleTTL(&buf, []string{"mykey"})
	resp := buf.String()
	if resp == ":-1\r\n" {
		t.Errorf("Key should have TTL before PERSIST")
	}
	buf.Reset()

	// Remove TTL
	app.handlePersist(&buf, []string{"mykey"})
	if buf.String() != ":1\r\n" {
		t.Errorf("PERSIST response: got %q, want %q", buf.String(), ":1\r\n")
	}

	// Verify TTL is now -1 (no expiry)
	buf.Reset()
	app.handleTTL(&buf, []string{"mykey"})
	if buf.String() != ":-1\r\n" {
		t.Errorf("TTL after PERSIST: got %q, want %q", buf.String(), ":-1\r\n")
	}
}

// TestPersistNonExistent tests PERSIST on a non-existent key.
func TestPersistNonExistent(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handlePersist(&buf, []string{"nonexistent"})
	if buf.String() != ":0\r\n" {
		t.Errorf("PERSIST nonexistent: got %q, want %q", buf.String(), ":0\r\n")
	}
}

// TestPersistNoExpiry tests PERSIST on a key that already has no expiry.
func TestPersistNoExpiry(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key without expiry
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()

	// PERSIST should return 0 (no TTL to remove)
	app.handlePersist(&buf, []string{"mykey"})
	if buf.String() != ":0\r\n" {
		t.Errorf("PERSIST no expiry: got %q, want %q", buf.String(), ":0\r\n")
	}
}

// TestSetClearsExpiry tests that SET clears any existing TTL.
func TestSetClearsExpiry(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key with expiry
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()
	app.handleExpire(&buf, []string{"mykey", "5000"})
	buf.Reset()

	// Verify it has TTL
	app.handleTTL(&buf, []string{"mykey"})
	if buf.String() == ":-1\r\n" {
		t.Errorf("Key should have TTL after EXPIRE")
	}
	buf.Reset()

	// SET again (should clear TTL)
	app.handleSet(&buf, []string{"mykey", "world"})
	buf.Reset()

	// Verify TTL is now -1 (no expiry)
	app.handleTTL(&buf, []string{"mykey"})
	if buf.String() != ":-1\r\n" {
		t.Errorf("TTL after SET: got %q, want %q", buf.String(), ":-1\r\n")
	}
}

// TestIncrPreservesExpiry tests that INCR does NOT clear existing TTL.
func TestIncrPreservesExpiry(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a numeric key with expiry
	app.handleSet(&buf, []string{"counter", "100"})
	buf.Reset()
	app.handleExpire(&buf, []string{"counter", "5000"})
	buf.Reset()

	// Verify it has TTL
	app.handleTTL(&buf, []string{"counter"})
	if buf.String() == ":-1\r\n" {
		t.Errorf("Key should have TTL after EXPIRE")
	}
	buf.Reset()

	// INCR the key (should preserve TTL)
	app.handleIncr(&buf, []string{"counter"})
	if buf.String() != ":101\r\n" {
		t.Errorf("INCR response: got %q, want %q", buf.String(), ":101\r\n")
	}
	buf.Reset()

	// Verify TTL is still set (not -1)
	app.handleTTL(&buf, []string{"counter"})
	resp := buf.String()
	if resp == ":-1\r\n" {
		t.Errorf("TTL after INCR should still be set, got -1 (no expiry)")
	}
	// TTL should be around 5000ms (allow some margin)
	if len(resp) >= 3 && resp[0] == ':' {
		ttl, err := strconv.ParseInt(resp[1:len(resp)-2], 10, 64)
		if err == nil && (ttl < 4800 || ttl > 5100) {
			t.Errorf("TTL after INCR: got %d, want ~5000", ttl)
		}
	}
}

// TestLazyExpiration tests that expired keys are not returned by GET.
func TestLazyExpiration(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key with very short expiry
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()
	app.handleExpire(&buf, []string{"mykey", "50"}) // 50ms
	buf.Reset()

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// GET should return nil (key expired)
	app.handleGet(&buf, []string{"mykey"})
	if buf.String() != "$-1\r\n" {
		t.Errorf("GET expired key: got %q, want %q", buf.String(), "$-1\r\n")
	}
}

// TestExpireUpdatesTTL tests that EXPIRE updates an existing TTL.
func TestExpireUpdatesTTL(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set a key with initial expiry
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()
	app.handleExpire(&buf, []string{"mykey", "10000"})
	buf.Reset()

	// Update to shorter expiry
	app.handleExpire(&buf, []string{"mykey", "2000"})
	if buf.String() != ":1\r\n" {
		t.Errorf("EXPIRE update response: got %q, want %q", buf.String(), ":1\r\n")
	}

	// Check TTL (should be around 2000ms now)
	buf.Reset()
	app.handleTTL(&buf, []string{"mykey"})
	resp := buf.String()
	if len(resp) < 3 || resp[0] != ':' {
		t.Fatalf("TTL response format invalid: %q", resp)
	}
	ttl, _ := strconv.ParseInt(resp[1:len(resp)-2], 10, 64)
	if ttl < 1900 || ttl > 2100 {
		t.Errorf("TTL after update: got %d, want ~2000", ttl)
	}
}

// TestExpireInvalidArgs tests EXPIRE with invalid arguments.
func TestExpireInvalidArgs(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Wrong number of args
	app.handleExpire(&buf, []string{"key"})
	if !bytes.HasPrefix(buf.Bytes(), []byte("-ERR wrong number of arguments")) {
		t.Errorf("EXPIRE wrong args: got %q", buf.String())
	}

	buf.Reset()
	app.handleExpire(&buf, []string{"key", "notanumber"})
	if !bytes.HasPrefix(buf.Bytes(), []byte("-ERR value is not an integer")) {
		t.Errorf("EXPIRE invalid number: got %q", buf.String())
	}
}

// TestActiveExpiration tests the background expiration cleanup.
func TestActiveExpiration(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Set multiple keys with short expiry
	for i := 0; i < 50; i++ {
		key := "key" + strconv.Itoa(i)
		app.handleSet(&buf, []string{key, "value"})
		buf.Reset()
		app.handleExpire(&buf, []string{key, "50"}) // 50ms
		buf.Reset()
	}

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// Run active expiration
	deleted := app.store.DeleteExpiredKeys()

	// Should have deleted all 50 keys
	if deleted != 50 {
		t.Errorf("Active expiration deleted %d keys, want 50", deleted)
	}
}

// TestDeleteExpiredKeys tests the store's DeleteExpiredKeys method.
func TestDeleteExpiredKeys(t *testing.T) {
	store := NewStore()

	// Set some keys with expiry in the past
	past := time.Now().UnixMilli() - 1000

	for i := 0; i < 100; i++ {
		key := "key" + strconv.Itoa(i)
		store.Set(key, []byte("value"))
		store.SetExpiry(key, past)
	}

	// Run cleanup
	deleted := store.DeleteExpiredKeys()

	if deleted != 100 {
		t.Errorf("DeleteExpiredKeys deleted %d keys, want 100", deleted)
	}

	// Verify keys are gone
	for i := 0; i < 100; i++ {
		key := "key" + strconv.Itoa(i)
		if store.Exists(key) {
			t.Errorf("Key %s should not exist after cleanup", key)
		}
	}
}

// TestSnapshotWithExpiry tests that expiry is correctly saved and loaded from snapshots.
func TestSnapshotWithExpiry(t *testing.T) {
	store := NewStore()

	// Set keys with various expiry states
	futureExpiry := time.Now().UnixMilli() + 60000 // 1 minute from now
	pastExpiry := time.Now().UnixMilli() - 1000    // 1 second ago

	// Key with future expiry
	store.Set("future", []byte("DATAvalue1"))
	store.SetExpiry("future", futureExpiry)

	// Key with no expiry
	store.Set("noexpiry", []byte("DATAvalue2"))

	// Key with past expiry (should be skipped on load)
	store.Set("past", []byte("DATAvalue3"))
	store.SetExpiry("past", pastExpiry)

	// Save snapshot
	var buf bytes.Buffer
	if err := store.SaveSnapshotToWriter(&buf); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Load into new store
	store2 := NewStore()
	reader := bytes.NewReader(buf.Bytes())
	bufReader := bufioReader(reader)
	if err := store2.LoadSnapshotFromReader(bufReader); err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}

	// Verify "future" key exists with correct expiry
	exp, exists := store2.GetExpiry("future")
	if !exists {
		t.Error("Key 'future' should exist after load")
	}
	if exp != futureExpiry {
		t.Errorf("Expiry mismatch: got %d, want %d", exp, futureExpiry)
	}

	// Verify "noexpiry" key exists with no expiry
	exp, exists = store2.GetExpiry("noexpiry")
	if !exists {
		t.Error("Key 'noexpiry' should exist after load")
	}
	if exp != -1 {
		t.Errorf("Key 'noexpiry' should have no expiry, got %d", exp)
	}

	// Verify "past" key does NOT exist (skipped during load)
	_, exists = store2.GetExpiry("past")
	if exists {
		t.Error("Key 'past' should not exist after load (expired)")
	}
}

// bufioReader is a helper to create a bufio.Reader for testing
func bufioReader(r *bytes.Reader) *bufio.Reader {
	return bufio.NewReader(r)
}
