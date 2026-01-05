package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestMemoryUsageBasic tests MEMORY USAGE on an existing key.
func TestMemoryUsageBasic(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Setup: create a string key
	app.handleSet(&buf, []string{"mykey", "hello"})
	buf.Reset()

	// Test MEMORY USAGE
	app.handleMemory(&buf, []string{"USAGE", "mykey"})
	resp := buf.String()

	// Should return an integer (colon prefix)
	if !strings.HasPrefix(resp, ":") {
		t.Errorf("expected integer response, got %q", resp)
	}

	// Parse and verify reasonable size
	// Key "mykey" (5) + DATA header (4) + "hello" (5) + overhead (72) = 86
	if resp != ":86\r\n" {
		t.Errorf("expected :86, got %q", resp)
	}
}

// TestMemoryUsageMissingKey tests MEMORY USAGE on a non-existent key.
func TestMemoryUsageMissingKey(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleMemory(&buf, []string{"USAGE", "nonexistent"})

	// Should return nil ($-1)
	if buf.String() != "$-1\r\n" {
		t.Errorf("expected nil response, got %q", buf.String())
	}
}

// TestMemoryUsageHLL tests MEMORY USAGE on a HyperLogLog key.
func TestMemoryUsageHLL(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Create an HLL with some data
	app.handleHLLAdd(&buf, []string{"hll_key", "item1", "item2", "item3"})
	buf.Reset()

	// Test MEMORY USAGE
	app.handleMemory(&buf, []string{"USAGE", "hll_key"})
	resp := buf.String()

	// Should return an integer
	if !strings.HasPrefix(resp, ":") {
		t.Errorf("expected integer response, got %q", resp)
	}

	// HLL should be at least header (16) + some data + key (7) + overhead (72)
	// Minimum for sparse HLL: ~100 bytes
	t.Logf("HLL memory usage: %s", strings.TrimSpace(resp))
}

// TestMemoryUsageBloom tests MEMORY USAGE on a Bloom Filter key.
func TestMemoryUsageBloom(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Create a Bloom filter
	app.handleBFAdd(&buf, []string{"bf_key", "item1"})
	buf.Reset()

	// Test MEMORY USAGE
	app.handleMemory(&buf, []string{"USAGE", "bf_key"})
	resp := buf.String()

	if !strings.HasPrefix(resp, ":") {
		t.Errorf("expected integer response, got %q", resp)
	}
	t.Logf("Bloom filter memory usage: %s", strings.TrimSpace(resp))
}

// TestMemoryUsageCMS tests MEMORY USAGE on a Count-Min Sketch key.
func TestMemoryUsageCMS(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Create a CMS
	app.handleCMSInit(&buf, []string{"cms_key", "1000", "5"})
	buf.Reset()

	// Test MEMORY USAGE
	app.handleMemory(&buf, []string{"USAGE", "cms_key"})
	resp := buf.String()

	if !strings.HasPrefix(resp, ":") {
		t.Errorf("expected integer response, got %q", resp)
	}

	// CMS 1000x5 should be: header (20) + counters (1000*5*4=20000) + key (7) + overhead (72)
	t.Logf("CMS memory usage: %s", strings.TrimSpace(resp))
}

// TestMemoryUsageTopK tests MEMORY USAGE on a TopK key.
func TestMemoryUsageTopK(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Create a TopK
	app.handleTopKReserve(&buf, []string{"topk_key", "10"})
	buf.Reset()

	// Test MEMORY USAGE
	app.handleMemory(&buf, []string{"USAGE", "topk_key"})
	resp := buf.String()

	if !strings.HasPrefix(resp, ":") {
		t.Errorf("expected integer response, got %q", resp)
	}
	t.Logf("TopK memory usage: %s", strings.TrimSpace(resp))
}

// TestMemoryUsageWrongArgs tests MEMORY USAGE with wrong number of arguments.
func TestMemoryUsageWrongArgs(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	tests := []struct {
		name string
		args []string
	}{
		{"no args", []string{}},
		{"usage no key", []string{"USAGE"}},
		{"usage too many args", []string{"USAGE", "key1", "key2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			app.handleMemory(&buf, tt.args)

			if !strings.HasPrefix(buf.String(), "-ERR") {
				t.Errorf("expected error response, got %q", buf.String())
			}
		})
	}
}

// TestMemoryUnknownSubcommand tests MEMORY with an unknown subcommand.
func TestMemoryUnknownSubcommand(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	app.handleMemory(&buf, []string{"UNKNOWN", "key"})

	if !strings.Contains(buf.String(), "unknown subcommand") {
		t.Errorf("expected 'unknown subcommand' error, got %q", buf.String())
	}
}

// TestMemoryUsageCaseInsensitive tests that subcommand is case-insensitive.
func TestMemoryUsageCaseInsensitive(t *testing.T) {
	app := newTestApp(t)
	var buf bytes.Buffer

	// Create a key
	app.handleSet(&buf, []string{"key", "value"})
	buf.Reset()

	// Test lowercase
	app.handleMemory(&buf, []string{"usage", "key"})
	if !strings.HasPrefix(buf.String(), ":") {
		t.Errorf("lowercase 'usage' should work, got %q", buf.String())
	}

	buf.Reset()

	// Test mixed case
	app.handleMemory(&buf, []string{"Usage", "key"})
	if !strings.HasPrefix(buf.String(), ":") {
		t.Errorf("mixed case 'Usage' should work, got %q", buf.String())
	}
}
