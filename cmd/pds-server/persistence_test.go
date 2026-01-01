package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"
	"testing"
)

// TestStoreBinaryFormat verifies the low-level binary serialization.
// It replaces the old "TestSnapshotRoundTrip" but uses memory buffers
// to verify SaveSnapshotToWriter and LoadSnapshotFromReader directly.
func TestStoreBinaryFormat(t *testing.T) {
	// 1. Setup: Create a store and populate it.
	originalStore := NewStore()
	testData := make(map[string][]byte)

	// Add data to hit multiple shards
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := []byte(fmt.Sprintf("HYLLvalue-%d", i))
		testData[key] = val
		originalStore.Set(key, val)
	}

	// 2. Action: Write to Buffer (Simulate Disk)
	var buf bytes.Buffer
	if err := originalStore.SaveSnapshotToWriter(&buf); err != nil {
		t.Fatalf("SaveSnapshotToWriter failed: %v", err)
	}

	// 3. Verification: Read from Buffer
	newStore := NewStore()
	reader := bufio.NewReader(&buf)
	if err := newStore.LoadSnapshotFromReader(reader); err != nil {
		t.Fatalf("LoadSnapshotFromReader failed: %v", err)
	}

	// 4. Assert Data Integrity
	for key, expectedVal := range testData {
		gotVal, found := newStore.Get(key)
		if !found {
			t.Errorf("Key %s missing from loaded store", key)
			continue
		}
		if !bytes.Equal(gotVal, expectedVal) {
			t.Errorf("Key %s mismatch. Got %s, want %s", key, gotVal, expectedVal)
		}
	}
}

// TestAOFLog verifies that the logCommand helper writes correct RESP format.
func TestAOFLog(t *testing.T) {
	filename := "test_journal.aof"
	defer func() { _ = os.Remove(filename) }()

	aof, err := NewAOF(filename)
	if err != nil {
		t.Fatalf("Failed to create AOF: %v", err)
	}

	app := &application{aof: aof}

	// Log a command
	app.logCommand("HLL.ADD", []string{"mykey", "val"})

	// Close to flush the buffer to disk
	_ = aof.Close()

	// Read file content
	content, _ := os.ReadFile(filename)

	// Expected RESP:
	expected := "*3\r\n$7\r\nHLL.ADD\r\n$5\r\nmykey\r\n$3\r\nval\r\n"

	if string(content) != expected {
		t.Errorf("AOF content mismatch.\nGot: %q\nWant: %q", string(content), expected)
	}
}

// TestAOFReplay verifies "Legacy" text-only AOF loading.
func TestAOFReplay(t *testing.T) {
	filename := "test_replay.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Create a dummy text-based AOF
	// Command: HLL.ADD replay_key value1
	content := "*3\r\n$7\r\nHLL.ADD\r\n$10\r\nreplay_key\r\n$6\r\nvalue1\r\n"
	if err := os.WriteFile(filename, []byte(content), 0o666); err != nil {
		t.Fatalf("Failed to write dummy AOF: %v", err)
	}

	// 2. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename

	// Register Handler so Router works
	app.router.Handle("HLL.ADD", app.handleHLLAdd)

	// 3. Action
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed: %v", err)
	}

	// 4. Verify
	val, found := app.store.Get("replay_key")
	if !found {
		t.Fatal("Key not found in store after text replay")
	}
	// HLL.ADD creates an HLL structure, check magic header
	if len(val) < 4 || string(val[0:4]) != "HYLL" {
		t.Error("Value does not look like an HLL")
	}
}

// TestHybridAOFLoading verifies the "Smart Loader" (Binary + Text).
func TestHybridAOFLoading(t *testing.T) {
	filename := "test_hybrid.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Prepare Binary Data
	store := NewStore()
	store.Set("binary_key", []byte("HYLL_BIN"))

	// 2. Create the File and Write Binary Preamble
	f, _ := os.Create(filename)
	// Write binary snapshot
	if err := store.SaveSnapshotToWriter(f); err != nil {
		t.Fatal(err)
	}

	// 3. Append Text Command (Tail) directly to the same file
	// Command: HLL.ADD text_key value1
	textContent := "*3\r\n$7\r\nHLL.ADD\r\n$8\r\ntext_key\r\n$6\r\nvalue1\r\n"
	_, _ = f.WriteString(textContent)
	_ = f.Close()

	// 4. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename
	app.router.Handle("HLL.ADD", app.handleHLLAdd)

	// 5. Action: Load Hybrid File
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed on hybrid file: %v", err)
	}

	// 6. Verify Binary Part Loaded
	if val, ok := app.store.Get("binary_key"); !ok || string(val) != "HYLL_BIN" {
		t.Error("Binary preamble was not loaded correctly")
	}

	// 7. Verify Text Part Replayed
	if _, ok := app.store.Get("text_key"); !ok {
		t.Error("Text tail was not replayed correctly")
	}
}

// TestSnapshotCorruption verifies that we detect modified files.
func TestSnapshotCorruption(t *testing.T) {
	store := NewStore()
	store.Set("key1", []byte("data"))

	filename := "test_corrupt.pds"
	defer func() { _ = os.Remove(filename) }()

	// 1. Save valid snapshot
	f, _ := os.Create(filename)
	if err := store.SaveSnapshotToWriter(f); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// 2. Corrupt the file
	data, _ := os.ReadFile(filename)
	// Flip a bit in the middle of the data (after header)
	if len(data) > 10 {
		data[10] = data[10] ^ 0xFF
	}
	_ = os.WriteFile(filename, data, 0o666)

	// 3. Attempt Load
	// We need to simulate how loadAOF opens it
	fCorrupt, _ := os.Open(filename)
	defer func() { _ = fCorrupt.Close() }()
	reader := bufio.NewReader(fCorrupt)

	newStore := NewStore()
	err := newStore.LoadSnapshotFromReader(reader)

	// 4. Assert Failure
	if err == nil {
		t.Fatal("LoadSnapshot succeeded on a corrupt file! Checksum logic failed.")
	}
}

// TestAOFReplay_BloomFilter verifies text-only AOF loading with Bloom Filter commands.
func TestAOFReplay_BloomFilter(t *testing.T) {
	filename := "test_replay_bf.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Create a text-based AOF with BF.ADD commands
	// Command: BF.ADD bf_replay_key item1
	content := "*3\r\n$6\r\nBF.ADD\r\n$13\r\nbf_replay_key\r\n$5\r\nitem1\r\n"
	if err := os.WriteFile(filename, []byte(content), 0o666); err != nil {
		t.Fatalf("Failed to write dummy AOF: %v", err)
	}

	// 2. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename

	// Register Handler so Router works
	app.router.Handle("BF.ADD", app.handleBFAdd)

	// 3. Action
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed: %v", err)
	}

	// 4. Verify
	val, found := app.store.Get("bf_replay_key")
	if !found {
		t.Fatal("Key not found in store after text replay")
	}

	// BF.ADD creates a Bloom Filter structure, check magic header
	// Bloom filter magic is 0x424C4F4F4D303031 ("BLOOM001" in LE)
	if len(val) < 8 {
		t.Error("Value too short to be a Bloom Filter")
	}

	// Check the magic bytes (little-endian uint64)
	magic := uint64(val[0]) | uint64(val[1])<<8 | uint64(val[2])<<16 | uint64(val[3])<<24 |
		uint64(val[4])<<32 | uint64(val[5])<<40 | uint64(val[6])<<48 | uint64(val[7])<<56
	expectedMagic := uint64(0x424C4F4F4D303031)
	if magic != expectedMagic {
		t.Errorf("Value does not look like a Bloom Filter. Got magic %x, want %x", magic, expectedMagic)
	}
}

// TestHybridAOFLoading_BloomFilter verifies the hybrid loader with Bloom Filter data.
func TestHybridAOFLoading_BloomFilter(t *testing.T) {
	filename := "test_hybrid_bf.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Prepare Binary Data with a raw bloom filter signature
	store := NewStore()
	// Create a minimal valid bloom filter (just the header with magic)
	bfData := make([]byte, 24) // MetadataSize = 24
	// Set magic: 0x424C4F4F4D303031 in little-endian
	bfData[0] = 0x31
	bfData[1] = 0x30
	bfData[2] = 0x30
	bfData[3] = 0x4D
	bfData[4] = 0x4F
	bfData[5] = 0x4F
	bfData[6] = 0x4C
	bfData[7] = 0x42
	store.Set("binary_bf_key", bfData)

	// 2. Create the File and Write Binary Preamble
	f, _ := os.Create(filename)
	if err := store.SaveSnapshotToWriter(f); err != nil {
		t.Fatal(err)
	}

	// 3. Append Text Command (Tail) - a BF.ADD command
	// Command: BF.ADD text_bf_key item1
	textContent := "*3\r\n$6\r\nBF.ADD\r\n$11\r\ntext_bf_key\r\n$5\r\nitem1\r\n"
	_, _ = f.WriteString(textContent)
	_ = f.Close()

	// 4. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename
	app.router.Handle("BF.ADD", app.handleBFAdd)

	// 5. Action: Load Hybrid File
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed on hybrid file: %v", err)
	}

	// 6. Verify Binary Part Loaded
	if val, ok := app.store.Get("binary_bf_key"); !ok || len(val) < 8 {
		t.Error("Binary preamble was not loaded correctly")
	}

	// 7. Verify Text Part Replayed
	if _, ok := app.store.Get("text_bf_key"); !ok {
		t.Error("Text tail was not replayed correctly")
	}
}

// TestAOFReplay_String verifies text-only AOF loading with String commands.
func TestAOFReplay_String(t *testing.T) {
	filename := "test_replay_string.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Create a text-based AOF with SET and INCR commands
	// Commands: SET string_key hello, INCR counter (3 times)
	content := "*3\r\n$3\r\nSET\r\n$10\r\nstring_key\r\n$5\r\nhello\r\n" +
		"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n" +
		"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n" +
		"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"
	if err := os.WriteFile(filename, []byte(content), 0o666); err != nil {
		t.Fatalf("Failed to write dummy AOF: %v", err)
	}

	// 2. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename

	// 3. Action
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed: %v", err)
	}

	// 4. Verify SET command
	val, found := app.store.Get("string_key")
	if !found {
		t.Fatal("String key not found in store after text replay")
	}
	// Check magic header "DATA"
	if len(val) < 4 || string(val[0:4]) != "DATA" {
		t.Error("Value does not have DATA magic header")
	}
	if string(val[4:]) != "hello" {
		t.Errorf("SET value mismatch: got %q, want %q", string(val[4:]), "hello")
	}

	// 5. Verify INCR commands (should be 3)
	counterVal, found := app.store.Get("counter")
	if !found {
		t.Fatal("Counter key not found in store after text replay")
	}
	if len(counterVal) < 4 || string(counterVal[0:4]) != "DATA" {
		t.Error("Counter does not have DATA magic header")
	}
	if string(counterVal[4:]) != "3" {
		t.Errorf("INCR value mismatch: got %q, want %q", string(counterVal[4:]), "3")
	}
}

// TestHybridAOFLoading_String verifies the hybrid loader with String data.
func TestHybridAOFLoading_String(t *testing.T) {
	filename := "test_hybrid_string.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Prepare Binary Data with a string value (DATA header + payload)
	store := NewStore()
	// Create a valid string value with DATA magic
	stringData := append([]byte("DATA"), []byte("binary_value")...)
	store.Set("binary_string_key", stringData)

	// Also add a counter
	counterData := append([]byte("DATA"), []byte("100")...)
	store.Set("binary_counter", counterData)

	// 2. Create the File and Write Binary Preamble
	f, _ := os.Create(filename)
	if err := store.SaveSnapshotToWriter(f); err != nil {
		t.Fatal(err)
	}

	// 3. Append Text Commands (Tail) - SET and INCR commands
	// SET text_string_key text_value
	// INCRBY binary_counter 50
	textContent := "*3\r\n$3\r\nSET\r\n$15\r\ntext_string_key\r\n$10\r\ntext_value\r\n" +
		"*3\r\n$6\r\nINCRBY\r\n$14\r\nbinary_counter\r\n$2\r\n50\r\n"
	_, _ = f.WriteString(textContent)
	_ = f.Close()

	// 4. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename

	// 5. Action: Load Hybrid File
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed on hybrid file: %v", err)
	}

	// 6. Verify Binary String Loaded
	if val, ok := app.store.Get("binary_string_key"); !ok {
		t.Error("Binary string preamble was not loaded correctly")
	} else if string(val) != "DATAbinary_value" {
		t.Errorf("Binary string value mismatch: got %q", string(val))
	}

	// 7. Verify Text SET Replayed
	if val, ok := app.store.Get("text_string_key"); !ok {
		t.Error("Text SET was not replayed correctly")
	} else if len(val) < 4 || string(val[0:4]) != "DATA" {
		t.Error("Text string missing DATA header")
	} else if string(val[4:]) != "text_value" {
		t.Errorf("Text string value mismatch: got %q", string(val[4:]))
	}

	// 8. Verify INCRBY was applied on top of binary data
	// Binary counter was 100, INCRBY 50 should make it 150
	if val, ok := app.store.Get("binary_counter"); !ok {
		t.Error("Counter key missing after hybrid load")
	} else if len(val) < 4 || string(val[0:4]) != "DATA" {
		t.Error("Counter missing DATA header")
	} else if string(val[4:]) != "150" {
		t.Errorf("Counter value after INCRBY mismatch: got %q, want 150", string(val[4:]))
	}
}

// TestAOFCompaction_String proves that String data is preserved through compaction.
func TestAOFCompaction_String(t *testing.T) {
	filename := "test_compaction_string.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Setup
	app := newTestApp(t)
	app.config.aofFilename = filename

	// Open AOF explicitly
	var err error
	app.aof, err = NewAOF(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.aof.Close() }()

	// 2. The "Bloat" Phase - many SET commands overwriting the same key
	iterations := 500
	for i := 0; i < iterations; i++ {
		val := fmt.Sprintf("value_%d", i)
		app.logCommand("SET", []string{"overwritten_key", val})
	}

	// Also log many INCR commands
	for i := 0; i < iterations; i++ {
		app.logCommand("INCR", []string{"counter_key"})
	}

	// Set final state in memory
	finalValue := append([]byte("DATA"), []byte(fmt.Sprintf("value_%d", iterations-1))...)
	app.store.Set("overwritten_key", finalValue)

	counterValue := append([]byte("DATA"), []byte(fmt.Sprintf("%d", iterations))...)
	app.store.Set("counter_key", counterValue)

	// Flush to disk
	_ = app.aof.Fsync()

	statBefore, _ := os.Stat(filename)
	sizeBefore := statBefore.Size()
	t.Logf("Log Size Before Compaction: %d bytes", sizeBefore)

	// 3. Compact
	if err := app.CompactAOF(); err != nil {
		t.Fatalf("CompactAOF failed: %v", err)
	}

	statAfter, _ := os.Stat(filename)
	sizeAfter := statAfter.Size()
	t.Logf("Log Size After Compaction: %d bytes", sizeAfter)

	// 4. Verify Size Reduction
	if sizeAfter >= sizeBefore {
		t.Errorf("Compaction did not reduce size. Before: %d, After: %d", sizeBefore, sizeAfter)
	}

	// 5. Verify Integrity (Reload)
	recoveryApp := newTestApp(t)
	recoveryApp.config.aofFilename = filename

	if err := recoveryApp.loadAOF(); err != nil {
		t.Fatalf("Failed to load compacted AOF: %v", err)
	}

	// Check overwritten_key
	val, exists := recoveryApp.store.Get("overwritten_key")
	if !exists {
		t.Fatal("overwritten_key missing after compaction")
	}
	expectedFinal := fmt.Sprintf("value_%d", iterations-1)
	if len(val) < 4 || string(val[4:]) != expectedFinal {
		t.Errorf("overwritten_key value mismatch: got %q, want %q", string(val[4:]), expectedFinal)
	}

	// Check counter_key
	val, exists = recoveryApp.store.Get("counter_key")
	if !exists {
		t.Fatal("counter_key missing after compaction")
	}
	expectedCounter := fmt.Sprintf("%d", iterations)
	if len(val) < 4 || string(val[4:]) != expectedCounter {
		t.Errorf("counter_key value mismatch: got %q, want %q", string(val[4:]), expectedCounter)
	}
}

// TestAOFReplay_CMS verifies text-only AOF loading with Count-Min Sketch commands.
func TestAOFReplay_CMS(t *testing.T) {
	filename := "test_replay_cms.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Create a text-based AOF with CMS.INIT and CMS.INCRBY commands
	// Commands: CMS.INIT cms_replay_key 100 5, CMS.INCRBY cms_replay_key item1 10
	content := "*4\r\n$8\r\nCMS.INIT\r\n$14\r\ncms_replay_key\r\n$3\r\n100\r\n$1\r\n5\r\n" +
		"*4\r\n$10\r\nCMS.INCRBY\r\n$14\r\ncms_replay_key\r\n$5\r\nitem1\r\n$2\r\n10\r\n"
	if err := os.WriteFile(filename, []byte(content), 0o666); err != nil {
		t.Fatalf("Failed to write dummy AOF: %v", err)
	}

	// 2. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename

	// Register Handlers so Router works
	app.router.Handle("CMS.INIT", app.handleCMSInit)
	app.router.Handle("CMS.INCRBY", app.handleCMSIncrBy)

	// 3. Action
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed: %v", err)
	}

	// 4. Verify
	val, found := app.store.Get("cms_replay_key")
	if !found {
		t.Fatal("Key not found in store after text replay")
	}

	// CMS.INIT creates a CMS structure, check magic header
	// CMS magic is 0x31534D43 ("CMS1" in LE)
	if len(val) < 4 {
		t.Error("Value too short to be a CMS")
	}

	// Check the magic bytes (little-endian uint32)
	magic := uint32(val[0]) | uint32(val[1])<<8 | uint32(val[2])<<16 | uint32(val[3])<<24
	expectedMagic := uint32(0x31534D43)
	if magic != expectedMagic {
		t.Errorf("Value does not look like a CMS. Got magic %x, want %x", magic, expectedMagic)
	}
}

// TestHybridAOFLoading_CMS verifies the hybrid loader with Count-Min Sketch data.
func TestHybridAOFLoading_CMS(t *testing.T) {
	filename := "test_hybrid_cms.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Prepare Binary Data with a raw CMS structure
	store := NewStore()
	// Create a minimal valid CMS (header only: 20 bytes)
	// Header: Magic (4) + Width (4) + Depth (4) + Count (8) = 20 bytes
	// Then counters: width * depth * 4 bytes
	width := uint32(10)
	depth := uint32(3)
	headerSize := 20
	dataSize := headerSize + int(width*depth*4)
	cmsData := make([]byte, dataSize)

	// Set magic: 0x31534D43 in little-endian
	cmsData[0] = 0x43 // 'C'
	cmsData[1] = 0x4D // 'M'
	cmsData[2] = 0x53 // 'S'
	cmsData[3] = 0x31 // '1'
	// Set width
	cmsData[4] = byte(width)
	cmsData[5] = byte(width >> 8)
	cmsData[6] = byte(width >> 16)
	cmsData[7] = byte(width >> 24)
	// Set depth
	cmsData[8] = byte(depth)
	cmsData[9] = byte(depth >> 8)
	cmsData[10] = byte(depth >> 16)
	cmsData[11] = byte(depth >> 24)
	// Count is 0 (bytes 12-19)

	store.Set("binary_cms_key", cmsData)

	// 2. Create the File and Write Binary Preamble
	f, _ := os.Create(filename)
	if err := store.SaveSnapshotToWriter(f); err != nil {
		t.Fatal(err)
	}

	// 3. Append Text Commands (Tail) - CMS.INIT and CMS.INCRBY commands
	// CMS.INIT text_cms_key 50 4
	// CMS.INCRBY text_cms_key item1 5
	textContent := "*4\r\n$8\r\nCMS.INIT\r\n$12\r\ntext_cms_key\r\n$2\r\n50\r\n$1\r\n4\r\n" +
		"*4\r\n$10\r\nCMS.INCRBY\r\n$12\r\ntext_cms_key\r\n$5\r\nitem1\r\n$1\r\n5\r\n"
	_, _ = f.WriteString(textContent)
	_ = f.Close()

	// 4. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename
	app.router.Handle("CMS.INIT", app.handleCMSInit)
	app.router.Handle("CMS.INCRBY", app.handleCMSIncrBy)

	// 5. Action: Load Hybrid File
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed on hybrid file: %v", err)
	}

	// 6. Verify Binary Part Loaded
	if val, ok := app.store.Get("binary_cms_key"); !ok || len(val) < 20 {
		t.Error("Binary preamble was not loaded correctly")
	}

	// 7. Verify Text Part Replayed
	if _, ok := app.store.Get("text_cms_key"); !ok {
		t.Error("Text tail was not replayed correctly")
	}
}

// TestAOFReplay_TopK verifies text-only AOF loading with Top-K commands.
func TestAOFReplay_TopK(t *testing.T) {
	filename := "test_replay_topk.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Create a text-based AOF with TOPK.RESERVE and TOPK.ADD commands
	// Commands: TOPK.RESERVE topk_replay_key 5, TOPK.ADD topk_replay_key item1 item2
	content := "*3\r\n$12\r\nTOPK.RESERVE\r\n$15\r\ntopk_replay_key\r\n$1\r\n5\r\n" +
		"*4\r\n$8\r\nTOPK.ADD\r\n$15\r\ntopk_replay_key\r\n$5\r\nitem1\r\n$5\r\nitem2\r\n"
	if err := os.WriteFile(filename, []byte(content), 0o666); err != nil {
		t.Fatalf("Failed to write dummy AOF: %v", err)
	}

	// 2. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename

	// Register Handlers so Router works
	app.router.Handle("TOPK.RESERVE", app.handleTopKReserve)
	app.router.Handle("TOPK.ADD", app.handleTopKAdd)

	// 3. Action
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed: %v", err)
	}

	// 4. Verify
	val, found := app.store.Get("topk_replay_key")
	if !found {
		t.Fatal("Key not found in store after text replay")
	}

	// TOPK.RESERVE creates a TopK structure, check magic header
	// TopK magic is 0x4B504F54 ("TOPK" in LE)
	if len(val) < 4 {
		t.Error("Value too short to be a TopK")
	}

	// Check the magic bytes (little-endian uint32)
	magic := uint32(val[0]) | uint32(val[1])<<8 | uint32(val[2])<<16 | uint32(val[3])<<24
	expectedMagic := uint32(0x4B504F54)
	if magic != expectedMagic {
		t.Errorf("Value does not look like a TopK. Got magic %x, want %x", magic, expectedMagic)
	}
}

// TestHybridAOFLoading_TopK verifies the hybrid loader with Top-K data.
func TestHybridAOFLoading_TopK(t *testing.T) {
	filename := "test_hybrid_topk.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Prepare Binary Data with a raw TopK structure
	store := NewStore()
	// Create a minimal valid TopK (header: 28 bytes + heap data + buckets)
	// Header: Magic (4) + K (4) + Width (4) + Depth (4) + Decay (8) + HeapN (4) = 28 bytes
	// Then: HeapN items + Width * Depth buckets (16 bytes each)
	k := uint32(5)
	width := uint32(10)
	depth := uint32(3)
	headerSize := 28
	heapItems := 0 // empty heap
	bucketSize := int(width * depth * 16)
	dataSize := headerSize + bucketSize
	topkData := make([]byte, dataSize)

	// Set magic: 0x4B504F54 in little-endian
	topkData[0] = 0x54 // 'T'
	topkData[1] = 0x4F // 'O'
	topkData[2] = 0x50 // 'P'
	topkData[3] = 0x4B // 'K'
	// Set K
	topkData[4] = byte(k)
	topkData[5] = byte(k >> 8)
	topkData[6] = byte(k >> 16)
	topkData[7] = byte(k >> 24)
	// Set width
	topkData[8] = byte(width)
	topkData[9] = byte(width >> 8)
	topkData[10] = byte(width >> 16)
	topkData[11] = byte(width >> 24)
	// Set depth
	topkData[12] = byte(depth)
	topkData[13] = byte(depth >> 8)
	topkData[14] = byte(depth >> 16)
	topkData[15] = byte(depth >> 24)
	// Set decay (0.9 as float64)
	decayBits := math.Float64bits(0.9)
	for i := 0; i < 8; i++ {
		topkData[16+i] = byte(decayBits >> (i * 8))
	}
	// Set heapN = 0
	topkData[24] = byte(heapItems)
	topkData[25] = byte(heapItems >> 8)
	topkData[26] = byte(heapItems >> 16)
	topkData[27] = byte(heapItems >> 24)

	store.Set("binary_topk_key", topkData)

	// 2. Create the File and Write Binary Preamble
	f, _ := os.Create(filename)
	if err := store.SaveSnapshotToWriter(f); err != nil {
		t.Fatal(err)
	}

	// 3. Append Text Commands (Tail) - TOPK.RESERVE and TOPK.ADD commands
	// TOPK.RESERVE text_topk_key 10
	// TOPK.ADD text_topk_key item1
	textContent := "*3\r\n$12\r\nTOPK.RESERVE\r\n$13\r\ntext_topk_key\r\n$2\r\n10\r\n" +
		"*3\r\n$8\r\nTOPK.ADD\r\n$13\r\ntext_topk_key\r\n$5\r\nitem1\r\n"
	_, _ = f.WriteString(textContent)
	_ = f.Close()

	// 4. Setup App
	app := newTestApp(t)
	app.config.aofFilename = filename
	app.router.Handle("TOPK.RESERVE", app.handleTopKReserve)
	app.router.Handle("TOPK.ADD", app.handleTopKAdd)

	// 5. Action: Load Hybrid File
	if err := app.loadAOF(); err != nil {
		t.Fatalf("loadAOF failed on hybrid file: %v", err)
	}

	// 6. Verify Binary Part Loaded
	if val, ok := app.store.Get("binary_topk_key"); !ok || len(val) < 28 {
		t.Error("Binary preamble was not loaded correctly")
	}

	// 7. Verify Text Part Replayed
	if _, ok := app.store.Get("text_topk_key"); !ok {
		t.Error("Text tail was not replayed correctly")
	}
}

// TestAOFCompaction proves that the Hybrid Rewrite drastically reduces disk usage
// by converting a long history of updates into a compact binary snapshot.
func TestAOFCompaction(t *testing.T) {
	filename := "test_compaction.aof"
	defer func() { _ = os.Remove(filename) }()

	// 1. Setup
	app := newTestApp(t)
	app.config.aofFilename = filename

	// Open AOF explicitly
	var err error
	app.aof, err = NewAOF(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.aof.Close() }()

	// 2. The "Bloat" Phase
	iterations := 1000
	for i := 0; i < iterations; i++ {
		val := fmt.Sprintf("%d", i)
		// A. Write to Log (Simulate history)
		app.logCommand("HLL.ADD", []string{"spamky", val})
	}

	// B. Update Memory (Simulate state)
	// In a real flow, HLL.ADD would have updated this.
	// We manually set the key so CompactAOF has something to save.
	// We use the "HYLL" prefix so verify checks pass.
	app.store.Set("spamky", []byte("HYLL_simulated_final_state"))

	// Flush to disk to get accurate size
	_ = app.aof.Fsync()

	statBefore, _ := os.Stat(filename)
	sizeBefore := statBefore.Size()

	if sizeBefore < 20000 {
		t.Fatalf("Expected bloated log > 20KB, got %d bytes", sizeBefore)
	}
	t.Logf("Log Size Before Compaction: %d bytes (Text Mode)", sizeBefore)

	// 3. The Compaction Phase
	if err := app.CompactAOF(); err != nil {
		t.Fatalf("CompactAOF failed: %v", err)
	}

	// 4. Verify Size Reduction
	statAfter, _ := os.Stat(filename)
	sizeAfter := statAfter.Size()

	t.Logf("Log Size After Compaction:  %d bytes (Hybrid/Binary Mode)", sizeAfter)

	// It should be much smaller now (binary state vs 1000 text commands)
	if sizeAfter > sizeBefore/10 {
		t.Errorf("Compaction failed to reduce size significantly. Before: %d, After: %d", sizeBefore, sizeAfter)
	}

	// 5. Verify Integrity (Reload)
	recoveryApp := newTestApp(t)
	recoveryApp.config.aofFilename = filename
	recoveryApp.router.Handle("HLL.ADD", recoveryApp.handleHLLAdd)

	if err := recoveryApp.loadAOF(); err != nil {
		t.Fatalf("Failed to load compacted AOF: %v", err)
	}

	val, exists := recoveryApp.store.Get("spamky")
	if !exists {
		t.Fatal("Data lost after compaction! Key 'spamky' missing.")
	}
	// Verify content matches what we put in the store
	if string(val) != "HYLL_simulated_final_state" {
		t.Error("Recovered data mismatch")
	}
}
