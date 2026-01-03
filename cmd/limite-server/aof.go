// aof.go provides the low-level file handle wrapper for the Append-Only File.
// This is a thin abstraction over os.File that adds buffered writes and mutex
// synchronization, allowing multiple goroutines to safely append commands.
//
// The AOF struct does not concern itself with what is being written—it simply
// accepts raw bytes and ensures they reach the disk safely. The higher-level
// persistence logic (format encoding, compaction, hybrid loading) lives in
// persistence.go and store.go.
//
// Write operations go to an in-memory buffer first (via bufio.Writer), which
// is periodically flushed to the OS by the background maintenance loop in
// main.go. The Fsync method forces the OS to commit buffered data to the
// physical disk, providing the durability guarantee.

package main

import (
	"bufio"
	"os"
	"sync"
)

type AOF struct {
	mu     sync.Mutex
	file   *os.File
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}

	aof := &AOF{
		file:   f,
		reader: bufio.NewReader(f),
		writer: bufio.NewWriter(f),
	}

	return aof, nil
}

func (aof *AOF) Write(data []byte) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// data is written to the internal RAM buffer, not directly to disk (yet).
	// If the buffer fills up, it will auto-flush to disk.
	_, err := aof.writer.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (aof *AOF) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	if err := aof.writer.Flush(); err != nil {
		return err
	}

	if err := aof.file.Close(); err != nil {
		return err
	}

	return nil
}

// Fsync flushes the internal buffer to the OS and forces the OS to write
// changes to the physical disk. This ensures durability.
func (aof *AOF) Fsync() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// 1. Flush the RAM buffer to the OS Kernel
	if err := aof.writer.Flush(); err != nil {
		return err
	}

	// 2. Force the OS to write to the physical disk
	// This prevents data loss even if the OS crashes/loses power.
	if err := aof.file.Sync(); err != nil {
		return err
	}

	return nil
}
