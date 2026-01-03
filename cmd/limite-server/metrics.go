package main

import "sync/atomic"

// Metrics holds the atomic counters for monitoring the server's health.
type Metrics struct {
	TotalConnections atomic.Uint64 // Counts total connections ever made
	TotalCommands    atomic.Uint64 // Counts total commands ever processed
}

// NewMetrics creates and returns a new Metrics struct.
func NewMetrics() *Metrics {
	return &Metrics{}
}
