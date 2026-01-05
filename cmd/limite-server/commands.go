package main

// commands creates a new router and registers all the application's command handlers.
// This is the single source of truth for what commands the server supports.
func (app *application) commands() *Router {
	router := NewRouter()

	// Generic Commands
	router.Handle("PING", app.handlePing)
	router.Handle("DEL", app.handleDel)
	router.Handle("MEMORY", app.handleMemory)

	// String Commands
	router.Handle("SET", app.handleSet)
	router.Handle("GET", app.handleGet)
	router.Handle("INCR", app.handleIncr)
	router.Handle("DECR", app.handleDecr)
	router.Handle("INCRBY", app.handleIncrBy)
	router.Handle("DECRBY", app.handleDecrBy)

	// Persistence Control
	router.Handle("COMPACT", app.handleCompact)

	// Metrics
	router.Handle("INFO", app.handleInfo)

	// HyperLogLog
	router.Handle("HLL.ADD", app.handleHLLAdd)
	router.Handle("HLL.COUNT", app.handleHLLCount)
	router.Handle("HLL.MERGE", app.handleHLLMerge)

	// Bloom Filters
	router.Handle("BF.ADD", app.handleBFAdd)
	router.Handle("BF.MADD", app.handleBFMAdd)
	router.Handle("BF.EXISTS", app.handleBFExists)
	router.Handle("BF.MEXISTS", app.handleBFMExists)

	// Count-Min Sketch
	router.Handle("CMS.INIT", app.handleCMSInit)
	router.Handle("CMS.INITBYPROB", app.handleCMSInitByProb)
	router.Handle("CMS.INCRBY", app.handleCMSIncrBy)
	router.Handle("CMS.QUERY", app.handleCMSQuery)

	// Top-K (HeavyKeeper)
	router.Handle("TOPK.RESERVE", app.handleTopKReserve)
	router.Handle("TOPK.ADD", app.handleTopKAdd)
	router.Handle("TOPK.INCRBY", app.handleTopKIncrBy)
	router.Handle("TOPK.QUERY", app.handleTopKQuery)
	router.Handle("TOPK.LIST", app.handleTopKList)

	return router
}
