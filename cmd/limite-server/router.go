package main

import (
	"io"
	"strings"
)

// CommandHandler defines the function signature for a command handler.
// Handlers write responses to the provided io.Writer, which is typically
// a buffered writer wrapping the connection.
type CommandHandler func(w io.Writer, args []string)

// Router holds the mapping of command strings to their handlers.
type Router struct {
	handlers map[string]CommandHandler
}

// NewRouter creates a new, empty router.
func NewRouter() *Router {
	return &Router{
		handlers: make(map[string]CommandHandler),
	}
}

// Handle registers a new command handler.
func (r *Router) Handle(name string, handler CommandHandler) {
	r.handlers[strings.ToUpper(name)] = handler
}

// Dispatch finds the handler for a given command and executes it.
func (r *Router) Dispatch(app *application, w io.Writer, parts []string) {
	if len(parts) == 0 {
		return
	}

	app.metrics.TotalCommands.Add(1)

	commandName := strings.ToUpper(parts[0])
	args := parts[1:]

	handler, found := r.handlers[commandName]
	if !found {
		app.unknownCommandResponse(w, commandName)
		return
	}

	handler(w, args)
}
