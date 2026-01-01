package main

import (
	"fmt"
	"io"
)

// wrongTypeResponse sends a WRONGTYPE error to the client.
func (app *application) wrongTypeResponse(w io.Writer) {
	_ = app.writeErrorResponse(w, "WRONGTYPE Operation against a key holding the wrong kind of value")
}

// unknownCommandResponse sends an unknown command error to the client.
func (app *application) unknownCommandResponse(w io.Writer, commandName string) {
	msg := fmt.Sprintf("ERR unknown command '%s'", commandName)
	_ = app.writeErrorResponse(w, msg)
}

// wrongNumberOfArgsResponse sends a wrong number of arguments error to the client.
func (app *application) wrongNumberOfArgsResponse(w io.Writer, commandName string) {
	msg := fmt.Sprintf("ERR wrong number of arguments for '%s' command", commandName)
	_ = app.writeErrorResponse(w, msg)
}
