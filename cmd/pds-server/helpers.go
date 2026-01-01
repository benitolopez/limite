package main

import (
	"net"
	"time"
)

// writeResponse is a helper function that writes data to a connection.
// It centralizes error handling and returns an error if the write fails.
// This allows the calling handler to stop processing if the client connection is dead.
func (app *application) writeResponse(conn net.Conn, data []byte) error {
	remoteAddr := conn.RemoteAddr().String()

	// Set a write deadline to protect against slow or stuck clients.
	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		app.logger.Error("failed to set write deadline", "error", err, "remote_addr", remoteAddr)
		return err
	}

	_, err := conn.Write(data)
	if err != nil {
		app.logger.Error("failed to write response", "error", err, "remote_addr", remoteAddr)
		return err
	}
	return nil
}
