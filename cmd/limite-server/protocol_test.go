package main

import (
	"testing"
)

func TestEncodeCommand(t *testing.T) {
	tests := []struct {
		name    string
		command string
		args    []string
		want    string
	}{
		{
			name:    "Simple Command",
			command: "PING",
			args:    []string{},
			// *1\r\n$4\r\nPING\r\n
			want: "*1\r\n$4\r\nPING\r\n",
		},
		{
			name:    "Command with Args",
			command: "HLL.ADD",
			args:    []string{"users", "user1"},
			// *3\r\n$7\r\nHLL.ADD\r\n$5\r\nusers\r\n$5\r\nuser1\r\n
			want: "*3\r\n$7\r\nHLL.ADD\r\n$5\r\nusers\r\n$5\r\nuser1\r\n",
		},
		{
			name:    "Empty String Argument",
			command: "SET",
			args:    []string{"mykey", ""},
			// *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$0\r\n\r\n
			want: "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$0\r\n\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeCommand(tt.command, tt.args)
			if string(got) != tt.want {
				t.Errorf("encodeCommand() = %q, want %q", string(got), tt.want)
			}
		})
	}
}
