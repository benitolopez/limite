package main

import (
	"bytes"
	"testing"
)

func TestWriteBulkBytesResponse(t *testing.T) {
	app := &application{}

	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{
			name:  "Simple string",
			input: []byte("hello"),
			want:  "$5\r\nhello\r\n",
		},
		{
			name:  "Empty bytes",
			input: []byte{},
			want:  "$0\r\n\r\n",
		},
		{
			name:  "Binary data with null bytes",
			input: []byte{0x00, 0xFF, 0x10},
			want:  "$3\r\n\x00\xff\x10\r\n",
		},
		{
			name:  "Longer string",
			input: []byte("the quick brown fox jumps over the lazy dog"),
			want:  "$43\r\nthe quick brown fox jumps over the lazy dog\r\n",
		},
		{
			name:  "Single byte",
			input: []byte{65}, // 'A'
			want:  "$1\r\nA\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := app.writeBulkBytesResponse(&buf, tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if buf.String() != tt.want {
				t.Errorf("got %q, want %q", buf.String(), tt.want)
			}
		})
	}
}
