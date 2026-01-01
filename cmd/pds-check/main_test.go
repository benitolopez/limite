package main

import (
	"encoding/binary"
	"testing"
)

func TestIdentifyTypeString(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		wantType    string
		wantDetails string
	}{
		{
			name:        "Integer string (positive)",
			data:        []byte{'D', 'A', 'T', 'A', '1', '2', '3'},
			wantType:    "String",
			wantDetails: "Int:123",
		},
		{
			name:        "Integer string (negative)",
			data:        []byte{'D', 'A', 'T', 'A', '-', '4', '5', '6'},
			wantType:    "String",
			wantDetails: "Int:-456",
		},
		{
			name:        "Integer string (zero)",
			data:        []byte{'D', 'A', 'T', 'A', '0'},
			wantType:    "String",
			wantDetails: "Int:0",
		},
		{
			name:        "Empty string",
			data:        []byte{'D', 'A', 'T', 'A'},
			wantType:    "String",
			wantDetails: "Len:0 (empty)",
		},
		{
			name:        "Short string",
			data:        []byte{'D', 'A', 'T', 'A', 'h', 'e', 'l', 'l', 'o'},
			wantType:    "String",
			wantDetails: `Len:5 Val:"hello"`,
		},
		{
			name:        "Long string (truncated)",
			data:        append([]byte{'D', 'A', 'T', 'A'}, []byte("this is a very long string that should be truncated in the output")...),
			wantType:    "String",
			wantDetails: `Len:65 Val:"this is a very long stri..."`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotDetails := identifyType(tt.data)
			if gotType != tt.wantType {
				t.Errorf("identifyType() type = %q, want %q", gotType, tt.wantType)
			}
			if gotDetails != tt.wantDetails {
				t.Errorf("identifyType() details = %q, want %q", gotDetails, tt.wantDetails)
			}
		})
	}
}

func TestIdentifyTypeHLL(t *testing.T) {
	// Create a minimal HLL header (16 bytes minimum for cardinality)
	hllDense := make([]byte, 16)
	copy(hllDense, "HYLL")
	hllDense[4] = 0 // Dense encoding
	binary.LittleEndian.PutUint64(hllDense[8:16], 12345)

	hllSparse := make([]byte, 16)
	copy(hllSparse, "HYLL")
	hllSparse[4] = 1 // Sparse encoding
	binary.LittleEndian.PutUint64(hllSparse[8:16], 100)

	tests := []struct {
		name        string
		data        []byte
		wantType    string
		wantDetails string
	}{
		{
			name:        "HLL Dense",
			data:        hllDense,
			wantType:    "HLL-Dense",
			wantDetails: "Card:~12345",
		},
		{
			name:        "HLL Sparse",
			data:        hllSparse,
			wantType:    "HLL-Sparse",
			wantDetails: "Card:~100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotDetails := identifyType(tt.data)
			if gotType != tt.wantType {
				t.Errorf("identifyType() type = %q, want %q", gotType, tt.wantType)
			}
			if gotDetails != tt.wantDetails {
				t.Errorf("identifyType() details = %q, want %q", gotDetails, tt.wantDetails)
			}
		})
	}
}

func TestIdentifyTypeBloomFilter(t *testing.T) {
	// Create a minimal Bloom Filter header (24 bytes)
	bf := make([]byte, 24)
	binary.LittleEndian.PutUint64(bf[0:8], bloomMagic)
	binary.LittleEndian.PutUint64(bf[8:16], 1000) // Total items
	binary.LittleEndian.PutUint64(bf[16:24], 3)   // Num layers

	gotType, gotDetails := identifyType(bf)
	if gotType != "BloomFilter" {
		t.Errorf("identifyType() type = %q, want %q", gotType, "BloomFilter")
	}
	if gotDetails != "Items:1000, Layers:3" {
		t.Errorf("identifyType() details = %q, want %q", gotDetails, "Items:1000, Layers:3")
	}
}

func TestIdentifyTypeCMS(t *testing.T) {
	// Create a minimal Count-Min Sketch header (20 bytes)
	// Layout: Magic(4) + Width(4) + Depth(4) + Count(8) = 20 bytes
	cms := make([]byte, 20)
	binary.LittleEndian.PutUint32(cms[0:4], cmsMagic) // Magic "CMS1"
	binary.LittleEndian.PutUint32(cms[4:8], 1000)     // Width
	binary.LittleEndian.PutUint32(cms[8:12], 5)       // Depth
	binary.LittleEndian.PutUint64(cms[12:20], 50000)  // Count

	gotType, gotDetails := identifyType(cms)
	if gotType != "CountMinSketch" {
		t.Errorf("identifyType() type = %q, want %q", gotType, "CountMinSketch")
	}
	if gotDetails != "Width:1000, Depth:5, Count:50000" {
		t.Errorf("identifyType() details = %q, want %q", gotDetails, "Width:1000, Depth:5, Count:50000")
	}
}

func TestIdentifyTypeTopK(t *testing.T) {
	// Create a minimal Top-K header (28 bytes)
	// Layout: Magic(4) + K(4) + Width(4) + Depth(4) + Decay(8) + HeapN(4) = 28 bytes
	topk := make([]byte, 28)
	binary.LittleEndian.PutUint32(topk[0:4], topkMagic) // Magic "TOPK"
	binary.LittleEndian.PutUint32(topk[4:8], 100)       // K
	binary.LittleEndian.PutUint32(topk[8:12], 2048)     // Width
	binary.LittleEndian.PutUint32(topk[12:16], 5)       // Depth
	// Decay at bytes 16-24 (float64, we don't display it)
	binary.LittleEndian.PutUint32(topk[24:28], 42) // HeapN (current heap size)

	gotType, gotDetails := identifyType(topk)
	if gotType != "TopK" {
		t.Errorf("identifyType() type = %q, want %q", gotType, "TopK")
	}
	if gotDetails != "K:100, Width:2048, Depth:5, HeapSize:42" {
		t.Errorf("identifyType() details = %q, want %q", gotDetails, "K:100, Width:2048, Depth:5, HeapSize:42")
	}
}

func TestIdentifyTypeTopKEmpty(t *testing.T) {
	// Test with an empty heap (freshly created TopK)
	topk := make([]byte, 28)
	binary.LittleEndian.PutUint32(topk[0:4], topkMagic) // Magic "TOPK"
	binary.LittleEndian.PutUint32(topk[4:8], 50)        // K
	binary.LittleEndian.PutUint32(topk[8:12], 1024)     // Width
	binary.LittleEndian.PutUint32(topk[12:16], 3)       // Depth
	binary.LittleEndian.PutUint32(topk[24:28], 0)       // HeapN = 0

	gotType, gotDetails := identifyType(topk)
	if gotType != "TopK" {
		t.Errorf("identifyType() type = %q, want %q", gotType, "TopK")
	}
	if gotDetails != "K:50, Width:1024, Depth:3, HeapSize:0" {
		t.Errorf("identifyType() details = %q, want %q", gotDetails, "K:50, Width:1024, Depth:3, HeapSize:0")
	}
}

func TestIdentifyTypeRaw(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"Empty data", []byte{}},
		{"Short data", []byte{0x01, 0x02}},
		{"Unknown magic", []byte{'U', 'N', 'K', 'N', 'O', 'W', 'N'}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotDetails := identifyType(tt.data)
			if gotType != "Raw" {
				t.Errorf("identifyType() type = %q, want %q", gotType, "Raw")
			}
			if gotDetails != "" {
				t.Errorf("identifyType() details = %q, want empty", gotDetails)
			}
		})
	}
}

func TestIsIntegerString(t *testing.T) {
	tests := []struct {
		input []byte
		want  bool
	}{
		{[]byte("123"), true},
		{[]byte("-456"), true},
		{[]byte("+789"), true},
		{[]byte("0"), true},
		{[]byte("-0"), true},
		{[]byte(""), false},
		{[]byte("-"), false},
		{[]byte("+"), false},
		{[]byte("12.34"), false},
		{[]byte("abc"), false},
		{[]byte("12abc"), false},
		{[]byte(" 123"), false},
		{[]byte("123 "), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			got := isIntegerString(tt.input)
			if got != tt.want {
				t.Errorf("isIntegerString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
