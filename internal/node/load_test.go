package node

import (
	"bytes"
	"testing"

	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	fxcbor "github.com/fxamacker/cbor/v2"
)

func TestExtractHeaderCbor(t *testing.T) {
	t.Parallel()

	header := gcbor.RawMessage{0x01}
	body := gcbor.RawMessage{0x02}
	blockCbor, err := fxcbor.Marshal([]gcbor.RawMessage{header, body})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	got, err := extractHeaderCbor(blockCbor)
	if err != nil {
		t.Fatalf("extractHeaderCbor returned error: %v", err)
	}
	if !bytes.Equal(got, header) {
		t.Fatalf("unexpected header bytes: got %x want %x", got, header)
	}
}

func TestCborArrayHeaderLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		want    int
		wantErr bool
	}{
		{
			name: "small definite",
			data: []byte{gcbor.CborTypeArray + 2, 0x01, 0x02},
			want: 1,
		},
		{
			name: "uint8 length",
			data: []byte{gcbor.CborTypeArray + 24, 0x20},
			want: 2,
		},
		{
			name: "uint16 length",
			data: []byte{gcbor.CborTypeArray + 25, 0x01, 0x00},
			want: 3,
		},
		{
			name: "uint32 length",
			data: []byte{gcbor.CborTypeArray + 26, 0x00, 0x01, 0x00, 0x00},
			want: 5,
		},
		{
			name: "uint64 length",
			data: []byte{gcbor.CborTypeArray + 27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00},
			want: 9,
		},
		{
			name: "indefinite",
			data: []byte{gcbor.CborTypeArray + 31, 0x01, 0xff},
			want: 1,
		},
		{
			name:    "empty input",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "non-array major type",
			data:    []byte{gcbor.CborTypeMap + 2},
			wantErr: true,
		},
		{
			name:    "truncated uint8 header",
			data:    []byte{gcbor.CborTypeArray + 24},
			wantErr: true,
		},
		{
			name:    "truncated uint16 header",
			data:    []byte{gcbor.CborTypeArray + 25, 0x01},
			wantErr: true,
		},
		{
			name:    "truncated uint32 header",
			data:    []byte{gcbor.CborTypeArray + 26, 0x00, 0x01},
			wantErr: true,
		},
		{
			name:    "truncated uint64 header",
			data:    []byte{gcbor.CborTypeArray + 27, 0x00, 0x00, 0x00},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := cborArrayHeaderLen(test.data)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("cborArrayHeaderLen returned error: %v", err)
			}
			if got != test.want {
				t.Fatalf("unexpected header len: got %d want %d", got, test.want)
			}
		})
	}
}
