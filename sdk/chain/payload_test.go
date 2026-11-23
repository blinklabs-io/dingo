// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chain_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/sdk/chain"
)

type nativeValue struct{ id int }

func TestPayloadNativeFastPathSkipsEncoding(t *testing.T) {
	t.Parallel()

	want := &nativeValue{id: 7}
	encodes := 0
	p := chain.NewNativePayload(
		chain.TxView{Type: 6, Hash: []byte{1}},
		want,
		func() ([]byte, error) {
			encodes++
			return []byte{0xde, 0xad}, nil
		},
	)

	got, ok := chain.NativeOf[*nativeValue](p)
	if !ok || got != want {
		t.Fatalf("NativeOf = %v, %v; want the sender's value", got, ok)
	}
	if encodes != 0 {
		t.Fatalf("reading the native value encoded %d times; want 0", encodes)
	}
	if p.View().Type != 6 {
		t.Fatalf("view type = %d; want 6", p.View().Type)
	}
}

func TestPayloadBytesEncodesOnDemand(t *testing.T) {
	t.Parallel()

	encodes := 0
	p := chain.NewNativePayload(
		chain.TxView{},
		&nativeValue{},
		func() ([]byte, error) {
			encodes++
			return []byte{0xde, 0xad}, nil
		},
	)
	for i := 1; i <= 2; i++ {
		raw, err := p.Bytes()
		if err != nil {
			t.Fatalf("Bytes: %v", err)
		}
		if !bytes.Equal(raw, []byte{0xde, 0xad}) {
			t.Fatalf("Bytes = %x", raw)
		}
		if encodes != i {
			t.Fatalf("after %d Bytes calls encodes = %d", i, encodes)
		}
	}
}

func TestPayloadFromBytesHasNoNativeValue(t *testing.T) {
	t.Parallel()

	raw := []byte{1, 2, 3}
	p := chain.NewPayload(chain.BlockView{}, raw)

	if p.Native() != nil {
		t.Fatalf("Native = %v; want nil", p.Native())
	}
	if _, ok := chain.NativeOf[*nativeValue](p); ok {
		t.Fatal("NativeOf reported a value for a bytes-only payload")
	}
	got, err := p.Bytes()
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("Bytes = %x, %v; want %x", got, err, raw)
	}
}

func TestPayloadNativeOfRejectsOtherType(t *testing.T) {
	t.Parallel()

	p := chain.NewNativePayload[chain.TxView](
		chain.TxView{}, &nativeValue{}, nil,
	)
	if _, ok := chain.NativeOf[string](p); ok {
		t.Fatal("NativeOf matched a value of another type")
	}
}

func TestPayloadWithoutEncodingFails(t *testing.T) {
	t.Parallel()

	p := chain.NewNativePayload[chain.TxView](
		chain.TxView{}, &nativeValue{}, nil,
	)
	if _, err := p.Bytes(); !errors.Is(err, chain.ErrNoEncoding) {
		t.Fatalf("Bytes error = %v; want ErrNoEncoding", err)
	}
}

func TestPayloadEmptyEncodingFails(t *testing.T) {
	t.Parallel()

	for _, empty := range [][]byte{nil, {}} {
		p := chain.NewNativePayload(
			chain.TxView{},
			&nativeValue{},
			func() ([]byte, error) { return empty, nil },
		)
		if raw, err := p.Bytes(); !errors.Is(err, chain.ErrNoEncoding) {
			t.Fatalf("Bytes = %x, %v; want ErrNoEncoding", raw, err)
		}
	}
	p := chain.NewPayload(chain.TxView{}, []byte{})
	if raw, err := p.Bytes(); !errors.Is(err, chain.ErrNoEncoding) {
		t.Fatalf("empty raw Bytes = %x, %v; want ErrNoEncoding", raw, err)
	}
}
