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

package chain

import "errors"

// ErrNoEncoding is returned by Payload.Bytes when the payload holds no encoded
// bytes and no encoder, or its encoder produced no bytes.
var ErrNoEncoding = errors.New("chain: payload has no encoded form")

// HeaderView is the decoded view of a header every role can read without
// understanding the chain's encoding.
type HeaderView struct {
	// Type is the chain's own discriminator for the encoding, such as the
	// Cardano block type. A decoder needs it to read the bytes back.
	Type     uint
	Hash     []byte
	PrevHash []byte
	Slot     uint64
	Number   uint64
}

// BlockView is the decoded view of a block.
type BlockView struct {
	HeaderView
	TxCount int
}

// TxView is the decoded view of a transaction.
type TxView struct {
	// Type is the chain's own discriminator for the encoding, such as the
	// Cardano transaction era.
	Type uint
	Hash []byte
}

// Payload is a block, header or transaction in transit between roles. It
// carries a view V that any role can read, and either or both of two forms of
// the full value: its encoded bytes, and the chain's own decoded value.
//
// A role built into the same binary as the sender reads the decoded value with
// NativeOf and never touches the bytes. A role outside it calls Bytes, which
// encodes only if the sender supplied no bytes. A Payload is immutable; its
// slices and native value must not be modified after construction.
type Payload[V any] struct {
	view   V
	raw    []byte
	native any
	encode func() ([]byte, error)
}

// Block is a block payload.
type Block = Payload[BlockView]

// Header is a block header payload.
type Header = Payload[HeaderView]

// Tx is a transaction payload.
type Tx = Payload[TxView]

// NewPayload returns a Payload that holds only encoded bytes, as one received
// from an external role.
func NewPayload[V any](view V, raw []byte) Payload[V] {
	return Payload[V]{view: view, raw: raw}
}

// NewNativePayload returns a Payload that holds the chain's decoded value.
// encode produces the bytes on demand and is called once per Bytes call, so it
// should return the value's own retained encoding when it has one. A nil
// encode, or one that returns no bytes, makes Bytes return ErrNoEncoding.
func NewNativePayload[V any](
	view V,
	native any,
	encode func() ([]byte, error),
) Payload[V] {
	return Payload[V]{view: view, native: native, encode: encode}
}

// View returns the decoded view.
func (p Payload[V]) View() V {
	return p.view
}

// Native returns the chain's decoded value, or nil when the payload holds only
// bytes.
func (p Payload[V]) Native() any {
	return p.native
}

// Bytes returns the encoded form. It returns the bytes the payload was built
// with, and otherwise encodes the native value. It never returns empty bytes
// without an error: no value has an empty encoding.
func (p Payload[V]) Bytes() ([]byte, error) {
	if len(p.raw) > 0 {
		return p.raw, nil
	}
	if p.encode == nil {
		return nil, ErrNoEncoding
	}
	raw, err := p.encode()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrNoEncoding
	}
	return raw, nil
}

// NativeOf returns the payload's decoded value as a T. ok is false when the
// payload holds only bytes or the value is of another type, in which case the
// caller decodes the bytes.
func NativeOf[T, V any](p Payload[V]) (value T, ok bool) {
	value, ok = p.native.(T)
	return value, ok
}
