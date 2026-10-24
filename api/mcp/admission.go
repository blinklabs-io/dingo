// Copyright 2025 Blink Labs Software
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

package mcp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
)

// maxCardanoPayloadBytes caps a caller-supplied transaction or transaction
// output, as raw CBOR bytes. A transaction is limited by the MaxTxSize
// protocol parameter (16 KiB on mainnet) and an output lives inside a
// transaction, so one cap serves both. It leaves eight times the mainnet
// limit as headroom for a parameter change or a test network, while keeping
// what a caller can make the node decode small.
const maxCardanoPayloadBytes = 128 << 10

// maxConcurrentPayloadParses bounds the output CBOR decodes that may run at
// once on one server.
const maxConcurrentPayloadParses = 4

var errParseBusy = errors.New(
	"too many concurrent payload parses; retry after one completes",
)

// checkEncodedPayloadSize rejects hex or base64 text too long to decode to
// maxCardanoPayloadBytes or fewer bytes, before any of it is decoded. Hex is
// the longest encoding of a byte, so text longer than its encoding of the
// cap cannot fit under either encoding.
func checkEncodedPayloadSize(what, text string) error {
	if limit := hex.EncodedLen(maxCardanoPayloadBytes); len(text) > limit {
		return fmt.Errorf(
			"%s is too large: %d encoded characters exceeds the limit of %d",
			what,
			len(text),
			limit,
		)
	}
	return nil
}

// checkDecodedPayloadSize rejects a payload of more than
// maxCardanoPayloadBytes decoded bytes. The encoded-size check bounds what is
// decoded, so this only has to enforce the exact limit.
func checkDecodedPayloadSize(what string, size int) error {
	if size > maxCardanoPayloadBytes {
		return fmt.Errorf(
			"%s is too large: %d bytes exceeds the limit of %d",
			what,
			size,
			maxCardanoPayloadBytes,
		)
	}
	return nil
}

// runAdmitted runs work in its own goroutine once it holds a slot of gate, so
// a caller-supplied payload is never parsed outside the slot budget or the
// request deadline. It refuses with busy when no slot is free instead of
// waiting. ctx cancels the work and ends the wait, but the slot is held until
// work returns, so an abandoned call cannot let work pile up in the
// background. A panic in work is returned as an error naming label.
func runAdmitted[T any](
	ctx context.Context,
	gate chan struct{},
	busy error,
	label string,
	work func(context.Context) (T, error),
) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	select {
	case gate <- struct{}{}:
	default:
		return zero, busy
	}
	type response struct {
		value T
		err   error
	}
	done := make(chan response, 1)
	go func() {
		var res response
		defer func() {
			<-gate
			if recovered := recover(); recovered != nil {
				res.err = fmt.Errorf("%s panicked: %v", label, recovered)
			}
			done <- res
		}()
		if err := ctx.Err(); err != nil {
			res.err = err
			return
		}
		res.value, res.err = work(ctx)
	}()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case res := <-done:
		return res.value, res.err
	}
}
