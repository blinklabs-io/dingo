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

package utxorpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
	submit "github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
)

func TestWaitForTxRejectsMalformedReferencesBeforeWork(t *testing.T) {
	for _, size := range []int{0, 31, 33} {
		for _, index := range []int{0, 1} {
			t.Run(
				fmt.Sprintf("size_%d/index_%d", size, index),
				func(t *testing.T) {
					eventBus := newControlledWaitForTxEventBus()
					var logs bytes.Buffer
					lookups := 0
					u := NewUtxorpc(UtxorpcConfig{
						Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
						EventBus: eventBus,
						LedgerState: &waitForTxLedgerStub{
							transactionByHash: func([]byte) (*models.Transaction, error) {
								lookups++
								return nil, errors.New("unexpected lookup")
							},
						},
					})
					refs := make([][]byte, 0, index+1)
					if index == 1 {
						refs = append(refs, bytes.Repeat([]byte{0x42}, 32))
					}
					refs = append(refs, make([]byte, size))
					server := &submitServiceServer{utxorpc: u}
					err := server.WaitForTx(
						context.Background(),
						connect.NewRequest(&submit.WaitForTxRequest{Ref: refs}),
						nil,
					)
					select {
					case <-eventBus.subscribed:
						t.Fatal(
							"malformed reference must be rejected before subscription",
						)
					default:
					}
					require.Zero(t, lookups)
					require.Empty(
						t,
						logs.String(),
						"malformed references must be rejected before logging",
					)
					require.Equal(
						t,
						connect.CodeInvalidArgument,
						connect.CodeOf(err),
					)
					require.ErrorContains(
						t,
						err,
						fmt.Sprintf(
							"transaction reference at index %d must be 32 bytes, got %d",
							index,
							size,
						),
					)
				},
			)
		}
	}
}

func TestWaitForTxReferenceAdmissionControls(t *testing.T) {
	for _, refs := range [][][]byte{nil, {}, {bytes.Repeat([]byte{0x42}, 32)}} {
		t.Run(
			fmt.Sprintf("references_%d_nil_%t", len(refs), refs == nil),
			func(t *testing.T) {
				eventBus := newControlledWaitForTxEventBus()
				lookupErr := errors.New("ledger lookup reached")
				lookups := 0
				u := NewUtxorpc(UtxorpcConfig{
					EventBus: eventBus,
					LedgerState: &waitForTxLedgerStub{
						transactionByHash: func(hash []byte) (*models.Transaction, error) {
							lookups++
							require.Equal(t, refs[0], hash)
							return nil, lookupErr
						},
					},
				})
				server := &submitServiceServer{utxorpc: u}
				err := server.WaitForTx(
					context.Background(),
					connect.NewRequest(&submit.WaitForTxRequest{Ref: refs}),
					nil,
				)
				if len(refs) == 0 {
					require.NoError(t, err)
					require.Zero(t, lookups)
					select {
					case <-eventBus.subscribed:
						t.Fatal("empty reference list must not subscribe")
					default:
					}
				} else {
					require.ErrorIs(t, err, lookupErr)
					require.Equal(t, 1, lookups, "valid reference must reach the existing lookup")
				}
			},
		)
	}
}
