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

package ouroboros

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestValidateLeiosEndorserBlockForHeaderExpiredMissingDoesNotWait(
	t *testing.T,
) {
	t.Parallel()
	tx := testDijkstraTx(t, 0)
	raw, err := cbor.Encode(
		&lcommon.LeiosEndorserBlock{
			TransactionReferences: []lcommon.LeiosTransactionReference{
				{
					TransactionHash: lcommon.Blake2b256Hash(tx),
					TransactionSize: uint32(len(tx)),
				},
			},
		},
	)
	require.NoError(t, err)
	point := ocommon.NewPoint(73, lcommon.Blake2b256Hash(raw).Bytes())
	header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(
		testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			testEbHash(point),
			uint64(len(raw)),
		),
	)
	require.NoError(t, err)
	view := &fakeLeiosAnnouncementLedger{
		slotTime: time.Now().Add(-leiosNotifyMaxAnnouncementAge - time.Minute),
	}
	o := newOuroboros(
		OuroborosConfig{
			EnableLeios:             true,
			LeiosAnnouncementLedger: view,
			LeiosTxFetchTailBudget:  time.Minute,
		},
	)
	defer func() { require.NoError(t, o.Close()) }()
	votes := &fakeLeiosVoteHandler{}
	o.leiosVotes = votes
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(point, raw, nil, leiosStorePeerOffered),
	)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- o.ValidateLeiosEndorserBlockForHeader(ctx, header, ledger.LeiosEndorserBlockBasisHash(header))
	}()
	var returned, registered bool
	var result error
	testutil.WaitForCondition(t, func() bool {
		select {
		case result = <-done:
			returned = true
			return true
		default:
		}
		o.leiosMu.RLock()
		registered = len(
			o.leiosClosureWaiters[leiosBlockKey(point.Slot, point.Hash)],
		) > 0
		o.leiosMu.RUnlock()
		return registered
	}, testutil.AsyncWait, "expired announcement returns or starts a closure wait")
	cancel()
	if !returned {
		result = testutil.RequireReceive(
			t,
			done,
			testutil.AsyncWait,
			"cancel closure wait after observing the decision",
		)
	}
	require.False(
		t,
		registered,
		"expired missing announcement must not park the ledger in a passive closure wait",
	)
	require.ErrorContains(t, result, "expired announcement")
	require.Zero(t, view.txValidated)
	votes.mu.Lock()
	published := len(votes.ebs)
	votes.mu.Unlock()
	require.Zero(t, published)
	o.leiosMu.RLock()
	waiters := len(o.leiosClosureWaiters)
	status := o.leiosEndorserBlocks[leiosBlockKey(point.Slot, point.Hash)].semanticValidationStatus
	o.leiosMu.RUnlock()
	require.Zero(t, waiters)
	require.NotEqual(t, leiosEBValidationValid, status)
	canceled, cancelMissing := context.WithCancel(t.Context())
	cancelMissing()
	require.ErrorIs(
		t,
		o.ValidateLeiosEndorserBlockForHeader(
			canceled,
			header,
			ledger.LeiosEndorserBlockBasisHash(header),
		),
		context.Canceled,
	)

	wrongHeader, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(
		testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			testEbHash(point),
			uint64(len(raw))+1,
		),
	)
	require.NoError(t, err)
	require.ErrorIs(
		t,
		o.ValidateLeiosEndorserBlockForHeader(
			t.Context(),
			wrongHeader,
			ledger.LeiosEndorserBlockBasisHash(wrongHeader),
		),
		ledger.ErrInvalidLeiosEndorserBlockTransactions,
		"expiry must not hide an invalid manifest binding",
	)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			raw,
			[]cbor.RawMessage{tx},
			leiosStorePeerOffered,
		),
	)
	require.NoError(
		t,
		o.ValidateLeiosEndorserBlockForHeader(
			t.Context(),
			header,
			ledger.LeiosEndorserBlockBasisHash(header),
		),
	)
	require.Equal(
		t,
		1,
		view.txValidated,
		"expired complete content must still undergo semantic validation",
	)
}

func TestValidateLeiosEndorserBlockForHeaderExpiredPendingKeepsBudget(
	t *testing.T,
) {
	t.Parallel()
	for _, terminal := range []uint8{leiosEBValidationValid, leiosEBValidationInvalid} {
		t.Run(
			map[uint8]string{leiosEBValidationValid: "valid", leiosEBValidationInvalid: "invalid"}[terminal],
			func(t *testing.T) {
				t.Parallel()
				tx := testDijkstraTx(t, 0)
				raw, err := cbor.Encode(
					&lcommon.LeiosEndorserBlock{
						TransactionReferences: []lcommon.LeiosTransactionReference{
							{
								TransactionHash: lcommon.Blake2b256Hash(tx),
								TransactionSize: uint32(len(tx)),
							},
						},
					},
				)
				require.NoError(t, err)
				point := ocommon.NewPoint(
					74,
					lcommon.Blake2b256Hash(raw).Bytes(),
				)
				header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(
					testDijkstraAnnouncementHeaderRawFor(
						t,
						point.Slot,
						testEbHash(point),
						uint64(len(raw)),
					),
				)
				require.NoError(t, err)
				view := &fakeLeiosAnnouncementLedger{
					slotTime: time.Now().
						Add(-leiosNotifyMaxAnnouncementAge - time.Minute),
				}
				o := newOuroboros(
					OuroborosConfig{
						EnableLeios:             true,
						LeiosAnnouncementLedger: view,
						LeiosTxFetchTailBudget:  20 * time.Millisecond,
					},
				)
				defer func() { require.NoError(t, o.Close()) }()
				require.NoError(
					t,
					o.storeLeiosEndorserBlock(
						point,
						raw,
						[]cbor.RawMessage{tx},
						leiosStorePeerOffered,
					),
				)
				basis := ledger.LeiosEndorserBlockBasisHash(header)
				key := leiosBlockKey(point.Slot, point.Hash)
				done := make(chan struct{})
				o.leiosMu.Lock()
				current := o.leiosEndorserBlocks[key]
				pending := *current
				pending.semanticValidationStatus = leiosEBValidationPending
				pending.semanticValidationParent = append([]byte(nil), basis...)
				pending.semanticValidationDone = done
				for _, cacheKey := range current.cacheKeys {
					if o.leiosEndorserBlocks[cacheKey] == current {
						o.leiosEndorserBlocks[cacheKey] = &pending
					}
				}
				o.leiosMu.Unlock()
				err = o.ValidateLeiosEndorserBlockForHeader(
					t.Context(),
					header,
					basis,
				)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.ErrorContains(
					t,
					err,
					"wait for Leios endorser validation",
				)
				require.Zero(
					t,
					view.txValidated,
					"pending semantic validation must not be bypassed or duplicated",
				)
				o.updateLeiosEndorserBlockValidation(key, basis, done, terminal)
				err = o.ValidateLeiosEndorserBlockForHeader(
					t.Context(),
					header,
					basis,
				)
				if terminal == leiosEBValidationInvalid {
					require.ErrorIs(
						t,
						err,
						ledger.ErrInvalidLeiosEndorserBlockTransactions,
					)
				} else {
					require.NoError(t, err)
				}
			},
		)
	}
}

func TestValidateLeiosEndorserBlockForHeaderPreservesUnexpiredWait(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		offset   time.Duration
		zero     bool
		clockErr error
	}{
		{name: "fresh"},
		{name: "future", offset: time.Hour},
		{name: "unknown", offset: -time.Hour, clockErr: errors.New("slot time unavailable")},
		{name: "zero", zero: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			start := time.Time{}
			if !tc.zero {
				start = time.Now().Add(tc.offset)
			}
			point, raw := testLeiosEndorserBlockRaw(t, 75)
			header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(
				testDijkstraAnnouncementHeaderRawFor(
					t,
					point.Slot,
					testEbHash(point),
					uint64(len(raw)),
				),
			)
			require.NoError(t, err)
			o := newOuroboros(
				OuroborosConfig{
					EnableLeios: true,
					LeiosAnnouncementLedger: &fakeLeiosAnnouncementLedger{
						slotTime:    start,
						slotTimeErr: tc.clockErr,
					},
					LeiosTxFetchTailBudget: time.Minute,
				},
			)
			defer func() { require.NoError(t, o.Close()) }()
			require.NoError(
				t,
				o.storeLeiosEndorserBlock(
					point,
					raw,
					nil,
					leiosStorePeerOffered,
				),
			)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- o.ValidateLeiosEndorserBlockForHeader(ctx, header, ledger.LeiosEndorserBlockBasisHash(header))
			}()
			var registered, returned bool
			var result error
			testutil.WaitForCondition(t, func() bool {
				select {
				case result = <-done:
					returned = true
					return true
				default:
				}
				o.leiosMu.RLock()
				registered = len(
					o.leiosClosureWaiters[leiosBlockKey(point.Slot, point.Hash)],
				) > 0
				o.leiosMu.RUnlock()
				return registered
			}, testutil.AsyncWait, "unexpired or unknown-time announcement starts its closure wait")
			cancel()
			if !returned {
				result = testutil.RequireReceive(
					t,
					done,
					testutil.AsyncWait,
					"cancel the observed closure wait",
				)
			}
			require.True(
				t,
				registered,
				"unexpired or unknown-time announcements must keep the diffusion wait",
			)
			require.ErrorIs(t, result, context.Canceled)
		})
	}
}

func TestValidateLeiosEndorserBlockForHeaderExpiredReusesCachedBody(
	t *testing.T,
) {
	t.Parallel()
	tx := testDijkstraTx(t, 0)
	raw, err := cbor.Encode(
		&lcommon.LeiosEndorserBlock{
			TransactionReferences: []lcommon.LeiosTransactionReference{
				{
					TransactionHash: lcommon.Blake2b256Hash(tx),
					TransactionSize: uint32(len(tx)),
				},
			},
		},
	)
	require.NoError(t, err)
	cached := ocommon.NewPoint(76, lcommon.Blake2b256Hash(raw).Bytes())
	selected := ocommon.NewPoint(77, cached.Hash)
	header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(
		testDijkstraAnnouncementHeaderRawFor(
			t,
			selected.Slot,
			testEbHash(selected),
			uint64(len(raw)),
		),
	)
	require.NoError(t, err)
	view := &fakeLeiosAnnouncementLedger{
		slotTime: time.Now().Add(-leiosNotifyMaxAnnouncementAge - time.Minute),
	}
	o := newOuroboros(
		OuroborosConfig{
			EnableLeios:             true,
			LeiosAnnouncementLedger: view,
			LeiosTxFetchTailBudget:  time.Minute,
		},
	)
	defer func() { require.NoError(t, o.Close()) }()
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			cached,
			raw,
			[]cbor.RawMessage{tx},
			leiosStoreAuthoritative,
		),
	)
	require.NoError(
		t,
		o.ValidateLeiosEndorserBlockForHeader(
			t.Context(),
			header,
			ledger.LeiosEndorserBlockBasisHash(header),
		),
	)
	require.Equal(t, 1, view.txValidated)
	data, found := o.lookupLeiosEndorserBlock(selected.Slot, selected.Hash)
	require.True(t, found)
	require.True(t, data.completeTxCache())
	require.True(t, data.slotVerified)
	require.Equal(t, selected.Slot, data.point.Slot)
}
