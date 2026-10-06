//go:build linux && devnet && !devnet_conformance

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

package scenarios

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestAwaitLeiosProducerOfferRequiresMatchingStreams(t *testing.T) {
	t.Parallel()

	point := pcommon.NewPoint(42, []byte("endorser-block"))
	other := pcommon.NewPoint(43, []byte("other-endorser-block"))
	tests := map[string][]struct {
		manifest    *pcommon.Point
		transaction *pcommon.Point
	}{
		"manifest before transaction": {
			{manifest: &point},
			{transaction: &point},
		},
		"transaction before manifest": {
			{transaction: &point},
			{manifest: &point},
		},
		"different points do not match": {
			{manifest: &point},
			{transaction: &other},
			{transaction: &point},
		},
	}
	for name, sequence := range tests {
		t.Run(name, func(t *testing.T) {
			matcher := leiosProducerOfferMatcher{}
			manifests := make(chan pcommon.Point)
			transactions := make(chan pcommon.Point)
			result := make(chan struct {
				point pcommon.Point
				err   error
			}, 1)
			go func() {
				got, err := matcher.await(
					context.Background(),
					manifests,
					transactions,
				)
				result <- struct {
					point pcommon.Point
					err   error
				}{point: got, err: err}
			}()

			for i, offer := range sequence {
				switch {
				case offer.manifest != nil:
					manifests <- *offer.manifest
				case offer.transaction != nil:
					transactions <- *offer.transaction
				default:
					require.FailNow(t, "empty offer step")
				}
				if i < len(sequence)-1 {
					select {
					case premature := <-result:
						require.FailNow(t, fmt.Sprintf(
							"matched before both exact offers: %+v",
							premature,
						))
					default:
					}
				}
			}

			got := <-result
			require.NoError(t, got.err)
			require.Equal(t, point, got.point)
		})
	}
}

func TestLeiosProducerOfferMatcherRetainsFutureCandidate(t *testing.T) {
	t.Parallel()

	matcher := leiosProducerOfferMatcher{}
	first := pcommon.NewPoint(42, []byte("first-endorser-block"))
	future := pcommon.NewPoint(43, []byte("future-endorser-block"))
	manifests := make(chan pcommon.Point)
	transactions := make(chan pcommon.Point)
	result := make(chan struct {
		point pcommon.Point
		err   error
	}, 1)
	go func() {
		point, err := matcher.await(
			context.Background(),
			manifests,
			transactions,
		)
		result <- struct {
			point pcommon.Point
			err   error
		}{point: point, err: err}
	}()
	manifests <- future
	transactions <- first
	manifests <- first
	firstResult := <-result
	require.NoError(t, firstResult.err)
	require.Equal(t, first, firstResult.point)

	go func() {
		point, err := matcher.await(
			context.Background(),
			manifests,
			transactions,
		)
		result <- struct {
			point pcommon.Point
			err   error
		}{point: point, err: err}
	}()
	transactions <- future
	futureResult := <-result
	require.NoError(t, futureResult.err)
	require.Equal(t, future, futureResult.point)
}

func TestAwaitLeiosApplicationStopsAfterReferenceRollback(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := awaitLeiosApplication(
		ctx,
		func(context.Context) (bool, error) { return false, nil },
		func() bool { return false },
	)
	require.ErrorIs(t, err, errLeiosReferenceRolledBack)
}

func TestAwaitLeiosApplicationFailsIfRetainedReferenceNeverApplies(
	t *testing.T,
) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := awaitLeiosApplication(
		ctx,
		func(context.Context) (bool, error) { return false, nil },
		func() bool { return true },
	)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, errors.Is(err, errLeiosReferenceRolledBack))
}

func TestAwaitLeiosApplicationRejectsAppliedRolledBackReference(
	t *testing.T,
) {
	t.Parallel()

	checks := 0
	appliedCalled := false
	err := awaitLeiosApplication(
		context.Background(),
		func(context.Context) (bool, error) {
			appliedCalled = true
			return true, nil
		},
		func() bool {
			checks++
			return checks == 1
		},
	)
	require.ErrorIs(t, err, errLeiosReferenceRolledBack)
	require.True(t, appliedCalled)
	require.Equal(t, 2, checks)
}

func TestAwaitLeiosTransactionOfferStopsAfterReferenceRollback(t *testing.T) {
	t.Parallel()

	matcher := leiosTransactionOfferMatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := matcher.await(
		ctx,
		make(chan pcommon.Point),
		make(chan error),
		pcommon.NewPoint(42, []byte("endorser-block")),
		func() bool { return false },
	)
	require.ErrorIs(t, err, errLeiosReferenceRolledBack)
}

func TestAwaitLeiosTransactionOfferRejectsOfferAfterReferenceRollback(
	t *testing.T,
) {
	t.Parallel()

	matcher := leiosTransactionOfferMatcher{}
	want := pcommon.NewPoint(42, []byte("endorser-block"))
	offers := make(chan pcommon.Point, 1)
	offers <- want
	checks := 0
	err := matcher.await(
		context.Background(),
		offers,
		make(chan error),
		want,
		func() bool {
			checks++
			return checks == 1
		},
	)
	require.ErrorIs(t, err, errLeiosReferenceRolledBack)
	require.Equal(t, 2, checks)
}

func TestLeiosTransactionOfferMatcherRetainsFutureOffer(t *testing.T) {
	t.Parallel()

	matcher := leiosTransactionOfferMatcher{}
	future := pcommon.NewPoint(43, []byte("future-endorser-block"))
	offers := make(chan pcommon.Point)
	errorsCh := make(chan error)
	result := make(chan error, 1)
	go func() {
		result <- matcher.await(
			context.Background(),
			offers,
			errorsCh,
			pcommon.NewPoint(42, []byte("first-endorser-block")),
			func() bool { return true },
		)
	}()
	offers <- future
	close(offers)
	require.ErrorContains(t, <-result, "transaction offer stream closed")

	require.NoError(t, matcher.await(
		context.Background(),
		make(chan pcommon.Point),
		make(chan error),
		future,
		func() bool { return true },
	))
}

func TestLeiosTransactionOfferMatcherRejectsClosedErrorStream(t *testing.T) {
	t.Parallel()

	matcher := leiosTransactionOfferMatcher{}
	errorsCh := make(chan error)
	close(errorsCh)
	err := matcher.await(
		context.Background(),
		make(chan pcommon.Point),
		errorsCh,
		pcommon.NewPoint(42, []byte("endorser-block")),
		func() bool { return true },
	)
	require.ErrorContains(t, err, "error stream closed")
}

func TestLeiosReferenceOnCanonicalChainRejectsReplacement(t *testing.T) {
	t.Parallel()

	reference := devnet.ObservedHeader{
		Slot: 42,
		Hash: []byte("referencing-block"),
	}
	replacement := devnet.ObservedHeader{
		Slot: reference.Slot,
		Hash: []byte("replacement-block"),
	}
	snapshots := []devnet.ChainSnapshot{
		{
			Node:    "relay",
			Headers: []devnet.ObservedHeader{replacement},
		},
	}

	require.False(t, leiosReferenceOnCanonicalChain(
		snapshots,
		"relay",
		reference,
	))
}

func TestLeiosReferencingHeader(t *testing.T) {
	t.Parallel()

	point := pcommon.NewPoint(42, []byte("repeated-endorser-block"))
	old := devnet.ObservedHeader{
		Slot:                  point.Slot - 1,
		Hash:                  []byte("old-ranking-block"),
		LeiosAnnouncementHash: point.Hash,
	}
	want := devnet.ObservedHeader{
		Slot:                  point.Slot + 1,
		Hash:                  []byte("current-ranking-block"),
		LeiosAnnouncementHash: point.Hash,
	}
	tests := map[string]struct {
		headers []devnet.ObservedHeader
		want    devnet.ObservedHeader
		ok      bool
	}{
		"older repeated hash is skipped": {
			headers: []devnet.ObservedHeader{old},
		},
		"eligible later header is selected": {
			headers: []devnet.ObservedHeader{old, want},
			want:    want,
			ok:      true,
		},
		"rolled back header is absent": {},
		"wrong hash is rejected": {
			headers: []devnet.ObservedHeader{
				{
					Slot:                  point.Slot + 1,
					Hash:                  []byte("wrong-ranking-block"),
					LeiosAnnouncementHash: []byte("different-endorser-block"),
				},
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			snapshots := []devnet.ChainSnapshot{
				{
					Node:    "relay",
					Headers: test.headers,
				},
			}
			got, ok := leiosReferencingHeader(snapshots, "relay", point)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.want, got)
		})
	}
}
