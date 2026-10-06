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
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

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

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := awaitLeiosTransactionOffer(
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

	want := pcommon.NewPoint(42, []byte("endorser-block"))
	offers := make(chan pcommon.Point, 1)
	offers <- want
	checks := 0
	err := awaitLeiosTransactionOffer(
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
