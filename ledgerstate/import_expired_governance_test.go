// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledgerstate

import (
	"bytes"
	"context"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"io"
	"log/slog"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	common "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func expiredGovStateFixture(
	t *testing.T,
	expires uint64,
	expired any,
) ([]byte, []byte, []byte) {
	t.Helper()
	hash := bytes.Repeat([]byte{0x65}, 32)
	stake := bytes.Repeat([]byte{0x66}, 28)
	proposal := []any{
		[]any{
			hash,
			uint64(0),
		}, map[uint64]uint64{}, map[uint64]uint64{}, map[uint64]uint64{},
		[]any{
			uint64(100_000_000_000),
			append([]byte{0xe0}, stake...),
			[]any{uint8(6)},
			[]any{
				"https://example.com/proposal",
				bytes.Repeat([]byte{0x67}, 32),
			},
		},
		uint64(4), expires,
	}
	pulsing := []any{
		[]any{
			[]any{},
			map[uint64]uint64{},
			map[uint64]uint64{},
			map[uint64]uint64{},
		},
		[]any{
			[]any{snothingCommittee(), nil, nil, nil, nil, nil, nil},
			[]any{},
			expired,
			false,
		},
	}
	return govStateWithRootsAndProposals(
		t,
		[4]*ParsedGovActionId{},
		false,
		[]any{proposal},
		pulsing,
	), hash, stake
}

func TestImportExpiredGovStateRefundsAtFirstPostAnchorBoundary(t *testing.T) {
	for _, expires := range []uint64{7, 9} {
		t.Run(string(rune('0'+expires)), func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
			hash := bytes.Repeat([]byte{0x65}, 32)
			set, err := cbor.Encode([]any{[]any{hash, uint64(0)}})
			require.NoError(t, err)
			data, hash, stake := expiredGovStateFixture(
				t,
				expires,
				cbor.RawTag{Number: 258, Content: set},
			)
			require.NoError(
				t,
				db.Metadata().
					ImportAccount(&models.Account{StakingKey: stake, AddedSlot: 1000, Reward: 700, Active: true}, nil),
			)
			cfg := ImportConfig{
				Database: db,
				Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
				State: &RawLedgerState{
					GovStateData:  data,
					Epoch:         10,
					EraIndex:      EraConway,
					EraBoundEpoch: 0,
					EraBoundSlot:  0,
				},
				EpochLength: func(uint) (uint, uint, error) { return 1, 100, nil },
			}
			for range 2 {
				require.NoError(
					t,
					importGovState(
						context.Background(),
						cfg,
						func(ImportProgress) {},
					),
				)
			}
			stored, err := db.Metadata().GetGovernanceProposal(hash, 0, nil)
			require.NoError(t, err)
			require.NotNil(t, stored)
			sameEpoch, err := db.GetExpiredAwaitingDropGovernanceProposals(
				t.Context(),
				10,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, sameEpoch)
			nextEpoch, err := db.GetExpiredAwaitingDropGovernanceProposals(
				t.Context(),
				11,
				nil,
			)
			require.NoError(t, err)
			txn := db.MetadataTxn(t.Context(), true)
			defer txn.Release()
			pp := &conway.ConwayProtocolParameters{}
			pp.ProtocolVersion.Major = 10
			_, err = governance.ProcessEpoch(
				t.Context(),
				&governance.EpochInput{
					DB:           dbadapter.New(db),
					Txn:          txn,
					PrevEpoch:    10,
					NewEpoch:     11,
					BoundarySlot: 1100,
					PParams:      pp,
					UpdateFn:     func(p common.ProtocolParameters, _ any) (common.ProtocolParameters, error) { return p, nil },
				},
			)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
			account, err := db.GetAccountByCredential(
				t.Context(),
				0,
				stake,
				true,
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, uint64(100_000_000_700), uint64(account.Reward))
			require.NotNil(
				t,
				stored.ExpiredEpoch,
				"a signed expiration verdict must survive import",
			)
			require.Equal(
				t,
				uint64(10),
				*stored.ExpiredEpoch,
				"the completed pulser belongs to the anchor epoch, not expires+1",
			)
			require.NotNil(t, stored.ExpiredSlot)
			require.Equal(t, uint64(1000), *stored.ExpiredSlot)
			require.Len(t, nextEpoch, 1)
			credits, err := db.Metadata().
				GetPostSnapshotRewardCreditEvents(1100, nil)
			require.NoError(t, err)
			require.Len(t, credits, 1)
			require.Equal(t, stake, credits[0].StakingKey)
			require.Equal(t, uint64(100_000_000_000), uint64(credits[0].Amount))
			require.True(t, credits[0].PostSnapshot)
			later, err := db.GetExpiredAwaitingDropGovernanceProposals(
				t.Context(),
				12,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, later)
		})
	}
}

func TestImportedExpiredGovStateRejectsMalformedVerdicts(t *testing.T) {
	hash := bytes.Repeat([]byte{0x65}, 32)
	id := []any{hash, uint64(0)}
	for _, tc := range []struct {
		name    string
		expired any
	}{
		{"null", nil},
		{"scalar", uint64(1)},
		{"map", map[uint64]uint64{}},
		{"short_id", []any{[]any{hash}}},
		{"long_id", []any{[]any{hash, uint64(0), uint64(0)}}},
		{"short_hash", []any{[]any{hash[:31], uint64(0)}}},
		{"negative_index", []any{[]any{hash, int64(-1)}}},
		{"overflow_index", []any{[]any{hash, uint64(math.MaxUint32) + 1}}},
		{"duplicate", []any{id, id}},
		{"tagged_id", []any{cbor.Tag{Number: 42, Content: id}}},
		{"wrong_tag", cbor.Tag{Number: 42, Content: []any{id}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data, _, _ := expiredGovStateFixture(t, 9, tc.expired)
			parsed, warning := ParseGovState(data, EraConway)
			require.Error(t, warning)
			require.NotNil(t, parsed)
			require.Error(t, parsed.PulsingStateParseError)
			_, _, err := parseImportedGovState(
				ImportConfig{
					State: &RawLedgerState{
						EraIndex:     EraConway,
						GovStateData: data,
						Epoch:        10,
					},
				},
			)
			require.ErrorContains(t, err, "pulsing state")
		})
	}
}

func TestImportedExpiredGovStateAcceptsEmptyVerdicts(t *testing.T) {
	t.Parallel()
	data, _, _ := expiredGovStateFixture(t, 11, []any{})
	state, warning := ParseGovState(data, EraConway)
	require.NoError(t, warning)
	require.NotNil(t, state)
	require.NoError(t, state.PulsingStateParseError)
}

func TestImportedExpiredGovStateRejectsEnactedOverlap(t *testing.T) {
	t.Parallel()
	hash := bytes.Repeat([]byte{0x65}, 32)
	data, _, _ := expiredGovStateFixture(t, 9, []any{[]any{hash, uint64(0)}})
	fields, err := decodeRawArray(data)
	require.NoError(t, err)
	proposals, err := decodeRawArray(fields[0])
	require.NoError(t, err)
	actions, err := decodeRawArray(proposals[1])
	require.NoError(t, err)
	require.Len(t, actions, 1)
	pulsing, err := decodeRawArray(fields[6])
	require.NoError(t, err)
	ratify, err := decodeRawArray(pulsing[1])
	require.NoError(t, err)
	ratify[1], err = cbor.Encode([]cbor.RawMessage{actions[0]})
	require.NoError(t, err)
	encodeFields := func(fields [][]byte) []byte {
		raw := make([]cbor.RawMessage, len(fields))
		for i, field := range fields {
			raw[i] = field
		}
		encoded, err := cbor.Encode(raw)
		require.NoError(t, err)
		return encoded
	}
	pulsing[1] = encodeFields(ratify)
	fields[6] = encodeFields(pulsing)
	data = encodeFields(fields)
	parsed, warning := ParseGovState(data, EraConway)
	require.ErrorContains(t, warning, "both enacted and expired")
	require.NotNil(t, parsed)
	require.Error(t, parsed.PulsingStateParseError)
}

func TestExpiredVerdictVetoesUniqueHFIFallback(t *testing.T) {
	t.Parallel()
	root := &ParsedGovActionId{TxHash: bytes.Repeat([]byte{0x71}, 32)}
	hash := bytes.Repeat([]byte{0x72}, 32)
	for _, tc := range []struct {
		name          string
		expired       bool
		ambiguous     bool
		wantCandidate bool
	}{
		{name: "unexpired", wantCandidate: true},
		{name: "expired", expired: true},
		{name: "ambiguous expired", expired: true, ambiguous: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proposals := []any{
				govActionStateForTest(
					hash,
					0,
					govActionTypeHardForkInitiation,
					root,
					4,
				),
			}
			if tc.ambiguous {
				proposals = append(
					proposals,
					govActionStateForTest(
						bytes.Repeat([]byte{0x73}, 32),
						0,
						govActionTypeHardForkInitiation,
						root,
						4,
					),
				)
			}
			expired := []any{}
			if tc.expired {
				expired = append(expired, []any{hash, uint64(0)})
			}
			pulsing := drepPulsingStateWithEnactCommittee(t, snothingCommittee()).([]any)
			pulsing[1].([]any)[2] = expired
			data := govStateWithRootsAndProposals(
				t,
				[4]*ParsedGovActionId{nil, root},
				false,
				proposals,
				pulsing,
			)
			state, err := ParseGovState(data, EraConway)
			require.NoError(t, err)
			state.RatifiedGovActionIds = nil
			if tc.wantCandidate {
				require.NotNil(t, findRatifiedHFICandidate(state))
			} else {
				require.Nil(t, findRatifiedHFICandidate(state))
			}
		})
	}
}
