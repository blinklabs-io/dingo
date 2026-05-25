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

package ledgerstate

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// govStateWithRoots builds a Conway GovState CBOR payload whose
// proposals container has the given per-purpose roots set and an
// empty OMap. The committee field is encoded based on
// committeePresent: true for a 1-element StrictMaybe Committee
// wrapper with a 2/3 quorum and empty members; false for SNothing.
//
// The shape mirrors testGovStateData but exposes the roots so
// tests can assert seeding behavior end to end.
func govStateWithRoots(
	t *testing.T,
	roots [4]*ParsedGovActionId,
	committeePresent bool,
) []byte {
	t.Helper()

	rootsAny := encodeRootsAsAny(t, roots)
	proposalsContainer := []any{rootsAny, []any{}}

	var committee any
	if committeePresent {
		// committee field is StrictMaybe (Committee era), encoded
		// as [ committee_body ] for SJust where committee_body =
		// [members_map, quorum]. Empty members + 2/3 quorum is
		// enough for parseCommittee to set CommitteeQuorum non-nil
		// — the seeding heuristic checks for CommitteeQuorum != nil
		// to distinguish UpdateCommittee from NoConfidence roots.
		committee = []any{
			[]any{
				map[any]uint64{},
				cbor.Rat{Rat: big.NewRat(2, 3)},
			},
		}
	} else {
		committee = []any{}
	}

	govState := []any{
		proposalsContainer,
		committee,
		// Constitution: [anchor, scriptHash], anchor=[url,hash].
		[]any{
			[]any{
				"https://example.com/constitution",
				bytes.Repeat([]byte{0xAA}, 32),
			},
			nil,
		},
	}
	data, err := cbor.Encode(govState)
	require.NoError(t, err)
	return data
}

func TestImportGovStateSeedsPrevGovActionIds(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	pp := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x11}, 32),
		ActionIndex: 0,
	}
	hf := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x22}, 32),
		ActionIndex: 0,
	}
	cm := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x33}, 32),
		ActionIndex: 0,
	}
	cn := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x44}, 32),
		ActionIndex: 0,
	}

	// committeePresent=false makes the committee root resolve to
	// NoConfidence (3); flipping it would make it UpdateCommittee
	// (4). We assert the action_type below.
	govStateData := govStateWithRoots(
		t,
		[4]*ParsedGovActionId{pp, hf, cm, cn},
		false,
	)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			GovStateData:  govStateData,
			Epoch:         500,
			EraIndex:      EraConway,
			EraBoundEpoch: 100,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	cases := []struct {
		name       string
		txHash     []byte
		actionIdx  uint32
		actionType uint8
		queryGroup []uint8
	}{
		{
			name:       "param-update",
			txHash:     pp.TxHash,
			actionIdx:  pp.ActionIndex,
			actionType: govActionTypeParameterChange,
			queryGroup: []uint8{govActionTypeParameterChange},
		},
		{
			name:       "hard-fork",
			txHash:     hf.TxHash,
			actionIdx:  hf.ActionIndex,
			actionType: govActionTypeHardForkInitiation,
			queryGroup: []uint8{govActionTypeHardForkInitiation},
		},
		{
			name:       "committee-no-confidence",
			txHash:     cm.TxHash,
			actionIdx:  cm.ActionIndex,
			actionType: govActionTypeNoConfidence,
			queryGroup: []uint8{
				govActionTypeNoConfidence,
				govActionTypeUpdateCommittee,
			},
		},
		{
			name:       "constitution",
			txHash:     cn.TxHash,
			actionIdx:  cn.ActionIndex,
			actionType: govActionTypeNewConstitution,
			queryGroup: []uint8{govActionTypeNewConstitution},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row, err := db.Metadata().GetGovernanceProposal(
				c.txHash, c.actionIdx, nil,
			)
			require.NoError(t, err)
			require.NotNil(t, row, "synthetic row missing for %s", c.name)
			assert.Equal(
				t, c.actionType, row.ActionType,
				"unexpected action_type for %s", c.name,
			)
			require.NotNil(t, row.EnactedEpoch, "EnactedEpoch unset for %s", c.name)
			require.NotNil(t, row.EnactedSlot, "EnactedSlot unset for %s", c.name)
			assert.Equal(
				t, uint64(0), row.AddedSlot,
				"AddedSlot must be 0 to survive rollback for %s", c.name,
			)

			// GetLastEnactedGovernanceProposal must surface the
			// synthetic row for its purpose; that's the lookup
			// epoch.go performs at every boundary tick.
			root, err := db.GetLastEnactedGovernanceProposal(
				c.queryGroup, nil,
			)
			require.NoError(t, err)
			require.NotNil(
				t, root, "no enacted root visible for %s", c.name,
			)
			assert.Equal(t, c.txHash, root.TxHash)
			assert.Equal(t, c.actionIdx, root.ActionIndex)
		})
	}
}

func TestImportGovStateSeedsCommitteeUpdateWhenCommitteePresent(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cm := &ParsedGovActionId{
		TxHash:      bytes.Repeat([]byte{0x55}, 32),
		ActionIndex: 0,
	}

	govStateData := govStateWithRoots(
		t,
		[4]*ParsedGovActionId{nil, nil, cm, nil},
		true, // committee present → root action_type = UpdateCommittee
	)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			GovStateData:  govStateData,
			Epoch:         500,
			EraIndex:      EraConway,
			EraBoundEpoch: 100,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	row, err := db.Metadata().GetGovernanceProposal(
		cm.TxHash, cm.ActionIndex, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, govActionTypeUpdateCommittee, row.ActionType)
}

func TestImportGovStateNoSeedingWhenAllSNothing(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	govStateData := govStateWithRoots(
		t,
		[4]*ParsedGovActionId{nil, nil, nil, nil},
		false,
	)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			GovStateData:  govStateData,
			Epoch:         500,
			EraIndex:      EraConway,
			EraBoundEpoch: 100,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	// Without prev gov action IDs there is nothing to seed.
	for _, group := range [][]uint8{
		{govActionTypeParameterChange},
		{govActionTypeHardForkInitiation},
		{govActionTypeNoConfidence, govActionTypeUpdateCommittee},
		{govActionTypeNewConstitution},
	} {
		root, err := db.GetLastEnactedGovernanceProposal(group, nil)
		require.NoError(t, err)
		require.Nil(t, root, "unexpected synthetic root for %v", group)
	}
}
