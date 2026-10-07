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
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func credKeyCBOR(t *testing.T, tag uint64, hash []byte) []byte {
	t.Helper()
	key, err := cbor.Encode([]any{tag, hash})
	require.NoError(t, err)
	return key
}

// committeeMapForTest encodes a cred->expiry map keeping the credential
// array keys as raw CBOR.
func committeeMapForTest(
	t *testing.T,
	entries []struct {
		tag    uint64
		hash   []byte
		expiry uint64
	},
) cbor.RawMessage {
	t.Helper()
	out := []byte{0xa0 + byte(len(entries))}
	for _, e := range entries {
		out = append(out, credKeyCBOR(t, e.tag, e.hash)...)
		v, err := cbor.Encode(e.expiry)
		require.NoError(t, err)
		out = append(out, v...)
	}
	return cbor.RawMessage(out)
}

// TestImportedRatifiedUpdateCommitteeEnactsAtNextBoundary drives a snapshot
// whose RatifyState.rsEnacted holds an UpdateCommittee while cgsCommittee
// still carries the committee in force. The import must keep the in-force
// committee, mark the proposal ratified at the snapshot epoch, and the next
// boundary must enact it by tagged credential identity.
func TestImportedRatifiedUpdateCommitteeEnactsAtNextBoundary(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	keepHash := bytes.Repeat([]byte{0x42}, 28)
	scriptHash := bytes.Repeat([]byte{0x44}, 28)
	addHash := bytes.Repeat([]byte{0x55}, 28)
	type entry = struct {
		tag    uint64
		hash   []byte
		expiry uint64
	}
	inForce := []any{[]any{
		committeeMapForTest(t, []entry{
			{0, keepHash, 700}, {1, scriptHash, 700},
		}),
		cbor.Rat{Rat: big.NewRat(2, 3)},
	}}
	next := []any{[]any{
		committeeMapForTest(t, []entry{
			{0, keepHash, 700}, {1, addHash, 900},
		}),
		cbor.Rat{Rat: big.NewRat(2, 3)},
	}}

	// Remove the script member (tag 1), add a new script member.
	actionCbor, err := cbor.Encode([]any{
		uint64(govActionTypeUpdateCommittee),
		nil,
		[]any{[]any{uint64(1), scriptHash}},
		committeeMapForTest(t, []entry{{1, addHash, 900}}),
		cbor.Rat{Rat: big.NewRat(2, 3)},
	})
	require.NoError(t, err)
	txHash := bytes.Repeat([]byte{0x77}, 32)
	proposal := govActionStateForTest(
		txHash, 0, govActionTypeUpdateCommittee, nil, 499,
	)
	body := proposal[4].([]any)
	body[1] = append([]byte{0xe0}, bytes.Repeat([]byte{0xa1}, 28)...)
	body[2] = cbor.RawMessage(actionCbor)

	govStateData, err := cbor.Encode([]any{
		[]any{encodeRootsAsAny(t, [4]*ParsedGovActionId{}), []any{proposal}},
		inForce,
		constitutionForTest(),
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		drepPulsingStateWithEnactCommittee(t, next, proposal),
	})
	require.NoError(t, err)

	var logs bytes.Buffer
	importCfg := govImportConfigForTest(db, govStateData)
	importCfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	require.NoError(t, importGovState(
		context.Background(),
		importCfg,
		func(ImportProgress) {},
	))
	require.Contains(
		t,
		logs.String(),
		"level=WARN msg=\"snapshot holds ratified committee actions not yet enacted\"",
	)

	members, err := db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 2)
	imported := map[string]uint64{}
	for _, member := range members {
		imported[models.CommitteeCredential{
			CredentialTag: member.ColdCredentialTag,
			Credential:    member.ColdCredHash,
		}.Key()] = member.ExpiresEpoch
	}
	wantImported := map[string]uint64{
		(models.CommitteeCredential{CredentialTag: 0, Credential: keepHash}).Key():   700,
		(models.CommitteeCredential{CredentialTag: 1, Credential: scriptHash}).Key(): 700,
	}
	require.Equal(t, wantImported, imported)

	row, err := db.Metadata().GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.NotNil(t, row.RatifiedEpoch, "UpdateCommittee must be ratified")
	require.Equal(t, uint64(500), *row.RatifiedEpoch)

	txn := db.MetadataTxn(true)
	defer txn.Release()
	pp := &conway.ConwayProtocolParameters{}
	pp.ProtocolVersion.Major = 10
	out, err := governance.ProcessEpoch(&governance.EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    500,
		NewEpoch:     501,
		BoundarySlot: 50_100,
		PParams:      pp,
		UpdateFn: func(
			p lcommon.ProtocolParameters, _ any,
		) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, out.EnactedCount)
	require.NoError(t, txn.Commit())

	members, err = db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	got := map[string]bool{}
	for _, m := range members {
		got[models.CommitteeCredential{
			CredentialTag: m.ColdCredentialTag,
			Credential:    m.ColdCredHash,
		}.Key()] = true
	}
	want := map[string]bool{}
	for _, c := range []struct {
		tag  uint8
		hash []byte
	}{{0, keepHash}, {1, addHash}} {
		want[models.CommitteeCredential{
			CredentialTag: c.tag,
			Credential:    c.hash,
		}.Key()] = true
	}
	require.Equal(t, want, got)
}

// A snapshot whose ratified action is not a committee action leaves the
// imported committee authoritative, so the import raises no committee warning.
func TestImportRatifiedNonCommitteeActionDoesNotWarn(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	txHash := bytes.Repeat([]byte{0x78}, 32)
	proposal := govActionStateForTest(
		txHash, 0, govActionTypeParameterChange, nil, 499,
	)
	inForce := committeeWithMember(t, bytes.Repeat([]byte{0x42}, 28), 700)
	govStateData, err := cbor.Encode([]any{
		[]any{encodeRootsAsAny(t, [4]*ParsedGovActionId{}), []any{proposal}},
		inForce,
		constitutionForTest(),
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		drepPulsingStateWithEnactCommittee(t, inForce, proposal),
	})
	require.NoError(t, err)

	var logs bytes.Buffer
	importCfg := govImportConfigForTest(db, govStateData)
	importCfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	require.NoError(t, importGovState(
		context.Background(),
		importCfg,
		func(ImportProgress) {},
	))
	row, err := db.Metadata().GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, row.RatifiedEpoch, "the action must be imported ratified")
	require.NotContains(t, logs.String(), "level=WARN")
}
