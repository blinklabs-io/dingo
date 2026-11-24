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

package server_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/midnight"
	"github.com/blinklabs-io/dingo/midnight/server"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

type archiveBlocks struct{ archive *testutil.PayloadArchive }

func (b archiveBlocks) LocatorAtSlot(
	context.Context,
	uint64,
) (types.PayloadLocator, error) {
	return b.archive, nil
}

// A released store's Midnight datum columns hold copies of the datums. After
// they become block offsets, the governance, Ariadne and epoch-candidate RPCs
// return the same datum bytes, and the candidate snapshot decodes to the same
// entries.
func TestMidnightRPCsAnswerTheSameAfterMigration(t *testing.T) {
	t.Parallel()
	db := newTestDatabase(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := raw.Exec(query, args...)
		require.NoError(t, err, query)
	}
	council := []byte{0xd8, 0x79, 0x9f, 0x01, 0xff}
	ariadne := []byte{0xd8, 0x79, 0x9f, 0x02, 0xff}
	candidate := []byte{0xd8, 0x79, 0x9f, 0x03, 0xff}
	for _, datum := range [][]byte{council, ariadne, candidate} {
		exec(
			"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 1)",
			lcommon.Blake2b256Hash(datum).Bytes(), datum,
		)
	}
	exec(
		`INSERT INTO midnight_governance_datums
(datum_type, tx_hash, output_index, datum, block_number) VALUES ('council', ?, 0, ?, 10)`,
		[]byte{5, 6, 7, 8}, council,
	)
	exec(
		"INSERT INTO midnight_ariadne_params (epoch, datum) VALUES (3, ?)",
		ariadne,
	)
	entries := []candidateEntryCbor{
		{TxHash: []byte{0xAA}, OutputIndex: 0, Datum: candidate},
	}
	snapshot, err := cbor.Marshal(entries)
	require.NoError(t, err)
	exec(
		"INSERT INTO midnight_epoch_candidates (epoch, block_number, candidates_cbor) VALUES (7, 700, ?)",
		snapshot,
	)

	archive := testutil.NewPayloadArchive()
	db.Metadata().(types.PayloadResolverSetter).SetPayloadResolver(archive)
	migrator, ok := db.Metadata().(metadata.PayloadColumnMigrator)
	require.True(t, ok)
	require.NoError(t, migrator.MigratePayloadColumns(
		t.Context(), archiveBlocks{archive: archive},
	))

	addr := startTestServerWithConfig(
		t,
		server.Config{
			Database:  server.NewDatabase(db),
			SlotTimer: fakeSlotTimer{},
		},
	)
	client := dialClient(t, addr)
	gov, err := client.GetCouncilDatum(
		callCtx(t), &midnight.CouncilDatumRequest{BlockNumber: 100},
	)
	require.NoError(t, err)
	require.Equal(t, council, gov.GetDatum())
	params, err := client.GetAriadneParameters(
		callCtx(t), &midnight.AriadneParametersRequest{Epoch: 3},
	)
	require.NoError(t, err)
	require.Equal(t, ariadne, params.GetDatum())

	stored, err := db.GetMidnightEpochCandidatesByEpoch(7)
	require.NoError(t, err)
	require.True(
		t, bytes.Equal(snapshot, stored.CandidatesCbor),
		"the snapshot decodes to the bytes the released store held",
	)
}
