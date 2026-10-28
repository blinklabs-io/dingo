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

package ledger

import (
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// rewardProvenanceNodeReply is the GetRewardProvenance reply cardano-node
// 11.1.3 sent over NtC v23 for a Conway devnet, as the result element of
// MsgResult: the era wrapper holding RewardProvenance's sixteen fields, every
// one zero or empty. The ledger's getRewardProvenance returns the default
// value as its provenance, so the reply does not depend on chain state.
const rewardProvenanceNodeReply = "81" + "90" +
	"00" + // slots per epoch
	"a0" + // blocks made per pool
	"00" + "00" + "00" + "00" + "00" + // maxLL, deltaR1, deltaR2, r, totalStake
	"00" + // blocks count
	"d81e820001" + // decentralisation
	"00" + // expected blocks
	"d81e820001" + // eta
	"00" + "00" + "00" + // rPot, deltaT1, activeStake
	"a0" + "a0" // per-pool provenance, desirabilities

// TestQueryRewardProvenance_NodeReply answers GetRewardProvenance with the
// bytes cardano-node sends.
func TestQueryRewardProvenance_NodeReply(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.publishSnapshotsLocked()
	got, err := ls.Query(
		t.Context(),
		shelleyBlockQuery(&olocalstatequery.ShelleyRewardProvenanceQuery{}),
		QueryPoint{},
	)
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Equal(t, rewardProvenanceNodeReply, hex.EncodeToString(gotCbor))

	// The pinned gouroboros decoder reads the reply the way cardano-cli does.
	var decoded olocalstatequery.RewardProvenanceResult
	_, err = cbor.Decode(gotCbor, &decoded)
	require.NoError(t, err)
	require.Zero(t, decoded.EpochLength)
	require.Empty(t, decoded.PoolMints)
}

// TestQueryRewardProvenance_ViaGetCBOR wraps the same reply as the CBOR-in-CBOR
// the node builds, which is the form a client uses for a query whose layout
// may differ between node versions.
func TestQueryRewardProvenance_ViaGetCBOR(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.publishSnapshotsLocked()
	got, err := ls.Query(
		t.Context(),
		shelleyBlockQuery(&olocalstatequery.ShelleyCborQuery{
			Query: &olocalstatequery.ShelleyRewardProvenanceQuery{},
		}),
		QueryPoint{},
	)
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	inner, err := hex.DecodeString(rewardProvenanceNodeReply[2:])
	require.NoError(t, err)
	want, err := cbor.Encode([]any{cbor.Tag{
		Number:  cbor.CborTagCbor,
		Content: inner,
	}})
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(want), hex.EncodeToString(gotCbor))
}
