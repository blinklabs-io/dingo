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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// stakePoolStateConwayGolden is the ledger's golden file for the
// GetStakePoolState reply in Conway, byte for byte, from cardano-ledger at
// commit 9df9f98cce86c5f2195142b2c02dd9b39dd9dc7c:
// libs/cardano-ledger-api/golden/conway/cbor/queryStakePoolState.cbor.
//
// It is a three-element array of replies: no pools, two pools with their
// delegators, and the same two pools without delegators. The node's
// single-element era result wrapper is not part of the file; the tests add it.
const stakePoolStateConwayGolden = "" +
	"83a0a2581c0d6a577e9441ad8ed9663931906e4d43ece8f82c712b1d0235affb" +
	"068e5820c5e21ab1c9f6022d81c3b25e3436cb7f1df77f9652ae3e1310c28e62" +
	"1dd87b4cf61b000000174876e8001a0a21fe80d81e82010a8200581c0d6a577e" +
	"9441ad8ed9663931906e4d43ece8f82c712b1d0235affb06d9010281581c0d6a" +
	"577e9441ad8ed9663931906e4d43ece8f82c712b1d0235affb06838400190bb8" +
	"447f00000150b80d01200000000000000000010000008301190bb97172656c61" +
	"792e6578616d706c652e636f6d82027172656c61792e6578616d706c652e636f" +
	"6d826e636f6e73656e7375732e706f6f6c427b7d1a1dcd650002d90102828201" +
	"581c92bf8969a49623af689a49861adf7639b25fd663c95733f147f44e718200" +
	"581c6d8143487c22dc58eceb5b53f9876e008f052212542d25a0ae8b859b18fa" +
	"89581c0d6a577e9441ad8ed9663931906e4d43ece8f82c712b1d0235affb0658" +
	"20c5e21ab1c9f6022d81c3b25e3436cb7f1df77f9652ae3e1310c28e621dd87b" +
	"4c1b00000022ecb25c001a0a21fe80d81e82010a581de00d6a577e9441ad8ed9" +
	"663931906e4d43ece8f82c712b1d0235affb06d9010281581c0d6a577e9441ad" +
	"8ed9663931906e4d43ece8f82c712b1d0235affb06838400190bb8447f000001" +
	"50b80d01200000000000000000010000008301190bb97172656c61792e657861" +
	"6d706c652e636f6d82027172656c61792e6578616d706c652e636f6d826e636f" +
	"6e73656e7375732e706f6f6c427b7d581cb77908a84072eb9151782d64504f0f" +
	"6975b7143d49514eaeb0991d008e5820e07a156ccb70a41eef8339001bd6a0e0" +
	"f773e0ee33ce94fb9222df1874d6fdddf61b0000000ba43b74001a0a21fe80d8" +
	"1e820118648200581c3113f778bfe62098fb783f7defac9b1f5f5d054e267005" +
	"f630b85acbd9010281581c3113f778bfe62098fb783f7defac9b1f5f5d054e26" +
	"7005f630b85acb80f61a1dcd650003d90102838201581c0fb2c57096165367d4" +
	"9491db222d5fc66d38f59be2c1cb467f002ac28200581c1a7d5e232ad01edfcd" +
	"4d8d651b616364c11cdc041bb5b92a4a01575e8200581c1cd2b7f71d400aaabd" +
	"4f85803ae5ff6f77c1a978586fda00e174216cf6f6a2581c0d6a577e9441ad8e" +
	"d9663931906e4d43ece8f82c712b1d0235affb068e5820c5e21ab1c9f6022d81" +
	"c3b25e3436cb7f1df77f9652ae3e1310c28e621dd87b4cf61b000000174876e8" +
	"001a0a21fe80d81e82010a8200581c0d6a577e9441ad8ed9663931906e4d43ec" +
	"e8f82c712b1d0235affb06d9010281581c0d6a577e9441ad8ed9663931906e4d" +
	"43ece8f82c712b1d0235affb06838400190bb8447f00000150b80d0120000000" +
	"0000000000010000008301190bb97172656c61792e6578616d706c652e636f6d" +
	"82027172656c61792e6578616d706c652e636f6d826e636f6e73656e7375732e" +
	"706f6f6c427b7d1a1dcd650002f618fa89581c0d6a577e9441ad8ed966393190" +
	"6e4d43ece8f82c712b1d0235affb065820c5e21ab1c9f6022d81c3b25e3436cb" +
	"7f1df77f9652ae3e1310c28e621dd87b4c1b00000022ecb25c001a0a21fe80d8" +
	"1e82010a581de00d6a577e9441ad8ed9663931906e4d43ece8f82c712b1d0235" +
	"affb06d9010281581c0d6a577e9441ad8ed9663931906e4d43ece8f82c712b1d" +
	"0235affb06838400190bb8447f00000150b80d01200000000000000000010000" +
	"008301190bb97172656c61792e6578616d706c652e636f6d82027172656c6179" +
	"2e6578616d706c652e636f6d826e636f6e73656e7375732e706f6f6c427b7d58" +
	"1cb77908a84072eb9151782d64504f0f6975b7143d49514eaeb0991d008e5820" +
	"e07a156ccb70a41eef8339001bd6a0e0f773e0ee33ce94fb9222df1874d6fddd" +
	"f61b0000000ba43b74001a0a21fe80d81e820118648200581c3113f778bfe620" +
	"98fb783f7defac9b1f5f5d054e267005f630b85acbd9010281581c3113f778bf" +
	"e62098fb783f7defac9b1f5f5d054e267005f630b85acb80f61a1dcd650003f6" +
	"f6f6"

// stakePoolStateDijkstraGolden is the ledger's golden file for the
// GetStakePoolState reply in Dijkstra, byte for byte, from cardano-ledger at
// commit 9df9f98cce86c5f2195142b2c02dd9b39dd9dc7c:
// libs/cardano-ledger-api/golden/dijkstra/cbor/queryStakePoolState.cbor.
//
// It has the shape of the Conway file, with a BLS key on the first pool and in
// its pending registration.
const stakePoolStateDijkstraGolden = "" +
	"83a0a2581c0d6a577e9441ad8ed9663931906e4d43ece8f82c712b1d0235affb" +
	"068e5820c5e21ab1c9f6022d81c3b25e3436cb7f1df77f9652ae3e1310c28e62" +
	"1dd87b4c82825860a57565542eaa01ef2b910bf0eaba4d98a1e5b8b79cc425db" +
	"08f8780732d0ea9bc85fc6175f272b2344bb27bc572ebf14022e52689dcedfcc" +
	"f44a00e5bd1aa59db44517217d6b0f21b372169ee761938c28914ddcb9663de5" +
	"4db288e760a8e14f5830b408288be1abfd4c34ce9d275b4cc1520aa600158faa" +
	"4791a74ff474a79223b359193aec1bb3fb2a1aadb3b054842aac18c81b000000" +
	"174876e8001a0a21fe80d81e82010a8200581c0d6a577e9441ad8ed966393190" +
	"6e4d43ece8f82c712b1d0235affb06d9010281581c0d6a577e9441ad8ed96639" +
	"31906e4d43ece8f82c712b1d0235affb06838400190bb8447f00000150b80d01" +
	"200000000000000000010000008301190bb97172656c61792e6578616d706c65" +
	"2e636f6d82027172656c61792e6578616d706c652e636f6d826e636f6e73656e" +
	"7375732e706f6f6c427b7d1a1dcd650002d90102828201581c92bf8969a49623" +
	"af689a49861adf7639b25fd663c95733f147f44e718200581c6d8143487c22dc" +
	"58eceb5b53f9876e008f052212542d25a0ae8b859b18fa8a581c0d6a577e9441" +
	"ad8ed9663931906e4d43ece8f82c712b1d0235affb065820c5e21ab1c9f6022d" +
	"81c3b25e3436cb7f1df77f9652ae3e1310c28e621dd87b4c825860a57565542e" +
	"aa01ef2b910bf0eaba4d98a1e5b8b79cc425db08f8780732d0ea9bc85fc6175f" +
	"272b2344bb27bc572ebf14022e52689dcedfccf44a00e5bd1aa59db44517217d" +
	"6b0f21b372169ee761938c28914ddcb9663de54db288e760a8e14f5830b40828" +
	"8be1abfd4c34ce9d275b4cc1520aa600158faa4791a74ff474a79223b359193a" +
	"ec1bb3fb2a1aadb3b054842aac1b00000022ecb25c001a0a21fe80d81e82010a" +
	"581de00d6a577e9441ad8ed9663931906e4d43ece8f82c712b1d0235affb06d9" +
	"010281581c0d6a577e9441ad8ed9663931906e4d43ece8f82c712b1d0235affb" +
	"06838400190bb8447f00000150b80d0120000000000000000001000000830119" +
	"0bb97172656c61792e6578616d706c652e636f6d82027172656c61792e657861" +
	"6d706c652e636f6d826e636f6e73656e7375732e706f6f6c427b7d581cb77908" +
	"a84072eb9151782d64504f0f6975b7143d49514eaeb0991d008e5820e07a156c" +
	"cb70a41eef8339001bd6a0e0f773e0ee33ce94fb9222df1874d6fdddf61b0000" +
	"000ba43b74001a0a21fe80d81e820118648200581c3113f778bfe62098fb783f" +
	"7defac9b1f5f5d054e267005f630b85acbd9010281581c3113f778bfe62098fb" +
	"783f7defac9b1f5f5d054e267005f630b85acb80f61a1dcd650003d901028382" +
	"01581c0fb2c57096165367d49491db222d5fc66d38f59be2c1cb467f002ac282" +
	"00581c1a7d5e232ad01edfcd4d8d651b616364c11cdc041bb5b92a4a01575e82" +
	"00581c1cd2b7f71d400aaabd4f85803ae5ff6f77c1a978586fda00e174216cf6" +
	"f6a2581c0d6a577e9441ad8ed9663931906e4d43ece8f82c712b1d0235affb06" +
	"8e5820c5e21ab1c9f6022d81c3b25e3436cb7f1df77f9652ae3e1310c28e621d" +
	"d87b4c82825860a57565542eaa01ef2b910bf0eaba4d98a1e5b8b79cc425db08" +
	"f8780732d0ea9bc85fc6175f272b2344bb27bc572ebf14022e52689dcedfccf4" +
	"4a00e5bd1aa59db44517217d6b0f21b372169ee761938c28914ddcb9663de54d" +
	"b288e760a8e14f5830b408288be1abfd4c34ce9d275b4cc1520aa600158faa47" +
	"91a74ff474a79223b359193aec1bb3fb2a1aadb3b054842aac18c81b00000017" +
	"4876e8001a0a21fe80d81e82010a8200581c0d6a577e9441ad8ed9663931906e" +
	"4d43ece8f82c712b1d0235affb06d9010281581c0d6a577e9441ad8ed9663931" +
	"906e4d43ece8f82c712b1d0235affb06838400190bb8447f00000150b80d0120" +
	"0000000000000000010000008301190bb97172656c61792e6578616d706c652e" +
	"636f6d82027172656c61792e6578616d706c652e636f6d826e636f6e73656e73" +
	"75732e706f6f6c427b7d1a1dcd650002f618fa8a581c0d6a577e9441ad8ed966" +
	"3931906e4d43ece8f82c712b1d0235affb065820c5e21ab1c9f6022d81c3b25e" +
	"3436cb7f1df77f9652ae3e1310c28e621dd87b4c825860a57565542eaa01ef2b" +
	"910bf0eaba4d98a1e5b8b79cc425db08f8780732d0ea9bc85fc6175f272b2344" +
	"bb27bc572ebf14022e52689dcedfccf44a00e5bd1aa59db44517217d6b0f21b3" +
	"72169ee761938c28914ddcb9663de54db288e760a8e14f5830b408288be1abfd" +
	"4c34ce9d275b4cc1520aa600158faa4791a74ff474a79223b359193aec1bb3fb" +
	"2a1aadb3b054842aac1b00000022ecb25c001a0a21fe80d81e82010a581de00d" +
	"6a577e9441ad8ed9663931906e4d43ece8f82c712b1d0235affb06d901028158" +
	"1c0d6a577e9441ad8ed9663931906e4d43ece8f82c712b1d0235affb06838400" +
	"190bb8447f00000150b80d01200000000000000000010000008301190bb97172" +
	"656c61792e6578616d706c652e636f6d82027172656c61792e6578616d706c65" +
	"2e636f6d826e636f6e73656e7375732e706f6f6c427b7d581cb77908a84072eb" +
	"9151782d64504f0f6975b7143d49514eaeb0991d008e5820e07a156ccb70a41e" +
	"ef8339001bd6a0e0f773e0ee33ce94fb9222df1874d6fdddf61b0000000ba43b" +
	"74001a0a21fe80d81e820118648200581c3113f778bfe62098fb783f7defac9b" +
	"1f5f5d054e267005f630b85acbd9010281581c3113f778bfe62098fb783f7def" +
	"ac9b1f5f5d054e267005f630b85acb80f61a1dcd650003f6f6f6"

func goldenBytes(t *testing.T, golden string) []byte {
	t.Helper()
	out, err := hex.DecodeString(strings.ReplaceAll(golden, " ", ""))
	require.NoError(t, err)
	return out
}

// ledgerGoldenReplies splits a ledger golden file into its replies, each in
// the single-element era result wrapper the node puts around the reply.
func ledgerGoldenReplies(t *testing.T, golden string) [][]byte {
	t.Helper()
	var maps []cbor.RawMessage
	_, err := cbor.Decode(goldenBytes(t, golden), &maps)
	require.NoError(t, err)
	replies := make([][]byte, 0, len(maps))
	for _, m := range maps {
		replies = append(replies, append([]byte{0x81}, m...))
	}
	return replies
}

// TestStakePoolStateGoldenFilesAreVerbatim guards the fixtures: they are the
// ledger's files, not this package's idea of them.
func TestStakePoolStateGoldenFilesAreVerbatim(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct{ golden, digest string }{
		"conway": {
			stakePoolStateConwayGolden,
			"2a218f2927f25a7e2f939d7f959542f346b432fba5d636ec4471775d84d932cd",
		},
		"dijkstra": {
			stakePoolStateDijkstraGolden,
			"09794fa5860071b0a4d73fccdcaca79b7784c3b46f4ea08a55f701f087f576cd",
		},
	} {
		sum := sha256.Sum256(goldenBytes(t, tc.golden))
		require.Equal(t, tc.digest, hex.EncodeToString(sum[:]), name)
	}
}

func goldenRelays(relays []lcommon.PoolRelay) []models.PoolRegistrationRelay {
	out := make([]models.PoolRegistrationRelay, 0, len(relays))
	for _, relay := range relays {
		kind := relay.Type
		row := models.PoolRegistrationRelay{
			Type: &kind,
			Ipv4: relay.Ipv4,
			Ipv6: relay.Ipv6,
		}
		if relay.Port != nil {
			row.Port = uint(*relay.Port)
		}
		if relay.Hostname != nil {
			row.Hostname = *relay.Hostname
		}
		out = append(out, row)
	}
	return out
}

func goldenOwners(set cbor.SetType[lcommon.Blake2b224]) []models.PoolRegistrationOwner {
	var out []models.PoolRegistrationOwner
	for _, owner := range set.Items() {
		out = append(out, models.PoolRegistrationOwner{
			KeyHash: bytes.Clone(owner[:]),
		})
	}
	return out
}

func goldenMetadata(m *lcommon.PoolMetadata) (string, []byte) {
	if m == nil {
		return "", nil
	}
	return m.Url, bytes.Clone(m.Hash)
}

// seedGoldenStakePoolState writes the registrations, delegations and
// retirements that make a node answer with the ledger's own reply for the
// pools of result: the first registration is the entry, a pending one the
// future parameters.
func seedGoldenStakePoolState(
	t *testing.T,
	ls *LedgerState,
	result olocalstatequery.StakePoolStateResult,
) {
	t.Helper()
	for poolId, entry := range result.Results {
		pool := poolId[:]
		url, hash := goldenMetadata(entry.Metadata)
		reg := &models.PoolRegistration{
			PoolKeyHash:                bytes.Clone(pool),
			VrfKeyHash:                 bytes.Clone(entry.Vrf[:]),
			RewardAccount:              bytes.Clone(entry.AccountId.Credential[:]),
			RewardAccountCredentialTag: uint8(entry.AccountId.CredType), // #nosec G115
			Pledge:                     dbtypes.Uint64(entry.Pledge),
			Cost:                       dbtypes.Uint64(entry.Cost),
			Margin:                     &dbtypes.Rat{Rat: new(big.Rat).Set(entry.Margin.Rat)},
			MetadataUrl:                url,
			MetadataHash:               hash,
			Owners:                     goldenOwners(entry.Owners),
			Relays:                     goldenRelays(entry.Relays),
			DepositAmount:              dbtypes.Uint64(entry.Deposit),
			AddedSlot:                  5,
		}
		if entry.BlsKey != nil {
			registeredIn := entry.BlsKey.RegisteredIn
			reg.LeiosKeyPublic = bytes.Clone(entry.BlsKey.Key.PublicKey)
			reg.LeiosKeyPossessionProof = bytes.Clone(
				entry.BlsKey.Key.PossessionProof,
			)
			reg.LeiosKeyRegistrationEpoch = &registeredIn
		}
		importGoldenRegistration(t, ls, reg)
		if future := entry.FutureStakePoolParams; future != nil {
			url, hash := goldenMetadata(future.Metadata)
			// The account address is the header byte and the credential;
			// bit 4 of the header marks a script credential.
			next := &models.PoolRegistration{
				PoolKeyHash:                bytes.Clone(pool),
				VrfKeyHash:                 bytes.Clone(future.Vrf[:]),
				RewardAccount:              bytes.Clone(future.AccountAddress[1:]),
				RewardAccountCredentialTag: (future.AccountAddress[0] >> 4) & 1,
				Pledge:                     dbtypes.Uint64(future.Pledge),
				Cost:                       dbtypes.Uint64(future.Cost),
				Margin:                     &dbtypes.Rat{Rat: new(big.Rat).Set(future.Margin.Rat)},
				MetadataUrl:                url,
				MetadataHash:               hash,
				Owners:                     goldenOwners(future.Owners),
				Relays:                     goldenRelays(future.Relays),
				DepositAmount:              dbtypes.Uint64(entry.Deposit),
				AddedSlot:                  150,
			}
			if future.BlsKey != nil {
				registeredIn := uint64(0)
				next.LeiosKeyPublic = bytes.Clone(future.BlsKey.PublicKey)
				next.LeiosKeyPossessionProof = bytes.Clone(
					future.BlsKey.PossessionProof,
				)
				next.LeiosKeyRegistrationEpoch = &registeredIn
			}
			importGoldenRegistration(t, ls, next)
		}
		if entry.Retiring != nil {
			// After the pending registration, which would cancel it.
			seedStakePoolRetirement(t, ls, pool, *entry.Retiring, 155)
		}
		if entry.Delegators != nil {
			for _, credential := range entry.Delegators.Items() {
				seedActiveDelegator(
					t, ls,
					credential.Credential[:],
					uint8(credential.CredType), // #nosec G115
					pool,
					true,
				)
			}
		}
	}
}

func importGoldenRegistration(
	t *testing.T,
	ls *LedgerState,
	reg *models.PoolRegistration,
) {
	t.Helper()
	require.NoError(t, ls.db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:   reg.PoolKeyHash,
			VrfKeyHash:    reg.VrfKeyHash,
			RewardAccount: reg.RewardAccount,
			Margin:        reg.Margin,
			Pledge:        reg.Pledge,
			Cost:          reg.Cost,
		},
		reg,
		nil,
	))
}

// TestQueryStakePoolState_MatchesLedgerGolden compares dingo's reply to the
// ledger's own golden bytes, for the pools, delegators, retirement and pending
// registration the golden file carries. The node is seeded from the golden's
// decoded values; the comparison is of the encoded reply against the file's
// bytes, so field order, set order, map order and every optional field are the
// ledger's.
func TestQueryStakePoolState_MatchesLedgerGolden(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		golden          string
		protocolVersion uint
	}{
		"conway":   {stakePoolStateConwayGolden, 10},
		"dijkstra": {stakePoolStateDijkstraGolden, 12},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			replies := ledgerGoldenReplies(t, tc.golden)
			require.Len(t, replies, 3)
			var withDelegators olocalstatequery.StakePoolStateResult
			_, err := cbor.Decode(replies[1], &withDelegators)
			require.NoError(t, err)
			require.Len(t, withDelegators.Results, 2)

			ls := newStakePoolStateLedger(t)
			placeStakePoolStateTip(t, ls, 160)
			pp := &conway.ConwayProtocolParameters{}
			pp.ProtocolVersion.Major = tc.protocolVersion
			ls.currentPParams = pp
			ls.publishSnapshotsLocked()
			seedGoldenStakePoolState(t, ls, withDelegators)
			var pools [][]byte
			for poolId := range withDelegators.Results {
				pools = append(pools, bytes.Clone(poolId[:]))
			}

			for _, c := range []struct {
				name              string
				includeDelegators bool
				pools             [][]byte
				want              []byte
			}{
				{"no pools", true, [][]byte{}, replies[0]},
				{"two pools with delegators", true, pools, replies[1]},
				{"two pools without delegators", false, pools, replies[2]},
				{"every pool without delegators", false, nil, replies[2]},
				{"every pool with delegators", true, nil, replies[1]},
			} {
				got, err := ls.Query(
					t.Context(),
					stakePoolStateQuery(c.includeDelegators, c.pools),
					QueryPoint{},
				)
				require.NoError(t, err, c.name)
				wire, err := cbor.Encode(got)
				require.NoError(t, err, c.name)
				require.Equal(
					t,
					hex.EncodeToString(c.want),
					hex.EncodeToString(wire),
					c.name,
				)
			}
		})
	}
}

// TestQueryStakePoolState_BlsKeyOnlyFromProtocolVersion12 seeds the pools of
// the ledger's Dijkstra golden, whose first pool registered a BLS key, and
// answers at protocol versions 11 and 12. The ledger encodes the key, in the
// entry and in its pending registration, only from version 12, and below that
// the field is null rather than the key.
func TestQueryStakePoolState_BlsKeyOnlyFromProtocolVersion12(t *testing.T) {
	t.Parallel()
	replies := ledgerGoldenReplies(t, stakePoolStateDijkstraGolden)
	var golden olocalstatequery.StakePoolStateResult
	_, err := cbor.Decode(replies[1], &golden)
	require.NoError(t, err)
	var pool, publicKey []byte
	for poolId, entry := range golden.Results {
		if entry.BlsKey != nil {
			pool = bytes.Clone(poolId[:])
			publicKey = entry.BlsKey.Key.PublicKey
		}
	}
	require.NotEmpty(t, pool, "the golden has a pool with a BLS key")

	for _, tc := range []struct {
		protocolVersion uint
		wantKey         bool
	}{
		{10, false},
		{11, false},
		{12, true},
		{13, true},
	} {
		ls := newStakePoolStateLedger(t)
		placeStakePoolStateTip(t, ls, 160)
		pp := &conway.ConwayProtocolParameters{}
		pp.ProtocolVersion.Major = tc.protocolVersion
		ls.currentPParams = pp
		ls.publishSnapshotsLocked()
		seedGoldenStakePoolState(t, ls, golden)

		got := runStakePoolState(
			t, ls, stakePoolStateQuery(false, [][]byte{pool}),
		)
		entry := got[gledger.PoolId(pool)]
		require.NotNil(t, entry.FutureStakePoolParams)
		if tc.wantKey {
			require.NotNil(t, entry.BlsKey, "version %d", tc.protocolVersion)
			require.Equal(t, publicKey, entry.BlsKey.Key.PublicKey)
			require.Equal(t, uint64(200), entry.BlsKey.RegisteredIn)
			require.NotNil(t, entry.FutureStakePoolParams.BlsKey)
			continue
		}
		require.Nil(t, entry.BlsKey, "version %d", tc.protocolVersion)
		require.Nil(
			t,
			entry.FutureStakePoolParams.BlsKey,
			"version %d",
			tc.protocolVersion,
		)
	}
}
