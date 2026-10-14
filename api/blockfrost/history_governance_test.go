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

package blockfrost

import (
	"errors"
	"net/http"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testGovActionID(t *testing.T, tx byte, index uint32) string {
	t.Helper()
	id, err := lcommon.NewGovActionId(fill32(tx), index)
	require.NoError(t, err)
	return id.String()
}

func TestHandleGovernanceProposalList(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.proposals.items = []GovernanceProposalInfo{{
		ID: "gov_action1abc", TxHash: "aa", CertIndex: 2,
		GovernanceType: "info_action",
	}}
	node.proposals.total = 1
	recorder := get(t, node, "/api/v0/governance/proposals?count=3&order=desc")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, 3, node.proposals.params.Count)
	assert.Equal(t, PaginationOrderDesc, node.proposals.params.Order)
	assert.JSONEq(t, `[{"id":"gov_action1abc","tx_hash":"aa","cert_index":2,"governance_type":"info_action"}]`, recorder.Body.String())
	assert.Equal(t, "1", recorder.Header().Get("X-Pagination-Count-Total"))
}

// Both identifier forms of every proposal operation reach the same node
// method with the same reference.
func TestHandleGovernanceProposalIdentifierForms(t *testing.T) {
	t.Parallel()
	id := testGovActionID(t, 0xab, 3)
	wantKey := hexOf(fill32(0xab)) + "#3"
	txForm := "/api/v0/governance/proposals/" + hexOf(fill32(0xab)) + "/3"
	idForm := "/api/v0/governance/proposals/" + id

	for _, tc := range []struct {
		name  string
		setup func(*mockNode)
		key   func(*mockNode) string
		part  string
		body  string
	}{
		{"proposal", func(n *mockNode) {
			n.proposal.value = GovernanceProposalDetailInfo{
				GovernanceProposalInfo: GovernanceProposalInfo{ID: id, TxHash: hexOf(fill32(0xab)), CertIndex: 3, GovernanceType: "info_action"},
				Description:            []byte(`{"tag":"InfoAction"}`),
				Deposit:                "100", ReturnAddress: "stake_test1x", Expiration: 9,
			}
		}, func(n *mockNode) string { return n.proposal.key }, "",
			`{"id":"` + id + `","tx_hash":"` + hexOf(fill32(0xab)) + `","cert_index":3,"governance_type":"info_action","governance_description":{"tag":"InfoAction"},"deposit":"100","return_address":"stake_test1x","ratified_epoch":null,"enacted_epoch":null,"dropped_epoch":null,"expired_epoch":null,"expiration":9}`},
		{"parameters", func(n *mockNode) {
			n.proposalParams.value = GovernanceProposalParametersInfo{ID: id, TxHash: "aa", CertIndex: 3}
		}, func(n *mockNode) string { return n.proposalParams.key }, "/parameters", ""},
		{"withdrawals", func(n *mockNode) {
			n.proposalWithdrawals.items = []GovernanceWithdrawalInfo{{StakeAddress: "stake_test1w", Amount: "5"}}
			n.proposalWithdrawals.total = 1
		}, func(n *mockNode) string { return n.proposalWithdrawals.key }, "/withdrawals",
			`[{"stake_address":"stake_test1w","amount":"5"}]`},
		{"votes", func(n *mockNode) {
			n.proposalVotes.items = []GovernanceProposalVoteInfo{{TxHash: "bb", CertIndex: 1, VoterRole: "drep", Voter: "drep1x", Vote: "yes", Counted: true}}
			n.proposalVotes.total = 1
		}, func(n *mockNode) string { return n.proposalVotes.key }, "/votes",
			`[{"tx_hash":"bb","cert_index":1,"voter_role":"drep","voter":"drep1x","vote":"yes","counted":true}]`},
		{"metadata", func(n *mockNode) {
			n.proposalMetadata.value = GovernanceProposalMetadataInfo{ID: id, TxHash: "aa", CertIndex: 3, URL: "u", Hash: "h", Bytes: "\\x7b7d"}
		}, func(n *mockNode) string { return n.proposalMetadata.key }, "/metadata",
			`{"id":"` + id + `","tx_hash":"aa","cert_index":3,"url":"u","hash":"h","json_metadata":null,"bytes":"\\x7b7d"}`},
	} {
		for form, target := range map[string]string{"tx": txForm + tc.part, "id": idForm + tc.part} {
			t.Run(tc.name+"/"+form, func(t *testing.T) {
				t.Parallel()
				node := &mockNode{}
				tc.setup(node)
				recorder := get(t, node, target)
				require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
				assert.Equal(t, wantKey, tc.key(node))
				if tc.body != "" {
					assert.JSONEq(t, tc.body, recorder.Body.String())
				}
			})
		}
	}
}

func TestHandleGovernanceProposalParametersAreNullWhenUnset(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	recorder := get(t, node, "/api/v0/governance/proposals/"+hexOf(fill32(1))+"/0/parameters")
	require.Equal(t, http.StatusOK, recorder.Code)
	var body map[string]any
	require.NoError(t, jsonDecode(recorder, &body))
	params, ok := body["parameters"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, params, 54, "every key of the OpenAPI parameters object is present")
	for key, value := range params {
		assert.Nil(t, value, key)
	}
}

func TestHandleGovernanceProposalErrors(t *testing.T) {
	t.Parallel()
	txHash := hexOf(fill32(1))
	for _, tc := range []struct {
		name   string
		target string
		status int
	}{
		{"bad hash", "/api/v0/governance/proposals/zz/0", http.StatusBadRequest},
		{"short hash", "/api/v0/governance/proposals/abcd/0", http.StatusBadRequest},
		{"bad index", "/api/v0/governance/proposals/" + txHash + "/x", http.StatusBadRequest},
		{"negative index", "/api/v0/governance/proposals/" + txHash + "/-1", http.StatusBadRequest},
		{"bad id", "/api/v0/governance/proposals/not-an-id", http.StatusBadRequest},
		{"bad id part", "/api/v0/governance/proposals/not-an-id/votes", http.StatusBadRequest},
		{"bad tx part", "/api/v0/governance/proposals/zz/0/votes", http.StatusBadRequest},
		{"unknown part", "/api/v0/governance/proposals/" + txHash + "/0/other", http.StatusNotFound},
		{"extra segments", "/api/v0/governance/proposals/" + txHash + "/0/votes/x", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recorder := get(t, &mockNode{}, tc.target)
			assert.Equal(t, tc.status, recorder.Code)
		})
	}

	for _, errCase := range []struct {
		err    error
		status int
	}{
		{models.ErrGovernanceProposalNotFound, http.StatusNotFound},
		{ErrHistoryNotRetained, http.StatusNotFound},
		{errors.New("boom"), http.StatusInternalServerError},
	} {
		for _, part := range []string{"", "/parameters", "/withdrawals", "/votes", "/metadata"} {
			t.Run(part+"/"+errCase.err.Error(), func(t *testing.T) {
				t.Parallel()
				node := &mockNode{}
				node.proposal.err = errCase.err
				node.proposalParams.err = errCase.err
				node.proposalWithdrawals.err = errCase.err
				node.proposalVotes.err = errCase.err
				node.proposalMetadata.err = errCase.err
				recorder := get(t, node, "/api/v0/governance/proposals/"+txHash+"/0"+part)
				assert.Equal(t, errCase.status, recorder.Code)
				assert.NotContains(t, recorder.Body.String(), "boom")
			})
		}
	}
}

func TestHandleGovernanceDRepOperations(t *testing.T) {
	t.Parallel()
	hash := hexOf(bytes32of(28, 0xd1))
	base := "/api/v0/governance/dreps/" + hash

	node := &mockNode{}
	node.drepDelegators.items = []DRepDelegatorInfo{{Address: "stake_test1a", Amount: "7"}}
	node.drepDelegators.total = 1
	recorder := get(t, node, base+"/delegators?count=4")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, hash, node.drepDelegators.key)
	assert.Equal(t, 4, node.drepDelegators.params.Count)
	assert.JSONEq(t, `[{"address":"stake_test1a","amount":"7"}]`, recorder.Body.String())

	deposit := "500"
	node.drepUpdates.items = []DRepUpdateInfo{
		{TxHash: "aa", CertIndex: 0, Action: "registered", Deposit: &deposit},
		{TxHash: "bb", CertIndex: 1, Action: "updated"},
	}
	node.drepUpdates.total = 2
	recorder = get(t, node, base+"/updates")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `[{"tx_hash":"aa","cert_index":0,"action":"registered","deposit":"500"},{"tx_hash":"bb","cert_index":1,"action":"updated","deposit":null}]`, recorder.Body.String())

	node.drepVotes.items = []DRepVoteInfo{{TxHash: "cc", CertIndex: 2, ProposalID: "gov_action1x", ProposalTxHash: "dd", ProposalCertIndex: 1, Vote: "no"}}
	node.drepVotes.total = 1
	recorder = get(t, node, base+"/votes")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `[{"tx_hash":"cc","cert_index":2,"proposal_id":"gov_action1x","proposal_tx_hash":"dd","proposal_cert_index":1,"vote":"no"}]`, recorder.Body.String())

	bytesValue := "\\x7b7d"
	node.drepMetadata.value = DRepAnchorInfo{DRepID: hash, Hex: hash, URL: "u", Hash: "h", Bytes: &bytesValue, JSONMetadata: []byte(`{}`)}
	recorder = get(t, node, base+"/metadata")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `{"drep_id":"`+hash+`","hex":"`+hash+`","url":"u","hash":"h","json_metadata":{},"bytes":"\\x7b7d"}`, recorder.Body.String())

	node.drepMetadata.value = DRepAnchorInfo{DRepID: hash, Hex: hash, URL: "u", Hash: "h", Error: &OffchainFetchErrorInfo{Code: "CONNECTION_ERROR", Message: "m"}}
	recorder = get(t, node, base+"/metadata")
	assert.JSONEq(t, `{"drep_id":"`+hash+`","hex":"`+hash+`","url":"u","hash":"h","json_metadata":null,"bytes":null,"error":{"code":"CONNECTION_ERROR","message":"m"}}`, recorder.Body.String())

	for _, suffix := range []string{"/delegators", "/metadata", "/updates", "/votes"} {
		assert.Equal(t, http.StatusBadRequest, get(t, node, "/api/v0/governance/dreps/not-a-drep"+suffix).Code, suffix)
	}
	node.drepDelegators.err = ErrDRepNotFound
	node.drepMetadata.err = ErrDRepNotFound
	node.drepUpdates.err = ErrDRepNotFound
	node.drepVotes.err = ErrHistoryNotRetained
	assert.Equal(t, http.StatusNotFound, get(t, node, base+"/delegators").Code)
	assert.Equal(t, http.StatusNotFound, get(t, node, base+"/metadata").Code)
	assert.Equal(t, http.StatusNotFound, get(t, node, base+"/updates").Code)
	assert.Equal(t, http.StatusNotFound, get(t, node, base+"/votes").Code)
}

func TestHandlePoolVotes(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.poolVotes.items = []PoolVoteInfo{{TxHash: "aa", CertIndex: 1, Vote: "abstain"}}
	node.poolVotes.total = 1
	recorder := get(t, node, "/api/v0/pools/pool1xyz/votes")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "pool1xyz", node.poolVotes.key)
	assert.JSONEq(t, `[{"tx_hash":"aa","cert_index":1,"vote":"abstain"}]`, recorder.Body.String())
	node.poolVotes.err = ErrInvalidPoolID
	assert.Equal(t, http.StatusBadRequest, get(t, node, "/api/v0/pools/bad/votes").Code)
	node.poolVotes.err = models.ErrPoolNotFound
	assert.Equal(t, http.StatusNotFound, get(t, node, "/api/v0/pools/pool1xyz/votes").Code)
	node.poolVotes.err = ErrHistoryNotRetained
	assert.Equal(t, http.StatusNotFound, get(t, node, "/api/v0/pools/pool1xyz/votes").Code)
}

func TestHandleCommittee(t *testing.T) {
	t.Parallel()
	hotID, hotHex, script := "cc_hot1x", "02aa", false
	node := &mockNode{}
	node.committee.value = CommitteeInfo{
		QuorumNum: 2, QuorumDenom: 3,
		Members: []CommitteeMemberInfo{
			{ColdID: "cc_cold1a", ColdHex: "12bb", Status: "authorized", HotID: &hotID, HotHex: &hotHex, HotHasScript: &script, ExpirationEpoch: 500},
			{ColdID: "cc_cold1b", ColdHex: "13cc", ColdHasScript: true, Status: "not_authorized", ExpirationEpoch: 600},
		},
	}
	recorder := get(t, node, "/api/v0/governance/committee")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `{"gov_action_id":null,"proposal_tx_hash":null,"proposal_index":null,"is_dissolved":false,"quorum":{"numerator":2,"denominator":3},"members":[
{"cc_cold_id":"cc_cold1a","cc_cold_hex":"12bb","cc_cold_has_script":false,"cc_hot_id":"cc_hot1x","cc_hot_hex":"02aa","cc_hot_has_script":false,"status":"authorized","expiration_epoch":500},
{"cc_cold_id":"cc_cold1b","cc_cold_hex":"13cc","cc_cold_has_script":true,"cc_hot_id":null,"cc_hot_hex":null,"cc_hot_has_script":null,"status":"not_authorized","expiration_epoch":600}]}`, recorder.Body.String())
	node.committee.err = errors.New("boom")
	assert.Equal(t, http.StatusInternalServerError, get(t, node, "/api/v0/governance/committee").Code)
}

func TestHandleCommitteeVotes(t *testing.T) {
	t.Parallel()
	url, hash := "u", "h"
	node := &mockNode{}
	node.committeeVotes.items = []CommitteeVoteInfo{{
		TxHash: "aa", VoterHotID: "cc_hot1x", ProposalID: "gov_action1x",
		ProposalTxHash: "bb", ProposalIndex: 2, GovernanceType: "info_action",
		Vote: "yes", MetadataURL: &url, MetadataHash: &hash,
		BlockHeight: 10, BlockTime: 1700000000,
	}}
	node.committeeVotes.total = 1
	want := `[{"tx_hash":"aa","voter_hot_id":"cc_hot1x","proposal_id":"gov_action1x","proposal_tx_hash":"bb","proposal_index":2,"governance_type":"info_action","vote":"yes","metadata_url":"u","metadata_hash":"h","block_height":10,"block_time":1700000000}]`
	recorder := get(t, node, "/api/v0/governance/committee/votes")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, want, recorder.Body.String())
	assert.Equal(t, "", node.committeeVotes.key, "no credential selects every vote")

	hot := committeeCredentialID("cc_hot", false, bytes32of(28, 0x0a), false)
	cold := committeeCredentialID("cc_cold", true, bytes32of(28, 0x0b), true)
	recorder = get(t, node, "/api/v0/governance/committee/"+hot+"/votes")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, want, recorder.Body.String())
	assert.Equal(t, hexOf(bytes32of(28, 0x0a)), node.committeeVotes.key)
	assert.False(t, node.committeeCold)

	recorder = get(t, node, "/api/v0/governance/committee/"+cold+"/votes")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, hexOf(bytes32of(28, 0x0b))+"/script", node.committeeVotes.key)
	assert.True(t, node.committeeCold)

	drepLike := committeeCredentialID("drep", false, bytes32of(28, 0x0a), false)
	for _, id := range []string{"nope", drepLike} {
		assert.Equal(t, http.StatusBadRequest, get(t, node, "/api/v0/governance/committee/"+id+"/votes").Code, id)
	}
	node.committeeVotes.err = ErrHistoryNotRetained
	assert.Equal(t, http.StatusNotFound, get(t, node, "/api/v0/governance/committee/votes").Code)
}

func TestGovernanceListsRejectInvalidPaginationBeforeQuery(t *testing.T) {
	t.Parallel()
	txHash := hexOf(fill32(1))
	drep := "/api/v0/governance/dreps/" + hexOf(bytes32of(28, 1))
	for _, path := range []string{
		"/api/v0/governance/proposals",
		"/api/v0/governance/proposals/" + txHash + "/0/votes",
		"/api/v0/governance/proposals/" + txHash + "/0/withdrawals",
		"/api/v0/governance/committee/votes",
		drep + "/votes",
		drep + "/updates",
		drep + "/delegators",
		"/api/v0/pools/pool1xyz/votes",
		"/api/v0/accounts/stake1abc/history",
		"/api/v0/accounts/stake1abc/mirs",
		"/api/v0/accounts/stake1abc/addresses/assets",
		"/api/v0/epochs/1/stakes",
		"/api/v0/epochs/1/blocks",
	} {
		for _, query := range []string{"count=0", "count=101", "page=0", "order=sideways"} {
			if path == "/api/v0/epochs/1/stakes" && query == "order=sideways" {
				// The stakes schema defines no order; see
				// TestHandleUnorderedEpochListsIgnoreOrder.
				continue
			}
			t.Run(path+"?"+query, func(t *testing.T) {
				t.Parallel()
				node := &mockNode{}
				recorder := get(t, node, path+"?"+query)
				assert.Equal(t, http.StatusBadRequest, recorder.Code)
				assert.Equal(t, &mockNode{}, node, "the node was queried")
			})
		}
	}
}
