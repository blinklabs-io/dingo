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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	"github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olsq "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

const governanceBoundaryTimeout = 3 * time.Minute

type governanceSigningKey struct {
	vkey         []byte
	skey         []byte
	address      common.Address
	hash         common.Blake2b224
	envelopeType string
}

type governanceKeyEnvelope struct {
	Type    string `json:"type"`
	CborHex string `json:"cborHex"`
}

type governanceAddressInfo struct {
	Base16 string `json:"base16"`
}

type governanceLSQ struct {
	addr     string
	magic    uint32
	conn     *ouroboros.Connection
	client   *olsq.Client
	acquired bool
}

func TestConwaySPORatificationUsesBoundaryMarkAndEnactsNextEpoch(t *testing.T) {
	if os.Getenv("DEVNET_ACCELERATED") != "1" {
		t.Skip(
			"requires the accelerated DevNet; run",
			"internal/test/devnet/run-tests.sh --accelerated",
		)
	}

	keysDir := os.Getenv("DEVNET_GOVERNANCE_KEYS_DIR")
	require.NotEmpty(
		t,
		keysDir,
		"accelerated Dingo run must expose governance signing keys",
	)
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	require.Equal(
		t,
		3,
		cfg.PoolCount,
		"scenario expects the three producer pools",
	)

	ntcAddr := devnet.DingoNtcAddrs()["dingo-1"]
	require.NotEmpty(t, ntcAddr, "missing Dingo producer NtC address")
	query, err := newGovernanceLSQ(ntcAddr, cfg.NetworkMagic)
	require.NoError(t, err)
	t.Cleanup(query.Close)
	require.NoError(t, query.Refresh())
	if os.Getenv("DEVNET_GOVERNANCE_DEBUG") == "1" {
		epoch, err := query.epoch()
		require.NoError(t, err)
		proposals, err := query.proposals()
		require.NoError(t, err)
		t.Logf(
			"epoch %d has %d active governance actions",
			epoch,
			len(proposals),
		)
		for _, proposal := range proposals {
			t.Logf(
				"action %x/%d proposed=%d expires=%d SPO votes=%d",
				proposal.Id.TransactionId, proposal.Id.GovActionIdx,
				proposal.ProposedIn, proposal.ExpiresAfter,
				len(proposal.SPOVotes),
			)
		}
		return
	}
	stopGovernanceTxPump(t)

	poolKeys, err := loadGovernancePoolKeys(
		filepath.Join(keysDir, "pool-keys"), cfg.PoolCount,
	)
	require.NoError(t, err)
	stakeKeys, err := loadGovernanceStakeKeys(filepath.Join(keysDir, "stake"))
	require.NoError(t, err)
	paymentKeys, err := loadGovernancePaymentKeys(keysDir)
	require.NoError(t, err)
	require.NotEmpty(t, paymentKeys, "no genesis payment keys were exposed")

	params, err := query.conwayParams()
	require.NoError(t, err)
	require.Positive(
		t,
		params.GovActionDeposit,
		"genesis must configure a governance deposit",
	)

	delegations, err := query.delegations(stakeKeys)
	require.NoError(t, err)
	toMove := make([]*governanceSigningKey, 0)
	for _, key := range stakeKeys {
		credential := devnet.StakeKeyToCredential(key.vkey)
		if pool, ok := delegations[credential]; ok &&
			pool == poolKeys[1].hash {
			toMove = append(toMove, key)
		}
	}
	require.NotEmpty(
		t, toMove, "pool 2 must have signing-key-backed delegated stake",
	)

	returnCred := devnet.StakeKeyToCredential(toMove[0].vkey)
	returnAddress, err := common.NewAddressFromParts(
		common.AddressTypeNoneKey,
		common.AddressNetworkTestnet,
		nil,
		returnCred.Bytes.Bytes(),
	)
	require.NoError(t, err)

	waitForGovernanceWindow(t, query, cfg.EpochLength)
	startEpoch, err := query.epoch()
	require.NoError(t, err)

	noConfidenceWallet, noConfidenceUTxO, err := query.fundedWallet(
		paymentKeys, params.GovActionDeposit,
	)
	require.NoError(t, err)
	newCommitteeCredential := &common.Credential{
		CredType:   uint(common.CredentialTypeAddrKeyHash),
		Credential: common.CredentialHash(poolKeys[0].hash),
	}
	aboveThresholdAction := &common.NoConfidenceGovAction{
		Type: uint(common.GovActionTypeNoConfidence),
	}
	noConfidenceBody, err := governanceTransactionBody(
		noConfidenceUTxO, noConfidenceWallet.address,
	)
	require.NoError(t, err)
	noConfidenceBody.TxProposalProcedures = []conway.ConwayProposalProcedure{
		governanceProcedure(
			params.GovActionDeposit,
			returnAddress,
			common.GovActionTypeNoConfidence,
			aboveThresholdAction,
			"no-confidence-above",
		),
	}
	noConfidenceTx, noConfidenceProposalID, err := buildGovernanceTransaction(
		noConfidenceBody,
		noConfidenceUTxO.Output.OutputAmount.Amount,
		params.GovActionDeposit,
		params,
		noConfidenceWallet,
	)
	require.NoError(t, err)
	require.NoError(t, submitGovernanceTransaction(
		ntcAddr, cfg.NetworkMagic, noConfidenceTx,
	))
	waitForGovernance(
		t,
		query,
		45*time.Second,
		"no-confidence action enters the ledger",
		func() (bool, error) {
			proposals, err := query.proposals()
			if err != nil {
				return false, err
			}
			return hasGovAction(proposals, noConfidenceProposalID, 0), nil
		},
	)

	delegationWallet, delegationUTxO, err := query.fundedWallet(paymentKeys, 0)
	require.NoError(t, err)
	delegationBody, err := governanceTransactionBody(
		delegationUTxO, delegationWallet.address,
	)
	require.NoError(t, err)
	delegationBody.TxCertificates = make(
		[]common.CertificateWrapper, 0, len(toMove),
	)
	delegationWitnesses := []*governanceSigningKey{delegationWallet}
	for _, key := range toMove {
		cred := devnet.StakeKeyToCredential(key.vkey)
		certificateCred := common.Credential{
			CredType:   uint(cred.Tag),
			Credential: common.CredentialHash(cred.Bytes),
		}
		cert := &common.StakeDelegationCertificate{
			CertType:        uint(common.CertificateTypeStakeDelegation),
			StakeCredential: &certificateCred,
			PoolKeyHash:     poolKeys[2].hash,
		}
		delegationBody.TxCertificates = append(
			delegationBody.TxCertificates,
			common.CertificateWrapper{
				Type:        uint(common.CertificateTypeStakeDelegation),
				Certificate: cert,
			},
		)
		delegationWitnesses = append(delegationWitnesses, key)
	}
	delegationTx, _, err := buildGovernanceTransaction(
		delegationBody,
		delegationUTxO.Output.OutputAmount.Amount,
		0,
		params,
		delegationWitnesses...,
	)
	require.NoError(t, err)
	require.NoError(t, submitGovernanceTransaction(
		ntcAddr, cfg.NetworkMagic, delegationTx,
	))
	waitForGovernance(
		t,
		query,
		45*time.Second,
		"pool 2 stake delegates to pool 3",
		func() (bool, error) {
			current, err := query.delegations(toMove)
			if err != nil {
				return false, err
			}
			for _, key := range toMove {
				if current[devnet.StakeKeyToCredential(key.vkey)] != poolKeys[2].hash {
					return false, nil
				}
			}
			return true, nil
		},
	)

	submitGovernancePoolVotes(
		t,
		query,
		ntcAddr,
		cfg.NetworkMagic,
		paymentKeys,
		poolKeys,
		params,
		governanceActionID(noConfidenceProposalID, 0),
		map[int]struct{}{0: {}, 2: {}},
	)

	firstBoundaryEpoch := startEpoch + 1
	waitForGovernance(
		t,
		query,
		governanceBoundaryTimeout,
		"ratification boundary",
		func() (bool, error) {
			epoch, err := query.epoch()
			return err == nil && epoch >= firstBoundaryEpoch, err
		},
	)
	proposalsAfterRatification, err := query.proposals()
	require.NoError(t, err)
	require.True(t, hasGovAction(
		proposalsAfterRatification, noConfidenceProposalID, 0,
	))

	waitForGovernance(
		t,
		query,
		governanceBoundaryTimeout,
		"enactment boundary",
		func() (bool, error) {
			epoch, err := query.epoch()
			return err == nil && epoch >= firstBoundaryEpoch+1, err
		},
	)
	waitForGovernance(
		t,
		query,
		45*time.Second,
		"above-threshold no-confidence action is enacted",
		func() (bool, error) {
			proposals, err := query.proposals()
			if err != nil {
				return false, err
			}
			return !hasGovAction(proposals, noConfidenceProposalID, 0), nil
		},
	)

	committeeWallet, committeeUTxO, err := query.fundedWallet(
		paymentKeys, params.GovActionDeposit,
	)
	require.NoError(t, err)
	belowThresholdAction := &common.UpdateCommitteeGovAction{
		Type:        uint(common.GovActionTypeUpdateCommittee),
		ActionId:    governanceActionID(noConfidenceProposalID, 0),
		Credentials: []common.Credential{},
		CredEpochs: map[*common.Credential]uint64{
			newCommitteeCredential: uint64(startEpoch) + 10,
		},
		Quorum: cbor.Rat{Rat: big.NewRat(1, 2)},
	}
	committeeBody, err := governanceTransactionBody(
		committeeUTxO, committeeWallet.address,
	)
	require.NoError(t, err)
	committeeBody.TxProposalProcedures = []conway.ConwayProposalProcedure{
		governanceProcedure(
			params.GovActionDeposit,
			returnAddress,
			common.GovActionTypeUpdateCommittee,
			belowThresholdAction,
			"committee-update-below",
		),
	}
	committeeTx, committeeProposalID, err := buildGovernanceTransaction(
		committeeBody,
		committeeUTxO.Output.OutputAmount.Amount,
		params.GovActionDeposit,
		params,
		committeeWallet,
	)
	require.NoError(t, err)
	require.NoError(t, submitGovernanceTransaction(
		ntcAddr, cfg.NetworkMagic, committeeTx,
	))
	waitForGovernance(
		t,
		query,
		45*time.Second,
		"committee-update child enters the enacted no-confidence chain",
		func() (bool, error) {
			proposals, err := query.proposals()
			if err != nil {
				return false, err
			}
			return hasGovAction(proposals, committeeProposalID, 0), nil
		},
	)
	submitGovernancePoolVotes(
		t,
		query,
		ntcAddr,
		cfg.NetworkMagic,
		paymentKeys,
		poolKeys,
		params,
		governanceActionID(committeeProposalID, 0),
		map[int]struct{}{0: {}, 1: {}},
	)

	waitForGovernance(
		t,
		query,
		governanceBoundaryTimeout,
		"below-threshold child remains active through its enactment boundary",
		func() (bool, error) {
			epoch, err := query.epoch()
			return err == nil && epoch >= firstBoundaryEpoch+3, err
		},
	)
	proposalsAfterChildRatification, err := query.proposals()
	require.NoError(t, err)
	belowThresholdProposal, belowThresholdExists := findGovAction(
		proposalsAfterChildRatification, committeeProposalID, 0,
	)
	require.True(
		t,
		belowThresholdExists,
		"below-threshold child must not ratify and enact after the stake change",
	)
	require.Len(t, belowThresholdProposal.SPOVotes, 3)
}

func stopGovernanceTxPump(t *testing.T) {
	t.Helper()
	if os.Getenv("DEVNET_GOVERNANCE_TXPUMP_STOPPED") == "1" {
		return
	}
	control, err := devnet.NewNodeControl(t.Logf)
	require.NoError(t, err)
	stopCtx, stopCancel := context.WithTimeout(
		context.Background(), 15*time.Second,
	)
	defer stopCancel()
	require.NoError(t, control.Stop(stopCtx, "txpump-dingo"))
	t.Cleanup(func() {
		startCtx, startCancel := context.WithTimeout(
			context.Background(), 15*time.Second,
		)
		defer startCancel()
		if err := control.Start(startCtx, "txpump-dingo"); err != nil {
			t.Errorf("restart txpump-dingo: %v", err)
		}
	})
}

func submitGovernancePoolVotes(
	t *testing.T,
	query *governanceLSQ,
	addr string,
	magic uint32,
	paymentKeys []*governanceSigningKey,
	poolKeys []*governanceSigningKey,
	params *conway.ConwayProtocolParameters,
	actionID *common.GovActionId,
	yesPoolIndices map[int]struct{},
) {
	t.Helper()
	voteWallet, voteUTxO, err := query.fundedWallet(paymentKeys, 0)
	require.NoError(t, err)
	voteBody, err := governanceTransactionBody(voteUTxO, voteWallet.address)
	require.NoError(t, err)
	voteBody.TxVotingProcedures = make(common.VotingProcedures)
	for i, pool := range poolKeys {
		voter := &common.Voter{
			Type: common.VoterTypeStakingPoolKeyHash,
			Hash: [28]byte(pool.hash),
		}
		vote := common.GovVoteNo
		if _, ok := yesPoolIndices[i]; ok {
			vote = common.GovVoteYes
		}
		voteBody.TxVotingProcedures[voter] = map[*common.GovActionId]common.VotingProcedure{
			actionID: {Vote: vote},
		}
	}
	voteWitnesses := []*governanceSigningKey{voteWallet}
	for _, pool := range poolKeys {
		voteWitnesses = append(voteWitnesses, pool)
	}
	voteTx, _, err := buildGovernanceTransaction(
		voteBody,
		voteUTxO.Output.OutputAmount.Amount,
		0,
		params,
		voteWitnesses...,
	)
	require.NoError(t, err)
	require.NoError(t, submitGovernanceTransaction(addr, magic, voteTx))
	waitForGovernance(
		t,
		query,
		45*time.Second,
		"all three pool keys' votes enter the ledger",
		func() (bool, error) {
			proposals, err := query.proposals()
			if err != nil {
				return false, err
			}
			proposal, ok := findGovAction(
				proposals,
				common.Blake2b256(actionID.TransactionId),
				uint32(actionID.GovActionIdx),
			)
			return ok && len(proposal.SPOVotes) == len(poolKeys), nil
		},
	)
}

func waitForGovernanceWindow(
	t *testing.T,
	query *governanceLSQ,
	epochLength uint64,
) {
	t.Helper()
	initialEpoch, err := query.epoch()
	require.NoError(t, err)
	waitForGovernance(
		t,
		query,
		2*time.Minute,
		"enough slots remain for governance submissions",
		func() (bool, error) {
			point, err := query.point()
			if err != nil {
				return false, err
			}
			remaining := epochLength - point.Slot%epochLength
			if remaining >= 45 {
				return true, nil
			}
			epoch, err := query.epoch()
			return err == nil && epoch > initialEpoch && remaining >= 45, err
		},
	)
}

func newGovernanceLSQ(addr string, magic uint32) (*governanceLSQ, error) {
	if addr == "" {
		return nil, fmt.Errorf("empty LocalStateQuery address")
	}
	return &governanceLSQ{addr: addr, magic: magic}, nil
}

func (q *governanceLSQ) Close() {
	if q.conn == nil {
		return
	}
	if q.acquired && q.client != nil {
		_ = q.client.Release()
		q.acquired = false
	}
	_ = q.conn.Close()
	q.conn = nil
	q.client = nil
}

func (q *governanceLSQ) Refresh() error {
	q.Close()
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(q.magic),
		ouroboros.WithNodeToNode(false),
	)
	if err != nil {
		return fmt.Errorf("ouroboros.New: %w", err)
	}
	if err := conn.DialTimeout("tcp", q.addr, 10*time.Second); err != nil {
		_ = conn.Close()
		return fmt.Errorf("dial %s: %w", q.addr, err)
	}
	protocol := conn.LocalStateQuery()
	if protocol == nil || protocol.Client == nil {
		_ = conn.Close()
		return fmt.Errorf("LocalStateQuery unavailable at %s", q.addr)
	}
	q.conn = conn
	q.client = protocol.Client
	if err := q.client.AcquireVolatileTip(); err != nil {
		q.Close()
		return fmt.Errorf("acquire volatile tip: %w", err)
	}
	q.acquired = true
	return nil
}

func (q *governanceLSQ) epoch() (int, error) {
	if err := q.Refresh(); err != nil {
		return 0, err
	}
	return q.client.GetEpochNo()
}

func (q *governanceLSQ) point() (*pcommon.Point, error) {
	if err := q.Refresh(); err != nil {
		return nil, err
	}
	point, err := q.client.GetChainPoint()
	if err != nil {
		return nil, err
	}
	return point, nil
}

func (q *governanceLSQ) conwayParams() (
	*conway.ConwayProtocolParameters,
	error,
) {
	if err := q.Refresh(); err != nil {
		return nil, err
	}
	params, err := q.client.GetCurrentProtocolParams()
	if err != nil {
		return nil, err
	}
	conwayParams, ok := params.(*conway.ConwayProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"expected Conway protocol parameters, got %T",
			params,
		)
	}
	return conwayParams, nil
}

func (q *governanceLSQ) proposals() ([]olsq.GovActionState, error) {
	if err := q.Refresh(); err != nil {
		return nil, err
	}
	proposals, err := q.client.GetProposals()
	if err != nil {
		return nil, err
	}
	return *proposals, nil
}

func (q *governanceLSQ) delegations(
	keys []*governanceSigningKey,
) (map[olsq.StakeCredential]common.PoolKeyHash, error) {
	if err := q.Refresh(); err != nil {
		return nil, err
	}
	creds := make([]olsq.StakeCredential, 0, len(keys))
	for _, key := range keys {
		creds = append(creds, devnet.StakeKeyToCredential(key.vkey))
	}
	result, err := q.client.GetFilteredDelegationsAndRewardAccounts(creds)
	if err != nil {
		return nil, err
	}
	return result.Delegations, nil
}

func (q *governanceLSQ) fundedWallet(
	keys []*governanceSigningKey,
	minimum uint64,
) (*governanceSigningKey, governanceUTxO, error) {
	if err := q.Refresh(); err != nil {
		return nil, governanceUTxO{}, err
	}
	addresses := make([]gledger.Address, 0, len(keys))
	keyByAddress := make(map[string]*governanceSigningKey, len(keys))
	for _, key := range keys {
		addresses = append(addresses, key.address)
		addrBytes, err := key.address.Bytes()
		if err != nil {
			return nil, governanceUTxO{}, fmt.Errorf(
				"encode funding address: %w",
				err,
			)
		}
		keyByAddress[string(addrBytes)] = key
	}
	result, err := q.client.GetUTxOByAddress(addresses)
	if err != nil {
		return nil, governanceUTxO{}, err
	}
	ids := make([]olsq.UtxoId, 0, len(result.Results))
	for id := range result.Results {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if cmp := bytes.Compare(ids[i].Hash[:], ids[j].Hash[:]); cmp != 0 {
			return cmp < 0
		}
		return ids[i].Idx < ids[j].Idx
	})
	for _, id := range ids {
		output := result.Results[id]
		addressBytes, err := output.Address().Bytes()
		if err != nil {
			return nil, governanceUTxO{}, fmt.Errorf(
				"encode UTxO address: %w",
				err,
			)
		}
		key := keyByAddress[string(addressBytes)]
		if key == nil || output.OutputAmount.Amount < minimum+1_000_000 {
			continue
		}
		return key, governanceUTxO{ID: id, Output: output}, nil
	}
	return nil, governanceUTxO{}, fmt.Errorf(
		"no genesis wallet UTxO covers %d lovelace",
		minimum+1_000_000,
	)
}

type governanceUTxO struct {
	ID     olsq.UtxoId
	Output babbage.BabbageTransactionOutput
}

func loadGovernancePoolKeys(
	dir string,
	count int,
) ([]*governanceSigningKey, error) {
	keys := make([]*governanceSigningKey, 0, count)
	for i := 1; i <= count; i++ {
		prefix := filepath.Join(dir, fmt.Sprintf("pool-%d", i))
		key, err := loadGovernanceSigningKey(prefix, "")
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func loadGovernancePaymentKeys(dir string) ([]*governanceSigningKey, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "genesis.*.skey"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	keys := make([]*governanceSigningKey, 0, len(paths))
	for _, path := range paths {
		prefix := strings.TrimSuffix(path, ".skey")
		key, err := loadGovernanceSigningKey(prefix, prefix+".addr.info")
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func loadGovernanceStakeKeys(dir string) ([]*governanceSigningKey, error) {
	var paths []string
	err := filepath.WalkDir(
		dir,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".skey") {
				return nil
			}
			paths = append(paths, path)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	byHash := make(map[common.Blake2b224]*governanceSigningKey)
	for _, path := range paths {
		prefix := strings.TrimSuffix(path, ".skey")
		key, err := loadGovernanceSigningKey(prefix, "")
		if err != nil {
			continue
		}
		if !strings.Contains(key.envelopeType, "Stake") {
			continue
		}
		byHash[key.hash] = key
	}
	keys := make([]*governanceSigningKey, 0, len(byHash))
	for _, key := range byHash {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i].hash[:], keys[j].hash[:]) < 0
	})
	if len(keys) == 0 {
		return nil, fmt.Errorf("no stake signing keys found under %s", dir)
	}
	return keys, nil
}

func loadGovernanceSigningKey(
	prefix, addressPath string,
) (*governanceSigningKey, error) {
	skey, _, _, err := readGovernanceKeyFile(prefix + ".skey")
	if err != nil {
		return nil, err
	}
	vkey, envelopeType, _, err := readGovernanceKeyFile(prefix + ".vkey")
	if err != nil {
		return nil, err
	}
	if len(skey) != ed25519.SeedSize || len(vkey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("unexpected Ed25519 key size for %s", prefix)
	}
	derived := ed25519.NewKeyFromSeed(skey).Public().(ed25519.PublicKey)
	if !bytes.Equal(vkey, derived) {
		return nil, fmt.Errorf(
			"verification key does not match signing key for %s", prefix,
		)
	}
	key := &governanceSigningKey{
		vkey:         vkey,
		skey:         skey,
		hash:         common.Blake2b224Hash(vkey),
		envelopeType: envelopeType,
	}
	if addressPath != "" {
		data, err := os.ReadFile(
			addressPath, //nolint:gosec // isolated test key directory
		)
		if err != nil {
			return nil, err
		}
		var info governanceAddressInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf(
				"decode address info %s: %w",
				addressPath,
				err,
			)
		}
		addressBytes, err := hex.DecodeString(info.Base16)
		if err != nil {
			return nil, fmt.Errorf(
				"decode address info %s: %w",
				addressPath,
				err,
			)
		}
		key.address, err = common.NewAddressFromBytes(addressBytes)
		if err != nil {
			return nil, fmt.Errorf(
				"decode funding address %s: %w",
				addressPath,
				err,
			)
		}
	}
	return key, nil
}

func readGovernanceKeyFile(path string) ([]byte, string, string, error) {
	data, err := os.ReadFile(path) //nolint:gosec // isolated test key directory
	if err != nil {
		return nil, "", "", err
	}
	var envelope governanceKeyEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, "", "", fmt.Errorf("decode key envelope %s: %w", path, err)
	}
	raw, err := hex.DecodeString(envelope.CborHex)
	if err != nil {
		return nil, "", "", fmt.Errorf("decode key bytes %s: %w", path, err)
	}
	if len(raw) == 34 && raw[0] == 0x58 && raw[1] == 0x20 {
		raw = raw[2:]
	}
	return raw, envelope.Type, envelope.CborHex, nil
}

func governanceTransactionBody(
	utxo governanceUTxO,
	change common.Address,
) (*conway.ConwayTransactionBody, error) {
	networkID := uint8(common.AddressNetworkTestnet)
	inputs := []shelley.ShelleyTransactionInput{{
		TxId:        utxo.ID.Hash,
		OutputIndex: uint32(utxo.ID.Idx),
	}}
	encodedInputs, err := cbor.Encode(cbor.Tag{
		Number:  cbor.CborTagSet,
		Content: inputs,
	})
	if err != nil {
		return nil, fmt.Errorf("encode tagged transaction inputs: %w", err)
	}
	var inputSet conway.ConwayTransactionInputSet
	if _, err := cbor.Decode(encodedInputs, &inputSet); err != nil {
		return nil, fmt.Errorf("decode tagged transaction inputs: %w", err)
	}
	return &conway.ConwayTransactionBody{
		TxInputs: inputSet,
		TxOutputs: []babbage.BabbageTransactionOutput{{
			OutputAddress: change,
			OutputAmount:  mary.MaryTransactionOutputValue{},
		}},
		TxNetworkId: &networkID,
	}, nil
}

func governanceProcedure(
	deposit uint64,
	returnAddress common.Address,
	actionType common.GovActionType,
	action common.GovAction,
	anchorName string,
) conway.ConwayProposalProcedure {
	return conway.ConwayProposalProcedure{
		PPDeposit:       deposit,
		PPRewardAccount: returnAddress,
		PPGovAction: conway.ConwayGovAction{
			Type:   uint(actionType),
			Action: action,
		},
		PPAnchor: common.GovAnchor{
			Url:      "https://example.invalid/" + anchorName,
			DataHash: sha256.Sum256([]byte(anchorName)),
		},
	}
}

func buildGovernanceTransaction(
	body *conway.ConwayTransactionBody,
	inputValue uint64,
	deposit uint64,
	params *conway.ConwayProtocolParameters,
	keys ...*governanceSigningKey,
) ([]byte, common.Blake2b256, error) {
	if len(body.TxOutputs) != 1 {
		return nil, common.Blake2b256{}, fmt.Errorf(
			"expected one change output",
		)
	}
	fee := uint64(params.MinFeeB)
	for attempt := 0; attempt < 8; attempt++ {
		if inputValue < deposit+fee {
			return nil, common.Blake2b256{}, fmt.Errorf(
				"input cannot cover fee and deposits",
			)
		}
		body.TxFee = fee
		body.TxOutputs[0].OutputAmount.Amount = inputValue - deposit - fee
		body.SetCbor(nil)
		bodyBytes, err := cbor.Encode(body)
		if err != nil {
			return nil, common.Blake2b256{}, fmt.Errorf(
				"encode transaction body: %w", err,
			)
		}
		if !bytes.Contains(bodyBytes, []byte{0xd9, 0x01, 0x02}) {
			return nil, common.Blake2b256{}, fmt.Errorf(
				"transaction inputs are missing CBOR set tag 258",
			)
		}
		body.SetCbor(bodyBytes)
		bodyHash := common.Blake2b256Hash(bodyBytes)
		witnessSet := conway.ConwayTransactionWitnessSet{
			VkeyWitnesses: cbor.NewSetType(
				governanceWitnesses(bodyHash, keys),
				true,
			),
		}
		witnessBytes, err := cbor.Encode(&witnessSet)
		if err != nil {
			return nil, common.Blake2b256{}, fmt.Errorf(
				"encode witness set: %w",
				err,
			)
		}
		witnessSet.SetCbor(witnessBytes)
		tx := &conway.ConwayTransaction{
			Body:       *body,
			WitnessSet: witnessSet,
			TxIsValid:  true,
		}
		txBytes, err := cbor.Encode(tx)
		if err != nil {
			return nil, common.Blake2b256{}, fmt.Errorf(
				"encode transaction: %w",
				err,
			)
		}
		nextFee := uint64(params.MinFeeB) +
			uint64(params.MinFeeA)*uint64(len(txBytes))
		if nextFee == fee {
			return txBytes, bodyHash, nil
		}
		fee = nextFee
	}
	return nil, common.Blake2b256{}, fmt.Errorf(
		"transaction fee did not converge",
	)
}

func governanceWitnesses(
	hash common.Blake2b256,
	keys []*governanceSigningKey,
) []common.VkeyWitness {
	seen := make(map[string]struct{}, len(keys))
	witnesses := make([]common.VkeyWitness, 0, len(keys))
	for _, key := range keys {
		if key == nil {
			continue
		}
		if _, ok := seen[string(key.vkey)]; ok {
			continue
		}
		seen[string(key.vkey)] = struct{}{}
		privateKey := ed25519.NewKeyFromSeed(key.skey)
		witnesses = append(witnesses, common.VkeyWitness{
			Vkey:      key.vkey,
			Signature: ed25519.Sign(privateKey, hash[:]),
		})
	}
	return witnesses
}

func submitGovernanceTransaction(
	addr string,
	magic uint32,
	txBytes []byte,
) error {
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
	)
	if err != nil {
		return fmt.Errorf("ouroboros.New: %w", err)
	}
	defer conn.Close() //nolint:errcheck
	if err := conn.DialTimeout("tcp", addr, 10*time.Second); err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	protocol := conn.LocalTxSubmission()
	if protocol == nil || protocol.Client == nil {
		return fmt.Errorf("LocalTxSubmission unavailable at %s", addr)
	}
	if err := protocol.Client.SubmitTx(
		uint16(gledger.EraIdConway), txBytes,
	); err != nil {
		return fmt.Errorf("submit governance transaction: %w", err)
	}
	return nil
}

func waitForGovernance(
	t *testing.T,
	query *governanceLSQ,
	timeout time.Duration,
	description string,
	condition func() (bool, error),
) {
	t.Helper()
	require.Eventually(t, func() bool {
		ok, err := condition()
		if err != nil {
			t.Logf("waiting for %s: %v", description, err)
			return false
		}
		return ok
	}, timeout, 2*time.Second, description)
}

func governanceActionID(
	txID common.Blake2b256,
	index uint32,
) *common.GovActionId {
	return &common.GovActionId{
		TransactionId: [32]byte(txID),
		GovActionIdx:  index,
	}
}

func hasGovAction(
	actions []olsq.GovActionState,
	txID common.Blake2b256,
	index uint32,
) bool {
	_, ok := findGovAction(actions, txID, index)
	return ok
}

func findGovAction(
	actions []olsq.GovActionState,
	txID common.Blake2b256,
	index uint32,
) (olsq.GovActionState, bool) {
	for _, action := range actions {
		if action.Id.TransactionId == [32]byte(txID) &&
			action.Id.GovActionIdx == index {
			return action, true
		}
	}
	return olsq.GovActionState{}, false
}
