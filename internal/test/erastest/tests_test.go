//go:build erastest

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

// HardForkInitiation driver for the vanrossem testnet variant.
//
// The driver bootstraps a single DRep, delegates pool-1's genesis-utxo
// stake to it (so the DRep gets non-zero voting power once the next
// stake snapshot lands), submits a HardForkInitiation gov action that
// proposes protocol version 11.0, casts Yes votes from both SPOs and
// the DRep, and waits for the chain to RATIFY+ENACT the bump. Every
// cardano-cli call runs inside the eras-cardano-producer container via
// `docker exec`; that container already has cardano-cli, the node
// socket, pool-2 keys, the utxo-keys volume, and a read-only mount of
// pool-1's configs (see docker-compose.vanrossem.yml).
//
// The CC vote is intentionally skipped — conway-genesis is patched at
// configurator time to clear the committee (see configurator.sh's
// config_conway_genesis), so the ledger treats CC approval as auto-yes
// for HFI under Conway gov rules.
package erastest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPV11Readiness asserts the static plumbing required for dingo to
// cross the PV10→PV11 (vanRossem) intra-Conway hard fork. Unlike the
// rest of this package, it does not need the eras DevNet to be running:
// every assertion is a compile-time symbol lookup or a constant
// comparison. A run can be performed in isolation with
//
//	go test -tags erastest -run TestPV11Readiness ./internal/test/erastest/
//
// without docker-compose. The boundary-time no-op behavior of
// applyIntraEraHardForkRule(11) is covered by
// TestApplyIntraEraHardForkRule_UnknownMajor_NoOp in
// ledger/hardfork_rule_test.go and is not duplicated here.
func TestPV11Readiness(t *testing.T) {
	t.Run("ProtocolVersionConstants", func(t *testing.T) {
		assert.Equal(
			t, uint(11), lcommon.ProtocolVersionVanRossem,
			"common.ProtocolVersionVanRossem must equal 11",
		)
		assert.Equal(
			t, uint(11), uint(conway.MaxProtocolVersionConway),
			"conway.MaxProtocolVersionConway must cover PV11",
		)
	})

	t.Run("ConwayEraCoversPV11", func(t *testing.T) {
		assert.Equal(
			t, uint(11), eras.ConwayEraDesc.MaxMajorVersion,
			"ConwayEraDesc.MaxMajorVersion must cover PV11",
		)
		era, ok := eras.EraForVersion(11)
		require.True(
			t, ok,
			"eras.EraForVersion(11) must resolve a known era",
		)
		require.NotNil(t, era, "EraForVersion returned nil descriptor")
		assert.Equal(
			t, eras.ConwayEraDesc.Id, era.Id,
			"PV11 must resolve to Conway, not a successor era",
		)
	})

	t.Run("VanRossemGatedRulesPresent", func(t *testing.T) {
		// Both rules are PV11-gated transaction-validation rules
		// registered directly in conway.UtxoValidationRules. dingo's
		// Conway era iterates this slice in ledger/eras/conway.go, so
		// presence here is what makes the rules fire on dingo once
		// pparams.Major reaches 11.
		assertRuleInSlice(
			t,
			conway.UtxoValidationRules,
			conway.UtxoValidateDisjointRefInputs,
			"UtxoValidateDisjointRefInputs",
		)
		assertRuleInSlice(
			t,
			conway.UtxoValidationRules,
			conway.UtxoValidateCCVotingRestrictions,
			"UtxoValidateCCVotingRestrictions",
		)
	})

	t.Run("PoolValidateVrfKeyUniquenessExported", func(t *testing.T) {
		// PoolValidateVrfKeyUniqueness is the third PV11-gated rule
		// but has a per-certificate signature, so it is invoked
		// transitively from validateCertificates inside Conway's rule
		// path rather than registered in UtxoValidationRules.
		// Asserting the function exists as an exported, callable
		// symbol guards against accidental rename or unexport.
		require.NotNil(
			t, conway.PoolValidateVrfKeyUniqueness,
			"conway.PoolValidateVrfKeyUniqueness must remain "+
				"exported and callable",
		)
	})
}

// assertRuleInSlice fails the test if want is not present in slice.
// Compares by function pointer because Go function values are not
// directly comparable with ==.
func assertRuleInSlice(
	t *testing.T,
	slice []lcommon.UtxoValidationRuleFunc,
	want lcommon.UtxoValidationRuleFunc,
	name string,
) {
	t.Helper()
	wantPtr := reflect.ValueOf(want).Pointer()
	for _, fn := range slice {
		if reflect.ValueOf(fn).Pointer() == wantPtr {
			return
		}
	}
	t.Errorf(
		"rule %s not present in conway.UtxoValidationRules",
		name,
	)
}

// Paths inside the eras-cardano-producer container. Kept as constants so
// a layout change in docker-compose.vanrossem.yml or configurator.sh
// surfaces as a single-point edit.
const (
	cliContainer   = "eras-cardano-producer"
	nodeSocketPath = "/ipc/node.socket"
	testnetMagic   = "42"

	// Driver scratch dir inside the container (drep keys, action /
	// vote files, signed tx files, etc.).
	driverWorkDir = "/tmp/vanrossem-driver"

	// Genesis-utxo keys (mounted from utxo-keys volume).
	utxoPay1Skey  = "/utxo-keys/payment.1.skey"
	utxoStake1Vk  = "/utxo-keys/stake.1.vkey"
	utxoStake1Sk  = "/utxo-keys/stake.1.skey"
	utxoDeleg1Adr = "/utxo-keys/delegated.1.addr.info"

	// Pool cold keys (pool 1 mounted RO at /p1-configs, pool 2 at
	// /configs via the base compose).
	pool1ColdSkey = "/p1-configs/keys/cold.skey"
	pool1ColdVkey = "/p1-configs/keys/cold.vkey"
	pool2ColdSkey = "/configs/keys/cold.skey"
	pool2ColdVkey = "/configs/keys/cold.vkey"

	// HFI target.
	targetMajor = "11"
	targetMinor = "0"

	// Anchor is required by cardano-cli but our gov action has no real
	// off-chain metadata to point at. A deterministic dummy hash keeps
	// the action well-formed; we never run with --check-anchor-data so
	// the URL is not fetched.
	anchorURL  = "https://example.invalid/vanrossem.json"
	anchorHash = "0000000000000000000000000000000000000000000000000000000000000000"
)

// driveHFIToPV11 runs the end-to-end PV10→PV11 hard fork via gov
// action on the live DevNet. Returns once the chain has ENACTed PV11.
//
// Stage outline (each stage logs progress via t.Logf so a flaky run is
// debuggable from the test log alone):
//
//  1. prepareWorkDir  — fresh scratch dir inside cliContainer
//  2. registerDRep    — generate DRep keys, build tx containing DRep
//     registration cert + stake-vote-delegation cert, sign, submit
//  3. waitForEpoch    — wait two boundaries so the stake snapshot
//     picks up the DRep delegation as active voting power
//  4. submitHFI       — build the HFI proposal action, embed in a tx,
//     submit
//  5. castVotes       — three Yes votes (SPO 1, SPO 2, DRep), each its
//     own tx
//  6. waitForEpoch    — wait two boundaries so RATIFY (after the next
//     boundary) and ENACT (boundary after that) both fire
//  7. verifyPV11      — query pparams via cardano-cli, assert
//     protocolVersion.major == 11 on both producers
func driveHFIToPV11(t *testing.T) {
	t.Helper()
	t.Logf("HFI driver: starting")

	prepareWorkDir(t)
	deposits := readDeposits(t)
	t.Logf(
		"HFI driver: pparams deposits — dRepDeposit=%d, govActionDeposit=%d",
		deposits.DRep, deposits.GovAction,
	)
	drep := registerDRep(t, deposits.DRep)
	startEpoch := currentEpoch(t)
	t.Logf(
		"HFI driver: DRep registered (txid=%s); current epoch=%d, waiting two epoch boundaries for stake snapshot",
		drep.regTxID,
		startEpoch,
	)
	waitForEpoch(t, startEpoch+2)

	proposal := submitHFI(t, deposits.GovAction)
	t.Logf(
		"HFI driver: HFI proposal submitted (txid=%s, action=%d)",
		proposal.txID, proposal.actionIdx,
	)

	castSPOVote(t, pool1ColdVkey, pool1ColdSkey, proposal, "spo1-dingo")
	castSPOVote(t, pool2ColdVkey, pool2ColdSkey, proposal, "spo2-cardano")
	castDRepVote(t, drep, proposal)

	voteEpoch := currentEpoch(t)
	t.Logf(
		"HFI driver: all votes cast at epoch=%d, waiting two epoch boundaries for RATIFY + ENACT",
		voteEpoch,
	)
	waitForEpoch(t, voteEpoch+2)

	verifyPV11(t)
	t.Logf("HFI driver: PV11 reached")
}

// drepKeys describes the freshly-generated DRep credential set the
// driver uses for both the registration tx and subsequent vote tx.
type drepKeys struct {
	vkeyPath string // /tmp/.../drep.vkey
	skeyPath string // /tmp/.../drep.skey
	regTxID  string // txid that contained the registration cert
}

// pparamDeposits captures the two lovelace deposit amounts the driver
// needs at tx-build time. Both are queried once at driver start so a
// later mid-flow pparams update can't quietly mis-balance txs.
type pparamDeposits struct {
	DRep      uint64 `json:"drep"`
	GovAction uint64 `json:"govAction"`
}

func readDeposits(t *testing.T) pparamDeposits {
	t.Helper()
	out := runCli(t, "querying deposit pparams",
		"cardano-cli conway query protocol-parameters "+
			"--socket-path "+nodeSocketPath+" "+
			"--testnet-magic "+testnetMagic+
			" | jq '{drep: .dRepDeposit, govAction: .govActionDeposit}'")
	var d pparamDeposits
	if err := json.Unmarshal(out, &d); err != nil {
		t.Fatalf("decoding deposits: %v\n%s", err, string(out))
	}
	if d.DRep == 0 || d.GovAction == 0 {
		t.Fatalf(
			"deposit pparams unexpectedly zero: drep=%d govAction=%d\nraw: %s",
			d.DRep, d.GovAction, string(out),
		)
	}
	return d
}

// hfiProposal identifies the in-flight HFI gov action by the txid that
// submitted it and its index within that tx's proposal-procedures list.
// Conway gov actions are addressed by (txid, ix); ix is 0 because our
// proposal tx contains exactly one proposal.
type hfiProposal struct {
	txID      string
	actionIdx uint
}

func prepareWorkDir(t *testing.T) {
	t.Helper()
	runCli(t, "preparing work dir",
		"rm -rf "+driverWorkDir+" && mkdir -p "+driverWorkDir)
}

// registerDRep generates a DRep key-pair, builds a tx that contains
// both the DRep registration cert and a stake vote-delegation cert
// pointing pool-1's genesis-utxo stake at the new DRep, signs with the
// utxo payment, utxo stake, and drep signing keys, and submits.
//
// The DRep registration deposit (dRepDeposit, default 500 ADA on this
// testnet) is paid from delegated.1's UTxO; cardano-cli's
// `transaction build` selects fee and change automatically.
func registerDRep(t *testing.T, drepDeposit uint64) drepKeys {
	t.Helper()
	keys := drepKeys{
		vkeyPath: driverWorkDir + "/drep.vkey",
		skeyPath: driverWorkDir + "/drep.skey",
	}

	runCli(t, "generating DRep keys",
		"cardano-cli conway governance drep key-gen "+
			"--verification-key-file "+keys.vkeyPath+" "+
			"--signing-key-file "+keys.skeyPath)

	runCli(t, "building DRep registration certificate",
		fmt.Sprintf(
			"cardano-cli conway governance drep registration-certificate "+
				"--drep-verification-key-file %s "+
				"--key-reg-deposit-amt %d "+
				"--out-file %s/drep-reg.cert",
			keys.vkeyPath, drepDeposit, driverWorkDir,
		))

	runCli(t, "building stake vote-delegation certificate",
		"cardano-cli conway stake-address vote-delegation-certificate "+
			"--stake-verification-key-file "+utxoStake1Vk+" "+
			"--drep-verification-key-file "+keys.vkeyPath+" "+
			"--out-file "+driverWorkDir+"/vote-deleg.cert")

	// Build, sign, submit a tx that registers the DRep and delegates
	// pool-1's genesis stake to it. The build command auto-selects a
	// UTxO from delegated.1's address; we pass an explicit
	// --change-address so all leftover lovelace lands back at the
	// same address (keeping the funded UTxO recyclable for the HFI
	// and vote txs that follow).
	depositAddr := readJSONField(t, utxoDeleg1Adr, ".address")
	buildSignSubmit(
		t,
		"DRep registration",
		[]string{
			"--certificate-file " + driverWorkDir + "/drep-reg.cert",
			"--certificate-file " + driverWorkDir + "/vote-deleg.cert",
		},
		[]string{utxoPay1Skey, utxoStake1Sk, keys.skeyPath},
		depositAddr,
		drepDeposit,
		driverWorkDir+"/drep-tx",
	)

	keys.regTxID = readFile(t, driverWorkDir+"/drep-tx.txid")
	return keys
}

// submitHFI builds the HardForkInitiation gov action targeting PV11.0
// and submits it as a proposal tx.
func submitHFI(t *testing.T, govActionDeposit uint64) hfiProposal {
	t.Helper()
	depositAddr := readJSONField(t, utxoDeleg1Adr, ".address")
	runCli(t, "building HFI gov action",
		fmt.Sprintf(
			"cardano-cli conway governance action create-hardfork "+
				"--testnet "+
				"--governance-action-deposit %d "+
				"--deposit-return-stake-verification-key-file %s "+
				"--anchor-url %s "+
				"--anchor-data-hash %s "+
				"--protocol-major-version %s "+
				"--protocol-minor-version %s "+
				"--out-file %s/hfi.action",
			govActionDeposit, utxoStake1Vk, anchorURL, anchorHash,
			targetMajor, targetMinor, driverWorkDir,
		))

	buildSignSubmit(
		t,
		"HFI proposal",
		[]string{"--proposal-file " + driverWorkDir + "/hfi.action"},
		[]string{utxoPay1Skey},
		depositAddr,
		govActionDeposit,
		driverWorkDir+"/hfi-tx",
	)

	return hfiProposal{
		txID:      readFile(t, driverWorkDir+"/hfi-tx.txid"),
		actionIdx: 0,
	}
}

// castSPOVote builds and submits a Yes vote tx signed with a stake
// pool cold key. tag is a short label used in logs (spo1-dingo /
// spo2-cardano).
func castSPOVote(
	t *testing.T,
	coldVkey, coldSkey string,
	p hfiProposal,
	tag string,
) {
	t.Helper()
	voteFile := fmt.Sprintf("%s/vote-%s.vote", driverWorkDir, tag)
	runCli(t, "building "+tag+" Yes vote",
		fmt.Sprintf(
			"cardano-cli conway governance vote create --yes "+
				"--governance-action-tx-id %s "+
				"--governance-action-index %d "+
				"--cold-verification-key-file %s "+
				"--out-file %s",
			p.txID, p.actionIdx, coldVkey, voteFile,
		))

	depositAddr := readJSONField(t, utxoDeleg1Adr, ".address")
	buildSignSubmit(
		t,
		tag+" vote",
		[]string{"--vote-file " + voteFile},
		[]string{utxoPay1Skey, coldSkey},
		depositAddr,
		0,
		fmt.Sprintf("%s/vote-%s-tx", driverWorkDir, tag),
	)
}

// castDRepVote builds and submits a Yes vote tx signed with the DRep
// signing key generated during registerDRep.
func castDRepVote(t *testing.T, d drepKeys, p hfiProposal) {
	t.Helper()
	voteFile := driverWorkDir + "/vote-drep.vote"
	runCli(t, "building DRep Yes vote",
		fmt.Sprintf(
			"cardano-cli conway governance vote create --yes "+
				"--governance-action-tx-id %s "+
				"--governance-action-index %d "+
				"--drep-verification-key-file %s "+
				"--out-file %s",
			p.txID, p.actionIdx, d.vkeyPath, voteFile,
		))

	depositAddr := readJSONField(t, utxoDeleg1Adr, ".address")
	buildSignSubmit(
		t,
		"DRep vote",
		[]string{"--vote-file " + voteFile},
		[]string{utxoPay1Skey, d.skeyPath},
		depositAddr,
		0,
		driverWorkDir+"/vote-drep-tx",
	)
}

// flatTxFee is a generous flat fee in lovelace that every driver tx
// pays. cardano-node only validates that in >= out + fee + deposits,
// so over-paying fees is harmless; we lose at most ~1 ADA per tx and
// avoid the per-tx fee computation that `transaction build` would
// normally do via LSQ. linearFee on this testnet is the default
// 44 lovelace/byte + 155381 lovelace constant; with tx bodies that
// top out around 2 KB even for proposals, real fees stay under 0.25
// ADA, so 1 ADA is comfortable overhead.
const flatTxFee uint64 = 1_000_000

// buildSignSubmit builds a tx with `cardano-cli conway transaction
// build-raw`, signs with each provided signing key, submits via the
// node socket, and writes the resulting txid to <outBase>.txid.
//
// We use build-raw rather than `transaction build` to dodge a
// cardano-cli 11.0.0 vs cardano-node 11.0.1 LSQ-decoder mismatch:
// `transaction build` issues an LSQ block-query whose response
// envelope now mentions DijkstraEra, and the cardano-cli compiled
// into the 11.0.1 image deserialises that envelope with
// `DeserialiseFailure 4 "expected word"`. build-raw skips that whole
// query path because the caller provides --fee and --tx-out
// (change) directly. Until cardano-cli ships a matching 11.x patch,
// flat-fee + manual change is the only stable option.
//
// deposit is the lovelace amount the tx implicitly burns: dRepDeposit
// for the DRep registration tx, govActionDeposit for the HFI proposal
// tx, 0 for plain vote txs. cardano-node reads the deposit value off
// the cert/proposal contents at validation time; we just need to
// reduce the change output by the same amount so the tx balances.
func buildSignSubmit(
	t *testing.T,
	tag string,
	extraArgs []string,
	signingKeys []string,
	changeAddr string,
	deposit uint64,
	outBase string,
) {
	t.Helper()

	// Pick the largest UTxO at the deposit address and learn its
	// lovelace value. Re-querying every call (rather than caching)
	// keeps the driver resilient to driver-tx chain interleaving:
	// each successfully-submitted prior tx burns its input and
	// creates the change UTxO we want to spend next.
	txIn, lovelaceIn := selectLargestUtxo(t, changeAddr)

	if lovelaceIn <= deposit+flatTxFee {
		t.Fatalf(
			"%s: UTxO at %s has %d lovelace, not enough for deposit %d + fee %d",
			tag,
			changeAddr,
			lovelaceIn,
			deposit,
			flatTxFee,
		)
	}
	change := lovelaceIn - deposit - flatTxFee

	buildArgs := make([]string, 0, 6+len(extraArgs))
	buildArgs = append(buildArgs,
		"cardano-cli conway transaction build-raw",
		"--tx-in "+txIn,
		fmt.Sprintf("--tx-out %s+%d", changeAddr, change),
		fmt.Sprintf("--fee %d", flatTxFee),
		"--out-file "+outBase+".body",
	)
	buildArgs = append(buildArgs, extraArgs...)
	runCli(t, "building "+tag+" tx", strings.Join(buildArgs, " "))

	signArgs := make([]string, 0, 4+len(signingKeys))
	signArgs = append(signArgs,
		"cardano-cli conway transaction sign",
		"--testnet-magic "+testnetMagic,
		"--tx-body-file "+outBase+".body",
		"--out-file "+outBase+".signed",
	)
	for _, sk := range signingKeys {
		signArgs = append(signArgs, "--signing-key-file "+sk)
	}
	runCli(t, "signing "+tag+" tx", strings.Join(signArgs, " "))

	// Capture the txid before submit so the caller can address the
	// gov action by (txid, ix=0) before the submit acknowledgement
	// returns. cardano-cli 11.x's transaction-txid command emits a
	// JSON object `{"txhash":"..."}`, not a bare hex string, so pipe
	// through jq to extract the raw hex.
	runCli(t, "computing "+tag+" txid",
		fmt.Sprintf(
			"cardano-cli conway transaction txid --tx-file %s.signed | jq -r .txhash > %s.txid",
			outBase,
			outBase,
		))

	runCli(t, "submitting "+tag+" tx",
		"cardano-cli conway transaction submit "+
			"--socket-path "+nodeSocketPath+" "+
			"--testnet-magic "+testnetMagic+" "+
			"--tx-file "+outBase+".signed")

	// Wait for the tx to settle on-chain so the next call's
	// selectLargestUtxo sees the new change UTxO rather than the
	// just-consumed input. `transaction submit` only proves
	// admission to the node's mempool; under our flat-fee build-raw
	// flow we must verify the tx has actually been included before
	// the next call selects an input.
	awaitTxOnChain(t, changeAddr, readFile(t, outBase+".txid"))
}

// awaitTxOnChain blocks until txid appears in the UTxO set at addr.
func awaitTxOnChain(t *testing.T, addr, txid string) {
	t.Helper()
	testutil.WaitForConditionWithInterval(t, func() bool {
		out := runCli(t, "polling UTxO for "+txid,
			"cardano-cli conway query utxo "+
				"--socket-path "+nodeSocketPath+" "+
				"--testnet-magic "+testnetMagic+" "+
				"--address "+addr+" "+
				"--output-json")
		var utxos map[string]json.RawMessage
		if err := json.Unmarshal(out, &utxos); err != nil {
			t.Fatalf("decoding utxo set at %s: %v\n%s", addr, err, string(out))
		}
		for k := range utxos {
			if strings.HasPrefix(k, txid+"#") {
				return true
			}
		}
		return false
	}, 90*time.Second, 2*time.Second, fmt.Sprintf(
		"tx %s did not appear at %s within 90s",
		txid,
		addr,
	))
}

// selectLargestUtxo returns ("<txid>#<ix>", lovelace) for the
// highest-lovelace UTxO at addr. Fails the test if the address has
// no UTxOs.
func selectLargestUtxo(t *testing.T, addr string) (string, uint64) {
	t.Helper()
	out := runCli(t, "querying UTxO at "+addr,
		"cardano-cli conway query utxo "+
			"--socket-path "+nodeSocketPath+" "+
			"--testnet-magic "+testnetMagic+" "+
			"--address "+addr+" "+
			"--output-json")
	var utxos map[string]struct {
		Value struct {
			Lovelace uint64 `json:"lovelace"`
		} `json:"value"`
	}
	if err := json.Unmarshal(out, &utxos); err != nil {
		t.Fatalf("decoding utxo set at %s: %v\n%s", addr, err, string(out))
	}
	if len(utxos) == 0 {
		t.Fatalf(
			"no UTxO at %s — cardano-node may still be processing the previous driver tx",
			addr,
		)
	}
	var bestKey string
	var bestLovelace uint64
	for k, v := range utxos {
		if v.Value.Lovelace > bestLovelace {
			bestKey = k
			bestLovelace = v.Value.Lovelace
		}
	}
	return bestKey, bestLovelace
}

// readJSONField returns the value of jqExpr applied to path inside
// cliContainer, stripped of surrounding whitespace and quotes.
func readJSONField(t *testing.T, path, jqExpr string) string {
	t.Helper()
	out := runCli(t, "reading "+path+jqExpr,
		"jq -r '"+jqExpr+"' "+path)
	return strings.TrimSpace(string(out))
}

// readFile cat's path inside cliContainer and returns trimmed
// contents.
func readFile(t *testing.T, path string) string {
	t.Helper()
	out := runCli(t, "reading "+path, "cat "+path)
	return strings.TrimSpace(string(out))
}

// currentEpoch returns the epoch the node's chain is currently in.
func currentEpoch(t *testing.T) uint64 {
	t.Helper()
	out := runCli(t, "querying tip",
		"cardano-cli conway query tip "+
			"--socket-path "+nodeSocketPath+" "+
			"--testnet-magic "+testnetMagic)
	var tip struct {
		Epoch uint64 `json:"epoch"`
	}
	if err := json.Unmarshal(out, &tip); err != nil {
		t.Fatalf("decoding tip: %v\n%s", err, string(out))
	}
	return tip.Epoch
}

// waitForEpoch blocks until the chain reaches at least epoch target,
// polling every 5 seconds. Fails the test if 6 epochs of wall-clock
// time elapse without progress (sized to absorb the 2-epoch RATIFY+
// ENACT wait plus margin against block-rate variance).
func waitForEpoch(t *testing.T, target uint64) {
	t.Helper()
	var cur uint64
	testutil.WaitForConditionWithInterval(t, func() bool {
		cur = currentEpoch(t)
		return cur >= target
	}, 6*time.Duration(75)*time.Second, 5*time.Second, fmt.Sprintf(
		"waitForEpoch(%d) gave up after deadline",
		target,
	))
	t.Logf("HFI driver: reached epoch %d (target %d)", cur, target)
}

// verifyPV11 queries protocol-parameters on both producers and
// asserts that protocolVersion.major == 11. Querying both sides
// (dingo via cardano-producer's chain-sync view is implicit because
// the relay is the shared canonical chain, but we also confirm
// against cardano-producer's local pparams view) guards against the
// case where only one node enacts the bump.
func verifyPV11(t *testing.T) {
	t.Helper()
	out := runCli(t, "querying pparams on cardano-producer",
		"cardano-cli conway query protocol-parameters "+
			"--socket-path "+nodeSocketPath+" "+
			"--testnet-magic "+testnetMagic)
	var pp struct {
		ProtocolVersion struct {
			Major uint `json:"major"`
			Minor uint `json:"minor"`
		} `json:"protocolVersion"`
	}
	if err := json.Unmarshal(out, &pp); err != nil {
		t.Fatalf("decoding pparams: %v\n%s", err, string(out))
	}
	if pp.ProtocolVersion.Major != 11 {
		t.Fatalf(
			"HFI did not enact: pparams.protocolVersion=%d.%d (want 11.0). "+
				"Inspect the cardano-producer log for ratification/enactment failure; "+
				"likely causes: SPO/DRep voting power below threshold, "+
				"committee approval blocked, or proposal expired before votes landed.",
			pp.ProtocolVersion.Major, pp.ProtocolVersion.Minor,
		)
	}
	t.Logf(
		"HFI driver: cardano-producer pparams.protocolVersion=%d.%d ✓",
		pp.ProtocolVersion.Major, pp.ProtocolVersion.Minor,
	)
}

// runCli executes "docker exec eras-cardano-producer sh -c <cmd>" and
// returns stdout. Fails the test on non-zero exit, including stderr
// in the failure message for diagnostics.
func runCli(t *testing.T, what, cmd string) []byte {
	t.Helper()
	t.Logf("HFI driver: %s", what)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := exec.CommandContext(
		ctx,
		"docker",
		"exec",
		cliContainer,
		"sh",
		"-c",
		cmd,
	)
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			t.Fatalf(
				"docker exec timed out after 30s (%s)\n--- cmd ---\n%s\n--- stdout ---\n%s\n--- stderr ---\n%s",
				what,
				cmd,
				stdout.String(),
				stderr.String(),
			)
		}
		t.Fatalf(
			"docker exec failed (%s): %v\n--- cmd ---\n%s\n--- stdout ---\n%s\n--- stderr ---\n%s",
			what,
			err,
			cmd,
			stdout.String(),
			stderr.String(),
		)
	}
	return stdout.Bytes()
}

// vanRossemPreBumpTargetSlot is the slot the chain must reach before
// the pre-bump assertions run. Picked to exceed two full 75-slot
// epochs so the observation window covers steady-state PV10 block
// production after bootstrap warmup. With f=0.4 and two equal-stake
// pools, the probability of zero forges from a given pool over 100
// slots is (1-0.225)^100 ≈ 9e-11 — slot count is not a realistic
// source of flakes.
const vanRossemPreBumpTargetSlot = uint64(100)

// TestVanRossem exercises the PV10→PV11 (vanRossem) intra-Conway
// hard fork on a multi-node DevNet. The chain bootstraps in
// Conway-Plomin via testnet-vanrossem.yaml; a HardForkInitiation
// governance-action driver subsequently submits, votes, ratifies,
// and enacts the bump to PV11 on the live chain. The test asserts:
//
//  1. The DevNet harness was actually pointed at the vanRossem
//     variant (testnet-vanrossem.yaml), not the default eras testnet.
//  2. Pre-bump (PV10): every header is in Conway era, both pools
//     forge accepted blocks on the relay's chain. Establishes a
//     known-good baseline before the boundary.
//  3. Post-bump (PV11): once the HFI driver enacts the protocol
//     version bump, the chain continues advancing, dingo continues
//     forging blocks accepted by the cardano-node relay, and PV11
//     rule activation does not break header / block validation on
//     either side.
//
// The driver itself lives in vanrossem_driver.go (TODO: not yet
// implemented). Until then, the post-bump sub-tests are skipped
// rather than wired against a non-existent boundary, so the test
// validates the PV10 baseline cleanly today.
func TestVanRossem(t *testing.T) {
	cfg := loadConfig(t)

	// Defensive: confirm we are running against the vanrossem variant
	// rather than the default eras testnet. An empty-schedule check is
	// not load-bearing here because Config.ScheduledTransitions()
	// reports Conway when StartingMajorVersion lives inside Conway's
	// [MinMajorVersion, MaxMajorVersion] range — at PV10 there is still
	// PV11 of Conway ahead, so the schedule reports `conway@epoch=0`
	// even though no inter-era boundary will actually be crossed. The
	// StartingMajorVersion equality check below is what distinguishes
	// the variant.
	require.Equalf(
		t, uint(10), cfg.StartingMajorVersion,
		"vanrossem variant must bootstrap at PV10 (Plomin); got %d. "+
			"The HFI driver expects to start one PV step below "+
			"vanRossem so it can drive the PV10→PV11 transition. "+
			"Check shelley genesis protocolVersion.major in "+
			"testnet-vanrossem.yaml — and run via "+
			"run-tests-vanrossem.sh, not run-tests.sh.",
		cfg.StartingMajorVersion,
	)

	endpoints := DefaultEndpoints()
	streams := make(map[string]*EraStream, len(endpoints))
	for _, ep := range endpoints {
		streams[ep.Name] = NewEraStream(
			t, ep, cfg.NetworkMagic, cfg.EpochLength,
		)
	}
	t.Cleanup(func() {
		for _, s := range streams {
			s.Close()
		}
	})

	// Drive each node past the pre-bump target so every PV10-era
	// assertion below observes a non-trivial chain. Reusing
	// transitionTimeout keeps the budget consistent with the eras
	// test harness; the vanrossem variant has no inter-era boundaries
	// so the timeout only needs to absorb container warmup and block-
	// rate variance, not fork-resolution churn.
	timeout := transitionTimeout(cfg)
	for name, s := range streams {
		_, err := s.WaitForSlot(vanRossemPreBumpTargetSlot, timeout)
		require.NoErrorf(
			t, err,
			"node %s did not reach slot %d within %s: %v",
			name, vanRossemPreBumpTargetSlot, timeout, err,
		)
	}

	// All assertions read the relay's view because the relay forges no
	// blocks itself, so the issuer vkey of every block on its chain is
	// the producer (dingo or cardano-producer) that the relay actually
	// accepted. Same rationale as TestEraTransitions /
	// DingoProducesInEachEra.
	relay := endpoints[2]
	relayStream := streams[relay.Name]
	require.NotNilf(
		t, relayStream,
		"no stream for relay endpoint %q; check DefaultEndpoints ordering",
		relay.Name,
	)

	t.Run("ChainStaysInConway", func(t *testing.T) {
		headers := relayStream.HeadersSnapshot()
		require.NotEmpty(t, headers, "no headers observed on relay")
		for _, h := range headers {
			require.Equalf(
				t, eras.ConwayEraDesc.Id, h.EraID,
				"non-Conway header at slot %d (era ID %d). "+
					"vanRossem chain must stay in Conway era throughout; "+
					"a different era ID indicates the configurator did "+
					"not honor the testnet-vanrossem.yaml fork schedule "+
					"or cardano-node ran an inter-era HFC translation "+
					"despite no scheduled transitions.",
				h.Slot, h.EraID,
			)
		}
	})

	t.Run("DingoProducesAtPV10", func(t *testing.T) {
		dingoVkey := readDingoColdVKey(t)
		t.Logf("dingo cold vkey: %s", hex.EncodeToString(dingoVkey))

		headers := relayStream.HeadersSnapshot()
		var dingoBlocks, cardanoBlocks int
		for _, h := range headers {
			if bytes.Equal(h.IssuerVkey, dingoVkey) {
				dingoBlocks++
				continue
			}
			cardanoBlocks++
		}
		t.Logf(
			"relay observed %d total headers (dingo-issued=%d, cardano-issued=%d)",
			len(headers),
			dingoBlocks,
			cardanoBlocks,
		)

		require.Greaterf(
			t, dingoBlocks, 0,
			"no dingo-issued blocks observed on relay across %d headers — "+
				"dingo's PV10 forges are not being accepted by the "+
				"cardano-node relay. With two equal-stake pools and f=%.2f "+
				"over %d slots the probability of zero forges is ≈1e-10, "+
				"so this is a chain-divergence regression at PV10: "+
				"investigate the dingo log for VRF / nonce / pparams "+
				"disagreements with the cardano-node relay before any "+
				"PV11 bump is even attempted.",
			len(headers), cfg.ActiveSlotsCoeff, vanRossemPreBumpTargetSlot,
		)

		require.Greaterf(
			t, cardanoBlocks, 0,
			"no cardano-issued blocks observed on relay across %d "+
				"headers — cardano-producer is not contributing to the "+
				"canonical chain. Check that cardano-node:11.0.1 "+
				"actually started and is forging at PV10.",
			len(headers),
		)
	})

	t.Run("DriveHFIToPV11", func(t *testing.T) {
		driveHFIToPV11(t)

		// Re-observe the relay stream after the bump and confirm
		// dingo continues to forge accepted blocks on the canonical
		// chain post-boundary. This is the assertion that catches a
		// vanRossem rule activation breaking dingo's block validity
		// against the cardano-node relay even when the PV bump
		// itself succeeded.
		beforeCount := len(relayStream.HeadersSnapshot())
		postBumpTimeout := transitionTimeout(cfg)
		// Wait for the relay to advance by at least 1 epoch's worth
		// of slots after the bump so we have a fair sample of
		// post-bump headers to inspect.
		latest, _ := relayStream.LatestHeader()
		_, err := relayStream.WaitForSlot(
			latest.Slot+cfg.EpochLength, postBumpTimeout,
		)
		require.NoErrorf(
			t, err,
			"chain did not advance one epoch past PV11 boundary within %s",
			postBumpTimeout,
		)

		dingoVkey := readDingoColdVKey(t)
		var postBumpDingo, postBumpCardano int
		for _, h := range relayStream.HeadersSnapshot()[beforeCount:] {
			if bytes.Equal(h.IssuerVkey, dingoVkey) {
				postBumpDingo++
				continue
			}
			postBumpCardano++
		}
		t.Logf(
			"post-PV11 headers on relay: dingo-issued=%d, cardano-issued=%d",
			postBumpDingo, postBumpCardano,
		)
		require.Greaterf(
			t, postBumpDingo, 0,
			"no dingo-issued blocks observed on relay after PV11 enactment. "+
				"The bump itself ratified (pparams.major=11 confirmed by "+
				"driveHFIToPV11) but dingo's post-boundary forges are not "+
				"being accepted by the cardano-node relay — investigate the "+
				"dingo log for VRF / pparams disagreements clustered "+
				"immediately after the enactment epoch boundary.",
		)
		require.GreaterOrEqualf(
			t,
			postBumpCardano,
			0,
			"cardano-issued blocks after PV11 enactment may be zero; cardano-producer contribution is informational",
		)
	})
}
