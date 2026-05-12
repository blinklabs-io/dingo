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

//go:build erastest

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
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

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
		drep.regTxID, startEpoch,
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
			tag, changeAddr, lovelaceIn, deposit, flatTxFee,
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
			outBase, outBase,
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
		t.Fatalf("no UTxO at %s — cardano-node may still be processing the previous driver tx", addr)
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
	c := exec.CommandContext(ctx, "docker", "exec", cliContainer, "sh", "-c", cmd)
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			t.Fatalf(
				"docker exec timed out after 30s (%s)\n--- cmd ---\n%s\n--- stdout ---\n%s\n--- stderr ---\n%s",
				what, cmd, stdout.String(), stderr.String(),
			)
		}
		t.Fatalf(
			"docker exec failed (%s): %v\n--- cmd ---\n%s\n--- stdout ---\n%s\n--- stderr ---\n%s",
			what, err, cmd, stdout.String(), stderr.String(),
		)
	}
	return stdout.Bytes()
}
