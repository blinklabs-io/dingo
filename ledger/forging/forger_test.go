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

package forging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/bursa"
	dingotestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// fakeRemoteKESSigner is a minimal RemoteKESSigner for exercising the
// agent-backed kesSign/updateKESPeriod paths without a real kesagent.Client.
type fakeRemoteKESSigner struct {
	signFunc      func(period uint64, message []byte) ([]byte, error)
	calls         []uint64
	readinessErr  error
	readinessFunc func() error
}

func (f *fakeRemoteKESSigner) Sign(
	period uint64,
	message []byte,
) ([]byte, error) {
	f.calls = append(f.calls, period)
	if f.signFunc != nil {
		return f.signFunc(period, message)
	}
	return append([]byte(nil), message...), nil
}

func (f *fakeRemoteKESSigner) CheckReady() error {
	if f.readinessFunc != nil {
		return f.readinessFunc()
	}
	return f.readinessErr
}

func TestRemoteCredentialsReadinessDetectsAgentLoss(t *testing.T) {
	t.Parallel()
	vrfPath, _, opCertPath := createTestKeys(t)
	pc := NewPoolCredentials()
	t.Cleanup(pc.Close)
	signer := &fakeRemoteKESSigner{}
	require.NoError(t, pc.LoadFromAgentSign(vrfPath, opCertPath, signer))
	require.NoError(t, pc.ValidateOpCert())
	require.NoError(
		t,
		pc.ValidateKESPeriod(
			synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
			0,
		),
	)
	require.NoError(t, pc.usableAtKESPeriod(0))
	signer.readinessErr = errors.New("agent is unavailable")
	require.ErrorIs(t, pc.usableAtKESPeriod(0), signer.readinessErr)
	require.Empty(t, signer.calls, "readiness must not sign")
}

func TestRemoteReadinessDoesNotBlockKESEvolution(t *testing.T) {
	t.Parallel()
	vrfPath, _, opCertPath := createTestKeys(t)
	pc := NewPoolCredentials()
	t.Cleanup(pc.Close)
	entered := make(chan struct{})
	release := make(chan struct{})
	signer := &fakeRemoteKESSigner{readinessFunc: func() error {
		close(entered)
		<-release
		return nil
	}}
	require.NoError(t, pc.LoadFromAgentSign(vrfPath, opCertPath, signer))
	require.NoError(t, pc.ValidateOpCert())
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)), 0,
	))
	snapshot := pc.acquireCredentialGeneration()
	defer snapshot.release()
	ready := make(chan error, 1)
	go func() { ready <- pc.usableAtKESPeriod(0) }()
	defer func() {
		close(release)
		require.NoError(t, dingotestutil.RequireReceive(t, ready, 5*time.Second, "readiness completes after release"))
	}()
	dingotestutil.RequireReceive(t, entered, 5*time.Second, "readiness handshake entered")
	evolved := make(chan error, 1)
	go func() {
		evolved <- pc.updateKESPeriodForGeneration(
			snapshot.id, snapshot.materialRevision, 1,
		)
	}()
	require.NoError(t, dingotestutil.RequireReceive(t, evolved, time.Second, "KES evolution must not wait for readiness I/O"))
}

// TestCredentialGenerationKesSignRejectsExpiredPeriod proves the
// opcert-lifetime gate applies inside kesSign itself, not only at its callers
// (BlockForger.SignBlockHeader, DefaultBlockBuilder.buildBlock).
// KES agent client bypassed exactly this: it signed through a direct call to
// the agent instead of through this method, so the opcert-lifetime check both
// of those callers otherwise rely on never ran for the agent path.
//
// Evolving to period 5 (via updateKESPeriod, which does not itself check
// expiry -- only kesSign does) keeps the KES key within its own 2^6
// cryptographic capacity, so kes.Sign's unrelated "key is at this period"
// check cannot be what rejects the sign below: only the opcert-lifetime
// policy gate can be.
func TestCredentialGenerationKesSignRejectsExpiredPeriod(t *testing.T) {
	t.Parallel()

	vrfPath, kesPath, opCertPath := createTestKeys(t)
	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromFiles(vrfPath, kesPath, opCertPath))
	// maxKESEvolutions=3 -> validated lifetime is periods [0, 3).
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()
	require.Equal(t, uint64(0), generation.opCertStartKES)
	require.Equal(t, uint64(3), generation.opCertExpiryKES)

	const expiredPeriod = 5
	require.NoError(t, generation.updateKESPeriod(expiredPeriod))

	_, err := generation.kesSign(expiredPeriod, []byte("header"))
	require.ErrorIs(t, err, errOpCertExpired)
}

// TestCredentialGenerationKesSignAgentPathRejectsExpiredPeriod is the same
// proof for the agent-backed ("sign" mode) signing path: the gate must apply
// identically whether or not a remote signer is installed, and the agent must
// never even be asked to sign a period the opcert has not authorized.
func TestCredentialGenerationKesSignAgentPathRejectsExpiredPeriod(
	t *testing.T,
) {
	t.Parallel()

	vrfPath, _, opCertPath := createTestKeys(t)
	signer := &fakeRemoteKESSigner{}
	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromAgentSign(vrfPath, opCertPath, signer))
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()

	const expiredPeriod = 5
	require.NoError(t, generation.updateKESPeriod(expiredPeriod))

	_, err := generation.kesSign(expiredPeriod, []byte("header"))
	require.ErrorIs(t, err, errOpCertExpired)
	require.Empty(
		t,
		signer.calls,
		"agent must not be asked to sign a period outside the validated opcert lifetime",
	)
}

// TestCredentialGenerationKesSignAgentPathDelegatesWithinLifetime proves the
// positive case alongside the negative one above: a period the opcert does
// authorize reaches the remote signer, carrying the caller's ABSOLUTE period
// unchanged (matching the bursa KES agent sign-mode wire protocol, which also
// takes an absolute period and translates internally).
func TestCredentialGenerationKesSignAgentPathDelegatesWithinLifetime(
	t *testing.T,
) {
	t.Parallel()

	vrfPath, _, opCertPath := createTestKeys(t)
	wantSig := []byte("agent-signature")
	signer := &fakeRemoteKESSigner{
		signFunc: func(uint64, []byte) ([]byte, error) {
			return wantSig, nil
		},
	}
	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromAgentSign(vrfPath, opCertPath, signer))
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()

	require.NoError(t, generation.updateKESPeriod(1))
	sig, err := generation.kesSign(1, []byte("header"))
	require.NoError(t, err)
	require.Equal(t, wantSig, sig)
	require.Equal(t, []uint64{1}, signer.calls)
}

// TestCredentialGenerationUpdateKESPeriodAgentPathRejectsBackward proves the
// agent-backed path enforces the same never-evolve-backward invariant the
// local-key path enforces in updateKESPeriodUnsafe/credentialGeneration.
func TestCredentialGenerationUpdateKESPeriodAgentPathRejectsBackward(
	t *testing.T,
) {
	t.Parallel()

	vrfPath, _, opCertPath := createTestKeys(t)
	pc := NewPoolCredentials()
	require.NoError(
		t,
		pc.LoadFromAgentSign(vrfPath, opCertPath, &fakeRemoteKESSigner{}),
	)
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()

	require.NoError(t, generation.updateKESPeriod(2))
	require.ErrorContains(
		t,
		generation.updateKESPeriod(1),
		"cannot evolve KES period backward",
	)
}

// TestPoolCredentialsLoadFromAgentServeKeyMatchesLocalPath proves serve-key
// material installs through the same identity/generation path LoadFromFiles
// uses, so it is signable, opcert-validatable, and produces a signature that
// verifies against the pushed KES verification key -- indistinguishable from
// a local key file once installed.
func TestPoolCredentialsLoadFromAgentServeKeyMatchesLocalPath(t *testing.T) {
	t.Parallel()

	vrfPath, kesPath, opCertPath := createTestKeys(t)
	kesKey, err := loadSecretKeyFromFile(kesPath)
	require.NoError(t, err)
	opCertKey, err := bursa.LoadKeyFromFile(opCertPath)
	require.NoError(t, err)

	material := AgentKESMaterial{
		AbsolutePeriod: opCertKey.OpCertKesPeriod,
		KESSKeyData:    kesKey.SKey,
		KESVKey:        opCertKey.VKey,
		OpCert: OpCert{
			KESVKey:     opCertKey.VKey,
			IssueNumber: opCertKey.OpCertIssueNumber,
			KESPeriod:   opCertKey.OpCertKesPeriod,
			Signature:   opCertKey.OpCertSignature,
			ColdVKey:    opCertKey.OpCertColdVKey,
		},
	}

	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromAgentServeKey(vrfPath, material))
	require.True(t, pc.IsLoaded())
	require.NoError(t, pc.ValidateOpCert())
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()
	require.NoError(t, generation.updateKESPeriod(0))
	sig, err := generation.kesSign(0, []byte("header"))
	require.NoError(t, err)
	require.True(
		t,
		kes.VerifySignedKES(opCertKey.VKey, 0, []byte("header"), sig),
	)
}

// TestPoolCredentialsLoadFromAgentServeKeyRejectsVKeyMismatch proves a pushed
// key whose verification key does not match its own operational certificate
// is refused rather than installed (P1: "pushed key material accepted
// without validation").
func TestPoolCredentialsLoadFromAgentServeKeyRejectsVKeyMismatch(t *testing.T) {
	t.Parallel()

	vrfPath, kesPath, opCertPath := createTestKeys(t)
	kesKey, err := loadSecretKeyFromFile(kesPath)
	require.NoError(t, err)
	opCertKey, err := bursa.LoadKeyFromFile(opCertPath)
	require.NoError(t, err)

	wrongVKey := append([]byte(nil), opCertKey.VKey...)
	wrongVKey[0] ^= 0xFF

	material := AgentKESMaterial{
		AbsolutePeriod: opCertKey.OpCertKesPeriod,
		KESSKeyData:    kesKey.SKey,
		KESVKey:        wrongVKey,
		OpCert: OpCert{
			KESVKey:     opCertKey.VKey,
			IssueNumber: opCertKey.OpCertIssueNumber,
			KESPeriod:   opCertKey.OpCertKesPeriod,
			Signature:   opCertKey.OpCertSignature,
			ColdVKey:    opCertKey.OpCertColdVKey,
		},
	}

	pc := NewPoolCredentials()
	err = pc.LoadFromAgentServeKey(vrfPath, material)
	require.ErrorContains(t, err, "does not match OpCert KES vkey")
	require.False(t, pc.IsLoaded())
}

// TestPoolCredentialsLoadFromAgentServeKeyRejectsWrongKeySize proves an
// undersized/oversized pushed secret key is refused before installation.
func TestPoolCredentialsLoadFromAgentServeKeyRejectsWrongKeySize(t *testing.T) {
	t.Parallel()

	vrfPath, kesPath, opCertPath := createTestKeys(t)
	kesKey, err := loadSecretKeyFromFile(kesPath)
	require.NoError(t, err)
	opCertKey, err := bursa.LoadKeyFromFile(opCertPath)
	require.NoError(t, err)

	material := AgentKESMaterial{
		AbsolutePeriod: opCertKey.OpCertKesPeriod,
		KESSKeyData:    kesKey.SKey[:len(kesKey.SKey)-1],
		KESVKey:        opCertKey.VKey,
		OpCert: OpCert{
			KESVKey:     opCertKey.VKey,
			IssueNumber: opCertKey.OpCertIssueNumber,
			KESPeriod:   opCertKey.OpCertKesPeriod,
			Signature:   opCertKey.OpCertSignature,
			ColdVKey:    opCertKey.OpCertColdVKey,
		},
	}

	pc := NewPoolCredentials()
	err = pc.LoadFromAgentServeKey(vrfPath, material)
	require.ErrorContains(t, err, "invalid agent KES key size")
	require.False(t, pc.IsLoaded())
}

// TestPoolCredentialsLoadFromAgentSignRequiresSigner proves a nil signer is
// refused rather than silently leaving PoolCredentials in an unsignable state.
func TestPoolCredentialsLoadFromAgentSignRequiresSigner(t *testing.T) {
	t.Parallel()

	vrfPath, _, opCertPath := createTestKeys(t)
	pc := NewPoolCredentials()
	err := pc.LoadFromAgentSign(vrfPath, opCertPath, nil)
	require.ErrorContains(t, err, "requires a non-nil signer")
}

// TestLoadFromAgentServeKeyValidatedNeverPublishesAnUnvalidatedCredential is
// the defect that installing and re-validating as two locked calls produced.
//
// The install clears the operational certificate's validated lifetime, and
// the validation restores it. Between two calls the credentials are published
// with that lifetime cleared, so a forge attempt landing there acquires a
// generation whose opCertValidated is false and is refused with "operational
// certificate is not validated" -- a lost block, on every KES evolution and
// opcert rotation, on a node whose only job is to forge.
//
// The reader here is the forge attempt: it acquires a generation and asks for
// the lifetime exactly as credentialGeneration.kesSign does, throughout a run
// of installs. A generation whose credentials are loaded must carry a
// validated lifetime; the only state it may otherwise be in is not-loaded,
// which is the fail-closed state an install error leaves and which this run
// never produces.
func TestLoadFromAgentServeKeyValidatedNeverPublishesAnUnvalidatedCredential(
	t *testing.T,
) {
	t.Parallel()

	vrfPath, kesPath, opCertPath := createTestKeys(t)
	kesKey, err := loadSecretKeyFromFile(kesPath)
	require.NoError(t, err)
	opCertKey, err := bursa.LoadKeyFromFile(opCertPath)
	require.NoError(t, err)

	material := AgentKESMaterial{
		AbsolutePeriod: opCertKey.OpCertKesPeriod,
		KESSKeyData:    kesKey.SKey,
		KESVKey:        opCertKey.VKey,
		OpCert: OpCert{
			KESVKey:     opCertKey.VKey,
			IssueNumber: opCertKey.OpCertIssueNumber,
			KESPeriod:   opCertKey.OpCertKesPeriod,
			Signature:   opCertKey.OpCertSignature,
			ColdVKey:    opCertKey.OpCertColdVKey,
		},
	}
	genesis := synthGenesis(1, 3, time.Second, time.Unix(0, 0))

	pc := NewPoolCredentials()
	require.NoError(
		t,
		pc.LoadFromAgentServeKeyValidated(vrfPath, material, genesis, 0),
	)

	done := make(chan struct{})
	observed := make(chan error, 1)
	go func() {
		defer close(observed)
		for {
			select {
			case <-done:
				return
			default:
			}
			generation := pc.acquireCredentialGeneration()
			loaded := generation.loaded
			_, _, _, err := generation.validatedKESProtocolLifetime()
			generation.release()
			if loaded && err != nil {
				select {
				case observed <- err:
				default:
				}
				return
			}
		}
	}()

	// Every iteration is a rotation: the same material re-pushed, which is
	// what a reconnect produces and what a KES evolution looks like to this
	// code path.
	for range 200 {
		require.NoError(
			t,
			pc.LoadFromAgentServeKeyValidated(vrfPath, material, genesis, 0),
		)
	}
	close(done)

	if err, ok := <-observed; ok {
		t.Fatalf(
			"a forge attempt observed loaded credentials with no validated "+
				"lifetime during an agent key install: %v",
			err,
		)
	}
}

// newStaleTipTestForger builds a production forger whose leader check always
// says "leader", so the only thing that can stop it forging is a gate.
// primaryTipSlot is this node's own primary chain tip; chainTipSlot is the
// ledger-applied tip a forged block would be built on.
func newStaleTipTestForger(
	t *testing.T,
	currentSlot, chainTipSlot, primaryTipSlot uint64,
	logs *bytes.Buffer,
) (*BlockForger, *forgerTestBuilder, *forgerTestBroadcaster) {
	t.Helper()
	block := newForgerTestBlock(currentSlot, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:        currentSlot,
			chainTipSlot:       chainTipSlot,
			primaryTipExplicit: true,
			primaryTipSlot:     primaryTipSlot,
			slotsPerKESPeriod:  100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder, broadcaster
}

// TestForgeSkipsWhenLedgerTipTrailsPrimaryChainTip is the stale-tip-forge
// regression. The forge loop takes its parent from the LEDGER-APPLIED tip.
// When this node's own primary chain tip is further ahead, that parent is a
// block the node has already superseded, so the forged block enters a fork
// race it has already lost and is orphaned. The upstream sync guard does not
// catch it: it compares the applied tip against the network with a tolerance
// sized for catch-up, and here there is no upstream lag at all -- the node's
// own ledger pipeline is the thing behind.
//
// Before the fix the forger built and broadcast the block regardless.
func TestForgeSkipsWhenLedgerTipTrailsPrimaryChainTip(t *testing.T) {
	var logs bytes.Buffer
	// Applied tip 83 slots behind the primary chain tip: the field case.
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		100, // ledger-applied tip
		183, // primary chain tip
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls, "must not build on a superseded parent")
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
	)
	// Never silently: the skip is a WARN an operator can alert on.
	require.Contains(
		t,
		logs.String(),
		"forge skip: too many primary-chain blocks are unapplied",
	)
	require.Contains(t, logs.String(), `"level":"WARN"`)
}

// TestStaleTipSkipCountsCouldNotForge pins the cardano-node parity counters
// across this refusal. The gate runs after checkLeaderSafe and before
// forgeNodeIsLeader.Inc(), so a lost leader slot moves about_to_lead at the
// top of the check and then nothing: node_is_leader never increments,
// not_leader counts only !isLeader, and without this the Dingo-specific
// dingo_forge_stale_tip_skip_total would be the sole record of the loss. An
// operator alerting on could_not_forge -- registered as "slots where forging
// failed (syncing, build error, etc)" -- would see a flat line while the
// producer stopped forging.
func TestStaleTipSkipCountsCouldNotForge(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForger(t, 200, 100, 183, &logs)
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Zero(t, builder.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"lost leader slot must move cardano_node_metrics_Forge_could_not_forge_int",
	)
}

// TestForgeProceedsWithinBlockAndSlotBounds pins the allowed local lag: two
// unapplied blocks inside the slot prefilter do not suppress forging.
func TestForgeProceedsWithinBlockAndSlotBounds(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200,
		100,
		105,
		&logs,
	)
	forger.slotClock = forgerTestSlotClock{
		currentSlot:           200,
		chainTipSlot:          100,
		chainTipBlockNumber:   1000,
		primaryTipExplicit:    true,
		primaryTipSlot:        105,
		primaryTipBlockNumber: 1002,
		primaryTipRelationSet: true,
		primaryTipAncestor:    true,
		primaryTipDepth:       2,
		slotsPerKESPeriod:     100,
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipHashDiverged),
	)
	require.NotContains(
		t,
		logs.String(),
		"forge skip: too many primary-chain blocks are unapplied",
	)
	// The post-mortem line survives the forge path. It is emitted below the
	// credential recheck, i.e. after the last gate that can still refuse the
	// slot, so a "forge context" line is never followed by a skip for it.
	require.Contains(t, logs.String(), "forge context")
}

// TestForgeRejectsLocalBlockGapAboveSmallBound ensures the era security
// parameter cannot widen the local transaction-state safety limit.
func TestForgeRejectsLocalBlockGapAboveSmallBound(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200,
		100,
		183,
		&logs,
	)
	forger.slotClock = forgerTestSlotClock{
		currentSlot:           200,
		chainTipSlot:          100,
		chainTipBlockNumber:   1000,
		primaryTipExplicit:    true,
		primaryTipSlot:        183,
		primaryTipBlockNumber: 1004,
		primaryTipRelationSet: true,
		primaryTipAncestor:    true,
		primaryTipDepth:       4,
		securityParam:         432,
		slotsPerKESPeriod:     100,
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
	)
	require.Contains(t, logs.String(), `"max_unapplied_blocks":2`)
	require.Contains(t, logs.String(), `"security_param_k":432`)
}

func TestForgeRejectsLocalSlotGapBeyondPrefilter(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200,
		49,
		150,
		&logs,
	)
	forger.slotClock = forgerTestSlotClock{
		currentSlot:           200,
		chainTipSlot:          49,
		chainTipBlockNumber:   1000,
		primaryTipExplicit:    true,
		primaryTipSlot:        150,
		primaryTipBlockNumber: 1001,
		primaryTipRelationSet: true,
		primaryTipAncestor:    true,
		primaryTipDepth:       1,
		slotsPerKESPeriod:     100,
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
	)
}

func TestForgeSkipsWhenCorroboratedPeerIsManyBlocksAhead(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		6_099,
		6_000,
		6_000,
		&logs,
	)
	forger.slotClock = forgerTestSlotClock{
		currentSlot:            6_099,
		chainTipSlot:           6_000,
		chainTipBlockNumber:    4_645_026,
		primaryTipExplicit:     true,
		primaryTipSlot:         6_000,
		primaryTipBlockNumber:  4_645_026,
		upstreamTipSlot:        6_099,
		upstreamTipBlockNumber: 4_646_750,
		upstreamActive:         true,
		slotsPerKESPeriod:      100,
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipPeerHeightGap),
		logs.String(),
	)
	require.Contains(t, logs.String(), `"reason":"peer_height_gap"`)
}

func TestForgeSkipsWhenAppliedTipIsNotPrimaryAncestor(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForger(t, 210, 198, 200, &logs)
	primaryHash := bytes.Repeat([]byte{0xBB}, 32)
	clock := forgerTestSlotClock{
		currentSlot:           210,
		chainTipSlot:          198,
		primaryTipExplicit:    true,
		primaryTipSlot:        200,
		primaryTipHash:        primaryHash,
		primaryTipRelationSet: true,
		primaryTipAncestor:    false,
		primaryTipDepth:       2,
		slotsPerKESPeriod:     100,
	}
	forger.slotClock = clock

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Zero(t, builder.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipPrimaryNotAncestor),
	)
	require.Contains(
		t,
		logs.String(),
		`"reason":"applied_tip_not_primary_ancestor"`,
	)
}

// TestTipGapGaugeReportsApplyBacklogOnEveryLeaderCheck is the observability
// half of the regression. dingo_forge_tip_gap_slots was reset to 0 at the top
// of every leader check and only set non-zero on the skip paths, so a producer
// forging tens of slots behind its own primary tip reported a gap of exactly
// 0 -- the one case where the gauge mattered was the one case it could not
// show.
func TestTipGapGaugeReportsApplyBacklogOnEveryLeaderCheck(t *testing.T) {
	var logs bytes.Buffer

	// Within tolerance: the forge proceeds, and the gauge still reports the
	// real backlog rather than 0.
	forger, builder, _ := newStaleTipTestForger(t, 200, 100, 103, &logs)
	forger.slotClock = forgerTestSlotClock{
		currentSlot:           200,
		chainTipSlot:          100,
		chainTipBlockNumber:   1000,
		primaryTipExplicit:    true,
		primaryTipSlot:        103,
		primaryTipBlockNumber: 1002,
		primaryTipRelationSet: true,
		primaryTipAncestor:    true,
		primaryTipDepth:       2,
		slotsPerKESPeriod:     100,
	}
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, builder.calls, "expected this check to forge")
	require.Equal(
		t,
		float64(3),
		testutil.ToFloat64(forger.metrics.tipGapSlots),
	)

	// Beyond tolerance: the gauge reports the backlog that caused the skip.
	skipping, _, _ := newStaleTipTestForger(t, 200, 100, 183, &logs)
	require.NoError(t, skipping.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		float64(83),
		testutil.ToFloat64(skipping.metrics.tipGapSlots),
	)

	// No backlog: zero, not a stale reading.
	caughtUp, _, _ := newStaleTipTestForger(t, 200, 199, 199, &logs)
	require.NoError(t, caughtUp.checkAndForgeProduction(context.Background()))
	require.Zero(t, testutil.ToFloat64(caughtUp.metrics.tipGapSlots))
}

// newEqualSlotForkTestForger builds a production forger whose applied tip and
// primary chain tip sit at the SAME slot but carry the given hashes.
func newEqualSlotForkTestForger(
	t *testing.T,
	appliedHash, primaryTipHash []byte,
	logs *bytes.Buffer,
) (*BlockForger, *forgerTestBuilder, *forgerTestBroadcaster) {
	t.Helper()
	block := newForgerTestBlock(200, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:        200,
			chainTipSlot:       100,
			chainTipHash:       appliedHash,
			primaryTipExplicit: true,
			primaryTipSlot:     100,
			primaryTipHash:     primaryTipHash,
			slotsPerKESPeriod:  100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder, broadcaster
}

// TestForgeSkipsOnEqualSlotPrimaryTipDivergence is the equal-slot fork the
// slot gap cannot see. Chain selection replaced the block at the applied tip's
// slot with a competing one at the SAME slot that the ledger has not applied,
// so the gap is 0 while the ledger state still describes the block that was
// replaced -- the builder would parent the block on one chain position while
// its transactions, protocol parameters and leader eligibility came from
// another.
func TestForgeSkipsOnEqualSlotPrimaryTipDivergence(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newEqualSlotForkTestForger(
		t,
		bytes.Repeat([]byte{0xAA}, 32),
		bytes.Repeat([]byte{0xBB}, 32),
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls, "must not forge across an equal-slot fork")
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipHashDiverged),
	)
	// The slot-gap reason must not be charged for a divergence.
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
	)
	// Reason-specific message: an equal-slot divergence is not a stale
	// ledger tip, and the shared message used to say it was.
	require.Contains(
		t,
		logs.String(),
		"forge skip: primary chain tip diverged from the applied tip at the same slot",
	)
	require.NotContains(
		t,
		logs.String(),
		"forge skip: too many primary-chain blocks are unapplied",
	)
	require.Contains(t, logs.String(), `"reason":"primary_tip_hash_diverged"`)
	require.Contains(t, logs.String(), `"level":"WARN"`)
	// The gauge is a slot gap and there is none; the divergence shows on the
	// counter, not here.
	require.Zero(t, testutil.ToFloat64(forger.metrics.tipGapSlots))
}

// TestForgeProceedsWhenPrimaryTipMatchesAppliedTip pins the other side: the
// same slot with the same hash is the normal caught-up state and must forge.
func TestForgeProceedsWhenPrimaryTipMatchesAppliedTip(t *testing.T) {
	var logs bytes.Buffer
	hash := bytes.Repeat([]byte{0xAA}, 32)
	forger, builder, broadcaster := newEqualSlotForkTestForger(
		t,
		hash,
		bytes.Clone(hash),
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipHashDiverged),
	)
	require.NotContains(
		t,
		logs.String(),
		"forge skip: too many primary-chain blocks are unapplied",
	)
}

// TestForgeProceedsWhenEitherTipHashIsEmpty pins that a genesis or
// uninitialised primary chain -- where there is no hash to compare -- does not
// wedge a fresh node into never forging.
func TestForgeProceedsWhenEitherTipHashIsEmpty(t *testing.T) {
	for name, tc := range map[string]struct {
		applied, primaryTip []byte
	}{
		"primary chain tip hash unknown": {
			applied:    bytes.Repeat([]byte{0xAA}, 32),
			primaryTip: []byte{},
		},
		"applied hash unknown": {
			applied:    []byte{},
			primaryTip: bytes.Repeat([]byte{0xBB}, 32),
		},
		"both at genesis": {applied: []byte{}, primaryTip: []byte{}},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			forger, builder, _ := newEqualSlotForkTestForger(
				t,
				tc.applied,
				tc.primaryTip,
				&logs,
			)
			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)
			require.Equal(t, 1, builder.calls)
			require.Zero(
				t,
				testutil.ToFloat64(
					forger.metrics.forgeStaleTipSkipHashDiverged,
				),
			)
		})
	}
}

// TestForgeStaleTipSkipReasonsArePreMaterialized pins that every reason series
// exists before the first skip, so a dashboard is not looking at an absent
// series.
//
// Eight series cover primary-chain ancestry, local and peer block-height gaps,
// existing tip disagreements, the pre-leader-check rival, and staleness.
func TestForgeStaleTipSkipReasonsArePreMaterialized(t *testing.T) {
	var logs bytes.Buffer
	hash := bytes.Repeat([]byte{0xAA}, 32)
	forger, _, _ := newEqualSlotForkTestForger(
		t,
		hash,
		bytes.Clone(hash),
		&logs,
	)
	require.Equal(
		t,
		8,
		testutil.CollectAndCount(forger.metrics.forgeStaleTipSkip),
	)
}

// forgeStaleTipTestNonLeader never elects this node, so a test can separate
// "the stale-tip condition holds" from "a block was actually lost to it".
type forgeStaleTipTestNonLeader struct{}

func (forgeStaleTipTestNonLeader) ShouldProduceBlock(uint64) bool {
	return false
}

func (forgeStaleTipTestNonLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, false
}

// newStaleTipTestForgerWithLeader is newStaleTipTestForger with the leader
// checker and the two tip hashes made explicit.
func newStaleTipTestForgerWithLeader(
	t *testing.T,
	leader LeaderChecker,
	currentSlot, chainTipSlot, primaryTipSlot uint64,
	appliedHash, primaryTipHash []byte,
	logs *bytes.Buffer,
) (*BlockForger, *forgerTestBuilder, *forgerTestBroadcaster) {
	t.Helper()
	block := newForgerTestBlock(currentSlot, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:        currentSlot,
			chainTipSlot:       chainTipSlot,
			chainTipHash:       appliedHash,
			primaryTipExplicit: true,
			primaryTipSlot:     primaryTipSlot,
			primaryTipHash:     primaryTipHash,
			slotsPerKESPeriod:  100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder, broadcaster
}

// TestForgeSkipsWhenPrimaryTipAlreadyHasTheCurrentSlot covers the guard that
// asks "does a block already exist at this slot". It compared the current slot
// against the LEDGER-APPLIED tip, but the parent comes from the primary tip,
// so inside the primary tip tolerance a peer's block at the current slot could
// already be on the primary tip while still unapplied. Forging then parents a
// block for slot S on a tip already at slot S -- a non-increasing slot,
// admitted locally and broadcast.
//
// The gap here is 2 slots, well inside the tolerance, so the stale-tip gate
// does not fire and this guard is the only thing that can catch it.
func TestForgeSkipsWhenPrimaryTipAlreadyHasTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		198, // ledger-applied tip, still behind
		200, // primary chain tip already carries a block at the current slot
		&logs,
	)
	securityParam := forger.slotClock.(forgerTestSlotClock).SecurityParam()
	require.LessOrEqual(
		t,
		uint64(2),
		uint64(securityParam),
		"this test needs the two-block gap to be inside K",
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(
		t,
		builder.calls,
		"must not forge a non-increasing slot on top of the primary chain tip",
	)
	require.Zero(t, broadcaster.calls)
	require.Contains(
		t,
		logs.String(),
		"forge skip: primary chain tip already has a block at this slot",
	)
	// Warned, not Debug: this gate runs before leader selection, so a slot
	// this node was scheduled to lead would otherwise vanish silently.
	require.Contains(t, logs.String(), `"level":"WARN"`)
}

// TestForgeSkipsWhenPrimaryTipIsAheadOfTheCurrentSlot covers the case that
// falls through every other gate: the applied tip is behind the current slot,
// so the applied-tip comparison passes, but the PRIMARY CHAIN TIP is ahead of
// it. The builder parents on the primary tip, so forging would produce a block
// for slot 200 whose parent already sits at slot 201 -- a block earlier than
// its own parent. Comparing the current slot against the applied tip alone
// cannot see this; comparing against max(applied, primaryTip) can.
func TestForgeSkipsWhenPrimaryTipIsAheadOfTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		199, // applied tip, behind the current slot
		201, // primary chain tip AHEAD of the current slot
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(
		t,
		builder.calls,
		"must not forge a block whose parent is at a later slot than itself",
	)
	require.Zero(t, broadcaster.calls)
	require.Contains(
		t,
		logs.String(),
		"forge skip: chain tip is ahead of the current slot",
	)
}

// TestForgeSkipsWhenAppliedTipAlreadyHasTheCurrentSlot pins that narrowing the
// past-slot comparison to a strict inequality did not re-open the plain
// equal-applied-tip case. Equal slots now fall through to the contested-slot
// handling, which is exactly why the comparison had to stop consuming them,
// and that handling still refuses the slot.
func TestForgeSkipsWhenAppliedTipAlreadyHasTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		200, // applied tip already at this slot
		200, // primary chain tip agrees
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
	require.Contains(
		t,
		logs.String(),
		"forge skip: leader slot already holds another block",
	)
}

// TestForgeStaleTipSkipCountsLostBlocksNotLeaderChecks pins that the stale-tip
// gate sits after leader selection. The condition holds for as long as the
// pipeline is behind, so gating before the leader check made the WARN and the
// counter fire once per slot -- once a second on a 1s-slot chain -- and made
// the counter measure leader checks rather than lost blocks.
func TestForgeStaleTipSkipCountsLostBlocksNotLeaderChecks(t *testing.T) {
	t.Run("not leader: no warning, no counter", func(t *testing.T) {
		var logs bytes.Buffer
		forger, builder, _ := newStaleTipTestForgerWithLeader(
			t,
			forgeStaleTipTestNonLeader{},
			200, 100, 183,
			nil, nil,
			&logs,
		)

		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)

		require.Zero(t, builder.calls)
		require.Zero(
			t,
			testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
			"a slot this node was never going to forge is not a lost block",
		)
		require.NotContains(
			t,
			logs.String(),
			"forge skip: too many primary-chain blocks are unapplied",
		)
		// The backlog is still reported on every leader check.
		require.Equal(
			t,
			float64(83),
			testutil.ToFloat64(forger.metrics.tipGapSlots),
		)
	})

	t.Run("leader: warning and counter", func(t *testing.T) {
		var logs bytes.Buffer
		forger, builder, _ := newStaleTipTestForgerWithLeader(
			t,
			forgerTestLeader{},
			200, 100, 183,
			nil, nil,
			&logs,
		)

		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)

		require.Zero(t, builder.calls)
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
		)
		require.Contains(
			t,
			logs.String(),
			"forge skip: too many primary-chain blocks are unapplied",
		)
	})
}

// TestForgeSkipsWhenPrimaryTipIsBehindTheAppliedTip covers the third
// disagreement shape. applyGap is 0 (the primary tip is not ahead) and the
// equal-slot hash check does not apply (the slots differ), so neither existing
// case sees it, yet the ledger describes a chain position ahead of the parent
// the builder would use. The ledger itself recognises this state and
// reconciles it at startup by rolling its tip back to the chain tip.
func TestForgeSkipsWhenPrimaryTipIsBehindTheAppliedTip(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForgerWithLeader(
		t,
		forgerTestLeader{},
		300, // current slot
		200, // ledger-applied tip
		190, // primary chain tip BEHIND the applied tip
		bytes.Repeat([]byte{0xAA}, 32),
		bytes.Repeat([]byte{0xBB}, 32),
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipPrimaryTipBehind),
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
	)
	require.Contains(t, logs.String(), `"reason":"primary_tip_behind_applied"`)
	require.Contains(t, logs.String(), `"level":"WARN"`)
}

// TestForgeProceedsWhenPrimaryTipIsUninitialised pins that a node whose
// primary chain has no tip yet -- zero slot, empty hash -- is not caught by
// the primary-tip-behind case and can still forge.
func TestForgeProceedsWhenPrimaryTipIsUninitialised(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForgerWithLeader(
		t,
		forgerTestLeader{},
		300, 200, 0,
		bytes.Repeat([]byte{0xAA}, 32),
		nil, // no primary chain tip hash: chain not initialised
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipPrimaryTipBehind),
	)
}

// TestForgeCountsLeaderSlotLostToUnappliedRival pins the counter on the one
// refusal this gate adds that runs BEFORE leader selection: the primary chain
// tip already holds a block at the current slot while the ledger has not
// applied it. That path returns before checkLeaderSafe, so a slot this node
// was scheduled to lead moves about_to_lead and nothing else -- no
// node_is_leader, no not_leader, no could_not_forge.
//
// It matters because one real-world event splits across two paths purely on
// pipeline timing. A rival block at our leader slot that the ledger HAS
// applied reaches the contested-slot branch above and moves both
// slotBattlesTotal and could_not_forge; the same rival still unapplied lands
// here. Without this counter a dashboard sees the lost block in the first case
// and not in the second.
//
// The count is taken from isScheduledLeaderSlot, which this path already
// consults to pick the log level, so the counter moves on exactly the slots
// the WARN marks and never on an ordinary slot.
func TestForgeCountsLeaderSlotLostToUnappliedRival(t *testing.T) {
	const leaderSlot = uint64(200)
	for _, tc := range []struct {
		name      string
		scheduled map[uint64]struct{}
		wantCount float64
		wantLevel string
	}{
		{
			// Not a slot this node was due to lead: nothing was lost,
			// so nothing is counted and the skip stays routine.
			name:      "ordinary slot counts nothing",
			scheduled: map[uint64]struct{}{},
			wantCount: 0,
			wantLevel: `"level":"DEBUG"`,
		},
		{
			// A scheduled leader slot: a block this node would have
			// forged, dropped by a gate that no parity counter covers.
			name: "scheduled leader slot counts one",
			scheduled: map[uint64]struct{}{
				leaderSlot: {},
			},
			wantCount: 1,
			wantLevel: `"level":"WARN"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			// The applied tip is 2 slots back -- inside the tolerance, so
			// the post-leader-check stale-tip gate does not fire -- while
			// the primary chain tip already carries a block at the current
			// slot.
			forger, builder, broadcaster := newStaleTipTestForgerWithLeader(
				t,
				&forgerScheduleAwareLeader{scheduled: tc.scheduled},
				leaderSlot,   // current slot
				leaderSlot-2, // ledger-applied tip, still behind
				leaderSlot,   // primary chain tip holds this slot already
				bytes.Repeat([]byte{0xAA}, 32),
				bytes.Repeat([]byte{0xBB}, 32),
				&logs,
			)
			securityParam := forger.slotClock.(forgerTestSlotClock).SecurityParam()
			require.LessOrEqual(
				t,
				uint64(2),
				uint64(securityParam),
				"this test needs the two-block gap to be inside K",
			)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			require.Zero(t, builder.calls)
			require.Zero(t, broadcaster.calls)
			require.Contains(
				t,
				logs.String(),
				"forge skip: primary chain tip already has a block at this slot",
			)
			require.Contains(t, logs.String(), tc.wantLevel)
			require.Equal(
				t,
				tc.wantCount,
				testutil.ToFloat64(
					forger.metrics.forgeStaleTipSkipUnappliedRival,
				),
			)
			// The three post-leader-check reasons describe a different
			// refusal and must not move on this path.
			require.Zero(
				t,
				testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
			)
			require.Zero(
				t,
				testutil.ToFloat64(
					forger.metrics.forgeStaleTipSkipHashDiverged,
				),
			)
			require.Zero(
				t,
				testutil.ToFloat64(
					forger.metrics.forgeStaleTipSkipPrimaryTipBehind,
				),
			)
		})
	}
}

// newStalenessTestForger builds a production forger for the staleness gates,
// with the upstream target and the corroborated endorser-block slot explicit.
//
// upstreamStalenessSlots and ebStalenessSlots are explicit and every caller
// that exercises those bounds must pass a non-zero value. The wall-clock bound
// applies only when a live upstream has published a positive target.
func newStalenessTestForger(
	t *testing.T,
	currentSlot, chainTipSlot, primaryTipSlot, upstreamSlot uint64,
	ebSlot uint64,
	appliedStalenessSlots uint64,
	upstreamStalenessSlots uint64,
	ebStalenessSlots uint64,
	logs *bytes.Buffer,
) (*BlockForger, *forgerTestBuilder) {
	t.Helper()
	block := newForgerTestBlock(currentSlot, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			currentSlot:        currentSlot,
			chainTipSlot:       chainTipSlot,
			primaryTipExplicit: true,
			primaryTipSlot:     primaryTipSlot,
			upstreamTipSlot:    upstreamSlot,
			slotsPerKESPeriod:  100,
		},
		LeiosVerifiedEbSlot:              func() uint64 { return ebSlot },
		ForgeAppliedTipStalenessSlots:    appliedStalenessSlots,
		ForgeUpstreamStalenessSlots:      upstreamStalenessSlots,
		ForgeEndorserBlockStalenessSlots: ebStalenessSlots,
		PromRegistry:                     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder
}

// TestForgeSkipsWhenNewestKnownBlockTrailsUpstream is the ghost the
// primary-chain-tip gate could not see. When header admission and ledger
// application stall together the primary chain tip equals the applied tip, so
// that gate's gap reads 0 and it passes -- while the node is many slots behind
// the network and forges on a parent the network has already built past.
//
// Measured against the corroborated upstream target rather than the wall clock,
// so it stays meaningful on a chain of any block rate.
//
// The bound is opt-in, so this test sets it explicitly. It cannot by itself
// distinguish "the network is 19 slots ahead" from "the block 19 slots after
// mine was just admitted and its body is still in flight" -- the upstream
// target is published at header admission while newestKnown counts blocks --
// which is precisely why the bound is not defaulted on. See
// TestForgeUpstreamStalenessIsOffByDefault.
func TestForgeSkipsWhenNewestKnownBlockTrailsUpstream(t *testing.T) {
	var logs bytes.Buffer
	// Primary chain tip == applied tip, so the gap is 0, but the network is
	// 19 slots ahead. Slot numbers are scaled down so the KES period stays
	// inside the test operational certificate.
	forger, builder := newStalenessTestForger(
		t, 300, 299, 299, 318, 0, 0, 5, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.tipGapSlots),
		"the primary-chain-tip gap really is 0 here; that is the point",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipAppliedStale),
	)
	require.Contains(t, logs.String(), `"reason":"applied_tip_stale"`)
	require.Contains(t, logs.String(), `"upstream_target_slot":318`)
	// applied_tip_stale is reached from two independent bounds. Both are
	// logged, and stale_source names the one that fired, so a post-mortem
	// does not have to guess which term refused the slot.
	require.Contains(t, logs.String(), `"stale_source":"upstream"`)
	require.Contains(t, logs.String(), `"upstream_staleness_slots":5`)
	require.Contains(t, logs.String(), `"applied_staleness_slots":0`)
	// Reason-specific message: both local tips agree here, so the shared
	// "ledger tip stale vs primary chain tip" named the wrong pair.
	require.Contains(
		t,
		logs.String(),
		"forge skip: newest known block is stale",
	)
	require.NotContains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs primary chain tip",
	)
}

// TestForgeProceedsOnAQuietChain pins that the staleness term does not punish a
// chain with a long block interval. The newest block is 500 slots old but the
// network agrees it is the newest, so nothing is wrong and the node must forge.
// A wall-clock bound would refuse here, which is why the default term is
// measured against upstream and the wall-clock one is off by default.
func TestForgeProceedsOnAQuietChain(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 600, 100, 100, 100, 0, 0, 5, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipAppliedStale),
	)
	// The forge-context line carries every input a post-mortem needs.
	require.Contains(t, logs.String(), `"msg":"forge context"`)
	require.Contains(t, logs.String(), `"newest_known_slot":100`)
}

// TestForgeAppliedTipStalenessKnobIsOptIn pins the additional wall-clock bound
// when a usable upstream reference exists: off by default, and refusing once
// an operator sets a bound.
func TestForgeAppliedTipStalenessKnobIsOptIn(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		var logs bytes.Buffer
		forger, builder := newStalenessTestForger(
			t, 600, 100, 100, 0, 0, 0, 0, 0, &logs,
		)
		forger.slotClock = forgerTestSlotClock{
			currentSlot: 600, chainTipSlot: 100,
			primaryTipExplicit: true, primaryTipSlot: 100,
			upstreamTipSlot: 100, slotsPerKESPeriod: 100,
		}
		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)
		require.Equal(t, 1, builder.calls)
	})

	t.Run("refuses once set", func(t *testing.T) {
		var logs bytes.Buffer
		forger, builder := newStalenessTestForger(
			t, 600, 100, 100, 100, 0, 100, 0, 0, &logs,
		)
		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)
		require.Zero(t, builder.calls)
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(
				forger.metrics.forgeStaleTipSkipAppliedStale,
			),
		)
		// The other half of the pair: the wall-clock term fired, and the
		// line says so rather than logging an upstream bound of 0 with no
		// way to tell the two apart.
		require.Contains(t, logs.String(), `"stale_source":"wall_clock"`)
		require.Contains(t, logs.String(), `"applied_staleness_slots":100`)
		require.Contains(t, logs.String(), `"upstream_staleness_slots":0`)
	})
}

func TestForgeDoesNotTreatUnknownUpstreamAsStalenessEvidence(t *testing.T) {
	for _, upstreamActive := range []bool{false, true} {
		for _, gap := range []uint64{100, 101} {
			for _, appliedStalenessSlots := range []uint64{0, 50} {
				t.Run(fmt.Sprintf(
					"active=%t/gap=%d/applied_bound=%d",
					upstreamActive,
					gap,
					appliedStalenessSlots,
				), func(t *testing.T) {
					var logs bytes.Buffer
					currentSlot := uint64(1_000)
					tipSlot := currentSlot - gap
					forger, builder := newStalenessTestForger(
						t, currentSlot, tipSlot, tipSlot, 0, 0,
						appliedStalenessSlots, 0, 0, &logs,
					)
					forger.slotClock = forgerTestSlotClock{
						currentSlot:        currentSlot,
						chainTipSlot:       tipSlot,
						primaryTipExplicit: true,
						primaryTipSlot:     tipSlot,
						upstreamActive:     upstreamActive,
						slotsPerKESPeriod:  100,
					}

					require.NoError(
						t,
						forger.checkAndForgeProduction(context.Background()),
					)
					require.Equal(t, 1, builder.calls)
					require.Equal(
						t,
						1,
						forger.blockBroadcaster.(*forgerTestBroadcaster).calls,
					)
					require.Zero(
						t,
						testutil.ToFloat64(
							forger.metrics.forgeStaleTipSkipAppliedStale,
						),
					)
					require.NotContains(
						t,
						logs.String(),
						`"stale_source":"wall_clock"`,
					)
				})
			}
		}
	}
}

// TestForgeSkipsWhenCorroboratedEndorserBlockIsAhead covers the Leios signal: a
// corroborated endorser block shares its announcing ranking block's slot, so it
// is proof a ranking block exists there even though no header has arrived. The
// headers alone look caught up -- the primary chain tip equals the applied
// tip -- so only this evidence can refuse the forge.
//
// The bound is opt-in, so this test sets ForgeEndorserBlockStalenessSlots
// explicitly. It used to borrow a slot-based primary-tip bound and passed
// with both staleness bounds at 0, which is exactly the always-on refusal
// TestForgeEndorserBlockStalenessIsOffByDefault now forbids.
func TestForgeSkipsWhenCorroboratedEndorserBlockIsAhead(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 320, 300, 300, 0, 313, 0, 0, 5, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.Contains(t, logs.String(), `"reason":"eb_manifest_ahead"`)
	require.Contains(t, logs.String(), `"eb_slot":313`)
	// The refusal names its own bound and its own gap, not the local
	// block-against-block tolerance it used to borrow.
	require.Contains(t, logs.String(), `"eb_gap_slots":13`)
	require.Contains(t, logs.String(), `"eb_staleness_slots":5`)
	// Reason-specific message: the applied tip and the primary chain tip are
	// in exact agreement here, so "ledger tip stale vs primary chain tip"
	// would point an operator at the one pair of values that is fine.
	require.Contains(
		t,
		logs.String(),
		"forge skip: corroborated endorser block is ahead of the applied tip",
	)
	require.NotContains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs primary chain tip",
	)
	require.Contains(t, logs.String(), `"gap_slots":0`)
}

// TestForgeEndorserBlockStalenessIsOffByDefault is the regression guard for the
// always-on endorser-block refusal, and the sibling of
// TestForgeUpstreamStalenessIsOffByDefault.
//
// The endorser-block watermark is a NETWORK-stage value: it advances at
// leios-notify announcement time, before a header for that slot has to arrive.
// It is also monotonic and never lowered on a fork. Compared against the
// locally applied tip with an always-on bound, an endorser block corroborated
// for a chain this node does not adopt refuses every leader slot for as long
// as the local chain sits below that slot -- with the applied tip and the
// primary chain tip in agreement and gap_slots reading 0, so every local
// indicator says the node is healthy while the producer goes quiet.
//
// So the bound is 0 (disabled) by default and the path never refuses without
// it. The shape below isolates that path: the watermark is ahead, both local
// tips agree, and the applied tip remains within the no-target sync tolerance.
func TestForgeEndorserBlockStalenessIsOffByDefault(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 360, 300, 300, 0, 360, 0, 0, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		builder.calls,
		"with no endorser-block bound configured, an advisory watermark "+
			"ahead of the local chain must not cost the leader slot",
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.NotContains(t, logs.String(), `"reason":"eb_manifest_ahead"`)
}

// TestForgeProceedsWhenEndorserBlockIsWithinItsBound is the negative case for
// the endorser-block path: with the bound ON, a corroborated endorser block
// that leads the applied tip by less than the bound still forges.
//
// Without this, nothing distinguished "the bound refuses when it should" from
// "the path refuses whenever any endorser block is ahead at all".
func TestForgeProceedsWhenEndorserBlockIsWithinItsBound(t *testing.T) {
	var logs bytes.Buffer
	// eb 304 leads the applied tip at 300 by 4, under the bound of 5.
	forger, builder := newStalenessTestForger(
		t, 320, 300, 300, 0, 304, 0, 0, 5, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		builder.calls,
		"an endorser block within the configured bound must still forge",
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.Contains(t, logs.String(), `"msg":"forge context"`)
	require.Contains(t, logs.String(), `"eb_slot":304`)
}

// TestForgeEndorserBlockBoundDoesNotBorrowThePrimaryChainTipTolerance pins that
// the two knobs are independent in both directions: widening the local
// tolerance must not silence the endorser-block bound, and setting the
// endorser-block bound must not tighten the local coherence check.
func TestForgeEndorserBlockBoundDoesNotBorrowThePrimaryChainTipTolerance(
	t *testing.T,
) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 320, 300, 300, 0, 313, 0, 0, 5, &logs,
	)
	// Local tolerance wide open; only the endorser-block bound is tight.
	forger.slotClock = forgerTestSlotClock{
		currentSlot:           320,
		chainTipSlot:          300,
		primaryTipExplicit:    true,
		primaryTipSlot:        300,
		securityParam:         5,
		primaryTipRelationSet: true,
		primaryTipAncestor:    true,
		primaryTipDepth:       0,
		slotsPerKESPeriod:     100,
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(
		t,
		builder.calls,
		"the endorser-block bound is its own knob; widening the local "+
			"tolerance must not disable it",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.Contains(t, logs.String(), `"max_unapplied_blocks":2`)
	require.Contains(t, logs.String(), `"eb_staleness_slots":5`)
}

// TestForgeIgnoresEndorserBlockSlotBeyondTheCurrentSlot pins the clamp. A
// corroborated slot ahead of the current slot means this node's clock is
// behind, which is a different fault; laundering it into a forge refusal would
// let a clock skew silently stop block production.
func TestForgeIgnoresEndorserBlockSlotBeyondTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 310, 309, 309, 0, 400, 0, 0, 5, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.Contains(t, logs.String(), `"eb_slot":0`)
}

// TestForgeStalenessDoesNotBlockWithoutAReference pins that a forge with no
// upstream reference proceeds rather than being refused. A node with no
// published target must not be prevented from forging by a bound that has
// nothing to measure against.
func TestForgeStalenessDoesNotBlockWithoutAReference(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 300, 299, 299, 0, 0, 0, 5, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls, "no reference must not block forging")
	require.Contains(t, logs.String(), `"msg":"forge context"`)
}

// TestForgeUpstreamStalenessIgnoresUnknownUpstreamTarget pins that a missing
// corroborated target is not substituted with the wall clock or another
// pipeline-stage value. A target-free peer-switch interval is not proof that
// the network is ahead.
func TestForgeUpstreamStalenessIgnoresUnknownUpstreamTarget(t *testing.T) {
	var logs bytes.Buffer
	block := newForgerTestBlock(300, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			// At tip: the previous slot's block, one slot behind the
			// current slot. An unknown target carries no contrary evidence.
			currentSlot:        300,
			chainTipSlot:       299,
			primaryTipExplicit: true,
			primaryTipSlot:     299,
			// The reachable unknown-target state: active upstream, no
			// target published yet.
			upstreamTipSlot:   0,
			upstreamActive:    true,
			slotsPerKESPeriod: 100,
		},
		// The knob is ON and tight. A bound that treated the unknown target
		// as evidence would fire here.
		ForgeUpstreamStalenessSlots: 5,
		PromRegistry:                prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		builder.calls,
		"an unpublished upstream target is not evidence of staleness; the "+
			"node is at tip and its header is what ends that window",
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipAppliedStale),
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeSyncSkip),
		"the sync gate must not claim this slot either; if it does, this "+
			"test is no longer exercising the staleness bound",
	)
}

// TestForgeUpstreamStalenessIsOffByDefault is the regression guard for a
// default-on bound that forfeited leader slots during ordinary operation.
//
// newestKnown counts BLOCKS this node holds; the upstream target is published
// when a HEADER is admitted (recordAdmittedHeaderFrontier advances both the
// admitted frontier and the published target). Between a header's admission at
// slot S and its body being applied, the target reads S while newestKnown is
// still the previous block's slot -- a difference equal to the inter-block gap,
// which is normal operation, not staleness.
//
// With the bound defaulted to 5 every gap above 5 slots refused the leader
// slot: for exponentially distributed gaps with a 20-slot mean that is roughly
// 78% of blocks, on every network. So the default is 0 (disabled), and this
// test pins that a forger built without the knob forges in exactly that shape.
func TestForgeUpstreamStalenessIsOffByDefault(t *testing.T) {
	var logs bytes.Buffer
	// The ordinary header-ahead-of-body window: a header at 318 has been
	// admitted and published as the target, our newest BLOCK is still 299.
	forger, builder := newStalenessTestForger(
		t, 300, 299, 299, 318, 0, 0, 0, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		builder.calls,
		"a header admitted ahead of its body is normal operation; with no "+
			"bound configured it must not cost the leader slot",
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipAppliedStale),
	)
}

// forgeStalenessPanicEbSource is a LeiosVerifiedEbSlot callback that panics,
// standing in for an embedder-supplied implementation that misbehaves. The
// callback is exported configuration, so the forger cannot assume it returns.
type forgeStalenessPanicEbSource struct{ calls int }

func (s *forgeStalenessPanicEbSource) slot() uint64 {
	s.calls++
	panic("endorser-block source boom")
}

// TestForgeRecoversPanicFromEndorserBlockSource pins that a panicking
// LeiosVerifiedEbSlot cannot take down the producer-loop goroutine, the same
// contract every other pluggable forging callback has. A recovered panic means
// "no corroborated endorser block", which is the state a node without the
// signal is in anyway, so the forge proceeds on the remaining evidence rather
// than being refused by a fault in an optional input.
func TestForgeRecoversPanicFromEndorserBlockSource(t *testing.T) {
	var logs bytes.Buffer
	source := &forgeStalenessPanicEbSource{}
	block := newForgerTestBlock(300, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			currentSlot:        300,
			chainTipSlot:       299,
			primaryTipExplicit: true,
			primaryTipSlot:     299,
			slotsPerKESPeriod:  100,
		},
		LeiosVerifiedEbSlot: source.slot,
		// Both bounds on, so a recovered panic cannot be mistaken for the
		// gates simply being disabled.
		ForgeUpstreamStalenessSlots:      50,
		ForgeAppliedTipStalenessSlots:    50,
		ForgeEndorserBlockStalenessSlots: 50,
		PromRegistry:                     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)
	})

	require.Equal(t, 1, source.calls)
	require.Equal(
		t,
		1,
		builder.calls,
		"a panicking optional signal must not cost the leader slot",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues(
				"endorser_block_slot",
			),
		),
	)
	require.Contains(t, logs.String(), "forge callback panic recovered")
}

// forgeStalenessCountingUpstreamClock answers UpstreamSyncTip with a
// DIFFERENT value on every call, so a forge cycle that reads it twice cannot
// agree with itself.
type forgeStalenessCountingUpstreamClock struct {
	forgerTestSlotClock
	calls    *int
	statuses []uint64
}

func (c forgeStalenessCountingUpstreamClock) UpstreamSyncTip() (
	ochainsync.Tip,
	bool,
) {
	i := *c.calls
	*c.calls++
	if i < len(c.statuses) {
		return ochainsync.Tip{
			Point: ocommon.Point{Slot: c.statuses[i]},
		}, true
	}
	return ochainsync.Tip{
		Point: ocommon.Point{Slot: c.statuses[len(c.statuses)-1]},
	}, true
}

func (c forgeStalenessCountingUpstreamClock) UpstreamSyncStatus() (
	uint64,
	bool,
) {
	i := *c.calls
	*c.calls++
	if i < len(c.statuses) {
		return c.statuses[i], true
	}
	return c.statuses[len(c.statuses)-1], true
}

// TestForgeReadsUpstreamSyncTipOncePerCycle pins the single-read contract.
//
// The staleness bound and the pre-existing sync gate both need (target,
// active). Reading the clock twice let one forge cycle evaluate the two
// against different pairs -- LedgerState derives them from the active
// connection and syncUpstreamState, both of which move -- so the
// upstream_target_slot on a refusal could name a target the sync gate never
// saw, and the two gates could disagree about whether the node was behind.
//
// The double returns 0 first and a far-ahead target second. With one read the
// cycle sees only the zero, which is "no target published yet" and no evidence
// of staleness, so the node at tip forges. With two reads the sync gate would
// see the second value and refuse.
func TestForgeReadsUpstreamSyncTipOncePerCycle(t *testing.T) {
	var logs bytes.Buffer
	calls := 0
	block := newForgerTestBlock(300, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgeStalenessCountingUpstreamClock{
			forgerTestSlotClock: forgerTestSlotClock{
				currentSlot:        300,
				chainTipSlot:       299,
				primaryTipExplicit: true,
				primaryTipSlot:     299,
				slotsPerKESPeriod:  100,
			},
			calls:    &calls,
			statuses: []uint64{0, 100000},
		},
		ForgeUpstreamStalenessSlots: 5,
		PromRegistry:                prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		calls,
		"one forge cycle must read UpstreamSyncTip once, so the "+
			"staleness bound, the sync gate and the log line all describe "+
			"the same upstream snapshot",
	)
	require.Equal(t, 1, builder.calls)
}

// TestForgeDoesNotCountOurOwnUnappliedBlockAsLostLeaderSlot pins the
// discriminator on the unapplied-tip refusal. The refusal itself is right for
// every block at the primary chain tip -- forging again for that slot would
// equivocate -- but only a RIVAL block there cost this node a leader slot.
// Our own just-forged block reaches the same branch while the ledger has not
// applied it yet, and it is at a scheduled leader slot by construction: the
// node was elected there, which is why it forged. Counting that as
// unapplied_rival_at_leader_slot reports a lost block for a slot that produced
// one, and raises it to WARN with leader_slot=true.
//
// tipBlockOwnership cannot separate the two here -- it compares against the
// APPLIED tip, which by construction names an earlier block -- so the branch
// compares SlotTracker's hash for the slot against the PRIMARY chain tip's,
// with the forge fence as the weaker fallback.
func TestForgeDoesNotCountOurOwnUnappliedBlockAsLostLeaderSlot(t *testing.T) {
	const leaderSlot = uint64(200)
	appliedHash := bytes.Repeat([]byte{0xAA}, 32)
	primaryTipHash := bytes.Repeat([]byte{0xBB}, 32)
	rivalOurHash := bytes.Repeat([]byte{0xCC}, 32)

	for _, tc := range []struct {
		name string
		// setup runs after the forger is built and before the forge
		// cycle, standing up the ownership signals for the case.
		setup     func(f *BlockForger)
		wantCount float64
		wantLevel string
		wantLog   string
		wantField string
	}{
		{
			// SlotTracker holds our hash for the slot and it is the
			// block on the primary chain tip: provably ours.
			name: "our own block identified by hash is not counted",
			setup: func(f *BlockForger) {
				f.slotTracker.RecordForgedBlock(
					leaderSlot,
					primaryTipHash,
				)
			},
			wantCount: 0,
			wantLevel: `"level":"DEBUG"`,
			wantLog:   "forge skip: slot already has our own block",
			wantField: `"matched_by":"forged_block_hash"`,
		},
		{
			// No tracked hash (a restart drops the in-memory
			// tracker), but the durable fence says this node
			// committed to the slot. Weaker, and deliberately
			// resolved in favour of "ours".
			name: "our own block identified by the fence is not counted",
			setup: func(f *BlockForger) {
				f.fenceLoaded = true
				f.lastForgedSlot = leaderSlot
			},
			wantCount: 0,
			wantLevel: `"level":"DEBUG"`,
			wantLog:   "forge skip: slot already has our own block",
			wantField: `"matched_by":"forge_fence"`,
		},
		{
			// The control: we forged a DIFFERENT block for this
			// slot, so the unapplied block at the tip is a rival's
			// and the slot really was lost. The fence covers the
			// slot too, proving the hash is the stronger signal
			// and does not get masked by it.
			name: "a rival block is still counted",
			setup: func(f *BlockForger) {
				f.slotTracker.RecordForgedBlock(
					leaderSlot,
					rivalOurHash,
				)
				f.fenceLoaded = true
				f.lastForgedSlot = leaderSlot
			},
			wantCount: 1,
			wantLevel: `"level":"WARN"`,
			wantLog: "forge skip: primary chain tip already has " +
				"a block at this slot",
			wantField: `"leader_slot":true`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			forger, builder, broadcaster := newStaleTipTestForgerWithLeader(
				t,
				&forgerScheduleAwareLeader{
					scheduled: map[uint64]struct{}{
						leaderSlot: {},
					},
				},
				leaderSlot,   // current slot
				leaderSlot-2, // ledger-applied tip, still behind
				leaderSlot,   // primary chain tip holds this slot
				appliedHash,
				primaryTipHash,
				&logs,
			)
			tc.setup(forger)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			// The refusal is unconditional either way: a second
			// block for a slot the chain already has one for is
			// never forged, whoever produced the first.
			require.Zero(t, builder.calls)
			require.Zero(t, broadcaster.calls)

			require.Contains(t, logs.String(), tc.wantLog)
			require.Contains(t, logs.String(), tc.wantLevel)
			require.Contains(t, logs.String(), tc.wantField)
			require.Equal(
				t,
				tc.wantCount,
				testutil.ToFloat64(
					forger.metrics.forgeStaleTipSkipUnappliedRival,
				),
			)
		})
	}
}

type forgerTestLeader struct{}

func (forgerTestLeader) ShouldProduceBlock(uint64) bool { return true }

func (forgerTestLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

type forgerBlockingLeader struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once

	mu    sync.Mutex
	calls int
}

func (l *forgerBlockingLeader) ShouldProduceBlock(uint64) bool {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	l.enteredOnce.Do(func() { close(l.entered) })
	<-l.release
	return true
}

func (l *forgerBlockingLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

func (l *forgerBlockingLeader) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

type forgerCountingLeader struct {
	mu    sync.Mutex
	calls int
}

func (l *forgerCountingLeader) ShouldProduceBlock(uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	return true
}

func (l *forgerCountingLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

func (l *forgerCountingLeader) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

// forgerNotLeader counts slot checks and never wins one, so every cycle of
// the producer loop reaches the leader check and moves the count. When
// cancelAt is set it calls cancel from inside that check, so the
// cancellation lands at a known point in the loop.
type forgerNotLeader struct {
	forgerCountingLeader
	cancelAt int
	cancel   context.CancelFunc
}

func (l *forgerNotLeader) ShouldProduceBlock(slot uint64) bool {
	l.forgerCountingLeader.ShouldProduceBlock(slot)
	if l.cancelAt > 0 && l.callCount() == l.cancelAt {
		l.cancel()
	}
	return false
}

// forgerFastSlotClock ends each slot a few milliseconds ahead so the
// slot-aligned loop cycles quickly.
type forgerFastSlotClock struct {
	forgerTestSlotClock
}

func (forgerFastSlotClock) NextSlotTime() (time.Time, error) {
	return time.Now().Add(5 * time.Millisecond), nil
}

// A fatal component error, such as a ledger rollover that cannot apply its
// reward update, cancels the node context the producer loop runs under. The
// loop must then exit and check no further slots, so the node stops forging
// on a ledger that halted.
func TestForgerStopsCheckingSlotsWhenItsContextIsCancelled(t *testing.T) {
	t.Parallel()

	// No deferred Stop: Stop waits for the loop, so on the failure this test
	// exists to catch it would hang the package instead of failing the test.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leader := &forgerNotLeader{cancelAt: 2, cancel: cancel}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerFastSlotClock{forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		}},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	require.NoError(t, forger.Start(ctx))

	require.Eventually(t, func() bool {
		return !forger.IsRunning()
	}, 5*time.Second, time.Millisecond,
		"producer loop kept running after its context was cancelled")
	require.Equal(t, 2, leader.callCount(),
		"producer loop checked a slot after its context was cancelled")
}

type forgerTestSlotClock struct {
	currentSlot         uint64
	chainTipSlot        uint64
	chainTipHash        []byte
	chainTipBlockNumber uint64
	// primaryTipExplicit selects whether primaryTipSlot/primaryTipHash are
	// used verbatim. When false the primary tip mirrors the applied tip,
	// which is the caught-up steady state and what every test that does not
	// care about the distinction wants.
	primaryTipExplicit     bool
	primaryTipSlot         uint64
	primaryTipHash         []byte
	primaryTipBlockNumber  uint64
	primaryTipRelationSet  bool
	primaryTipAncestor     bool
	primaryTipDepth        uint64
	upstreamTipSlot        uint64
	upstreamTipBlockNumber uint64
	upstreamActive         bool
	securityParam          int
	slotsPerKESPeriod      uint64
}

func (c forgerTestSlotClock) CurrentSlot() (uint64, error) {
	return c.currentSlot, nil
}

func (c forgerTestSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

func (c forgerTestSlotClock) ChainTip() ocommon.Point {
	return ocommon.Point{Slot: c.chainTipSlot, Hash: c.chainTipHash}
}

func (c forgerTestSlotClock) ChainTipSnapshot() ochainsync.Tip {
	return ochainsync.Tip{
		Point:       c.ChainTip(),
		BlockNumber: c.chainTipBlockNumber,
	}
}

func (c forgerTestSlotClock) ForgeTipSnapshot() (ochainsync.Tip, int) {
	return c.ChainTipSnapshot(), c.SecurityParam()
}

// PrimaryChainTip mirrors the applied tip unless the test describes a primary
// tip of its own. Mirroring is the caught-up steady state, so a test that sets
// no primary chain tip field observes no backlog and no divergence.
//
// Setting primaryTipSlot or primaryTipHash is itself enough to opt in: a test
// that set primaryTipSlot but forgot primaryTipExplicit would otherwise
// silently get the mirrored applied tip, so its gap would read 0 and it would
// pass no matter what the forger did -- which is exactly what happened to the
// configurable tolerance test. primaryTipExplicit remains for the one case the
// values cannot express on their own: an explicitly empty primary tip (slot 0,
// no hash), which is an uninitialised primary chain.
//
// The values are used verbatim, including a primary tip BEHIND the applied
// tip, which is a real state the forger must handle and which a clamp would
// hide.
func (c forgerTestSlotClock) PrimaryChainTip() ocommon.Point {
	if !c.primaryTipExplicit && c.primaryTipSlot == 0 &&
		c.primaryTipHash == nil {
		return ocommon.Point{Slot: c.chainTipSlot, Hash: c.chainTipHash}
	}
	return ocommon.Point{Slot: c.primaryTipSlot, Hash: c.primaryTipHash}
}

func (c forgerTestSlotClock) PrimaryChainTipRelation(
	point ocommon.Point,
) (ochainsync.Tip, uint64, bool, error) {
	primary := c.PrimaryChainTip()
	depth := uint64(0)
	ancestor := true
	if c.primaryTipRelationSet {
		return ochainsync.Tip{
			Point:       primary,
			BlockNumber: c.primaryTipBlockNumber,
		}, c.primaryTipDepth, c.primaryTipAncestor, nil
	}
	if primary.Slot < point.Slot {
		ancestor = false
	} else if primary.Slot == point.Slot && len(primary.Hash) > 0 &&
		len(point.Hash) > 0 && !bytes.Equal(primary.Hash, point.Hash) {
		ancestor = false
	} else if c.primaryTipBlockNumber > c.chainTipBlockNumber {
		depth = c.primaryTipBlockNumber - c.chainTipBlockNumber
	} else if primary.Slot > point.Slot {
		depth = primary.Slot - point.Slot
	}
	return ochainsync.Tip{
		Point:       primary,
		BlockNumber: c.primaryTipBlockNumber,
	}, depth, ancestor, nil
}

// NextSlotTime reports a boundary that is still ahead, which is what a
// healthy clock reports for a leader forging inside its own slot. Handing
// back the current instant would instead mean the slot has already closed,
// and endorser-block production is skipped for a closed slot.
func (forgerTestSlotClock) NextSlotTime() (time.Time, error) {
	return time.Now().Add(time.Second), nil
}

// ChainTipHash satisfies the optional ChainTipHashProvider. It returns
// nil unless a test sets chainTipHash, so every existing test keeps the
// fence-only behaviour.
func (c forgerTestSlotClock) ChainTipHash() []byte {
	return c.chainTipHash
}

func (c forgerTestSlotClock) UpstreamTipSlot() uint64 {
	return c.upstreamTipSlot
}

func (c forgerTestSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return c.upstreamTipSlot, c.upstreamActive || c.upstreamTipSlot > 0
}

func (c forgerTestSlotClock) UpstreamSyncTip() (ochainsync.Tip, bool) {
	return ochainsync.Tip{
		Point:       ocommon.Point{Slot: c.upstreamTipSlot},
		BlockNumber: c.upstreamTipBlockNumber,
	}, c.upstreamActive || c.upstreamTipSlot > 0
}

func (c forgerTestSlotClock) SecurityParam() int {
	if c.securityParam > 0 {
		return c.securityParam
	}
	return 5
}

// TestCheckAndForgeProductionAllowsUnknownActiveUpstreamTarget verifies that
// an active upstream with no admitted target does not suppress forging based on
// wall-clock distance from the local tip. That distance describes a network
// quiet stretch, not whether a peer is ahead.
func TestCheckAndForgeProductionAllowsUnknownActiveUpstreamTarget(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(1000, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			// The tip lags the current slot by 991 slots, well past the
			// tolerance below, so this node is behind on its own reckoning.
			currentSlot:       1000,
			chainTipSlot:      9,
			upstreamActive:    true,
			slotsPerKESPeriod: 100000,
		},
		ForgeSyncToleranceSlots: 99,
		PromRegistry:            prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
}

func TestCheckAndForgeProductionStopsAtProtocolKESExpiry(t *testing.T) {
	creds := setupTestCredentials(t)
	genesis := synthGenesis(1, 2, time.Second, time.Unix(0, 0))
	require.NoError(t, creds.ValidateKESPeriod(genesis, 0))

	block := newForgerTestBlock(1, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	clock := &forgerTestSlotClock{
		currentSlot:       1,
		chainTipSlot:      0,
		slotsPerKESPeriod: 1,
	}
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(&logs, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	// The final period in [start, start+maxEvolutions) remains valid.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, leader.callCount())
	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
	lastValidCurrent := testutil.ToFloat64(forger.metrics.currentKESPeriod)
	lastValidRemaining := testutil.ToFloat64(
		forger.metrics.remainingKESPeriods,
	)
	lastValidExpiry := testutil.ToFloat64(forger.metrics.opCertExpiryKES)

	// The same loaded producer reaches the first expired period while running.
	clock.currentSlot = 2
	clock.chainTipSlot = 1
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		1,
		leader.callCount(),
		"expired period must not run leader selection",
	)
	require.Equal(t, 1, builder.calls, "expired period must not build a block")
	require.Equal(
		t,
		1,
		broadcaster.calls,
		"expired period must not broadcast a block",
	)
	require.Contains(t, logs.String(), "operational certificate expired")
	require.Equal(t, float64(1), lastValidCurrent)
	require.Equal(t, float64(1), lastValidRemaining)
	require.Equal(t, float64(2), lastValidExpiry)

	require.Equal(
		t,
		float64(2),
		testutil.ToFloat64(forger.metrics.currentKESPeriod),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.remainingKESPeriods),
	)
	require.Equal(
		t,
		float64(2),
		testutil.ToFloat64(forger.metrics.opCertExpiryKES),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

func TestCheckAndForgeProductionStopsBeforeOpCertStart(t *testing.T) {
	creds := setupTestCredentials(t)
	creds.mu.Lock()
	creds.generation++
	creds.opCert.KESPeriod = 5
	creds.opCertStartKES = 5
	creds.maxKESEvolutions = 2
	creds.opCertExpiryKES = 7
	creds.opCertValidated = true
	creds.mu.Unlock()

	block := newForgerTestBlock(4, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leiosChecker := &forgerTestLeiosChecker{reason: "not eligible"}
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(&logs, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       4,
			chainTipSlot:      3,
			slotsPerKESPeriod: 1,
		},
		LeiosProduceChecker: leiosChecker,
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosMempool:        forgerTestMempoolProvider{},
		LeiosTxValidator:    &mockTxValidator{},
		PromRegistry:        prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(
		t,
		forger.checkAndForgeProduction(context.Background()),
		"pre-start policy gate must decline before KES evolution",
	)
	require.Zero(
		t,
		leader.callCount(),
		"pre-start period must not run leader selection",
	)
	require.Zero(t, leiosChecker.calls, "pre-start period must not run Leios")
	require.Zero(t, builder.calls, "pre-start period must not build a block")
	require.Zero(
		t,
		broadcaster.calls,
		"pre-start period must not broadcast a block",
	)
	require.Contains(
		t,
		logs.String(),
		"operational certificate is not yet valid",
	)
	require.Equal(
		t,
		float64(4),
		testutil.ToFloat64(forger.metrics.currentKESPeriod),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.remainingKESPeriods),
	)
	require.Equal(
		t,
		float64(5),
		testutil.ToFloat64(forger.metrics.opCertStartKES),
	)
	require.Equal(
		t,
		float64(7),
		testutil.ToFloat64(forger.metrics.opCertExpiryKES),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

func TestCheckAndForgeProductionCountsKESUpdateFailure(t *testing.T) {
	creds := setupTestCredentials(t)
	require.NoError(t, creds.UpdateKESPeriod(1))

	builder := &forgerTestBuilder{block: newForgerTestBlock(1, 2)}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "failed to update KES period")
	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

func TestSignBlockHeaderEnforcesProtocolKESLifetime(t *testing.T) {
	creds := setupTestCredentials(t)
	creds.mu.Lock()
	creds.generation++
	creds.opCertStartKES = 0
	creds.maxKESEvolutions = 2
	creds.opCertExpiryKES = 2
	creds.opCertValidated = true
	creds.mu.Unlock()
	require.NoError(t, creds.UpdateKESPeriod(2))

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			slotsPerKESPeriod: 1,
		},
	})
	require.NoError(t, err)

	signature, err := forger.SignBlockHeader(2, []byte("expired header"))
	require.ErrorIs(t, err, errOpCertExpired)
	require.Nil(t, signature)
}

func TestCheckAndForgeProductionRejectsIdentityReloadDuringSelection(
	t *testing.T,
) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(vrfPath, kesPath, opCertPath))
	require.NoError(t, creds.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	leader := &forgerBlockingLeader{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(1, 2),
	}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 1,
		},
	})
	require.NoError(t, err)

	forgeDone := make(chan error, 1)
	go func() {
		forgeDone <- forger.checkAndForgeProduction(context.Background())
	}()
	dingotestutil.RequireReceive(
		t,
		leader.entered,
		dingotestutil.AsyncWait,
		"leader entered",
	)

	alternateVRFPath := createAlternateTestVRFKey(t)
	reloadDone := make(chan error, 1)
	go func() {
		reloadDone <- creds.LoadFromFiles(
			alternateVRFPath,
			kesPath,
			opCertPath,
		)
	}()
	reloadErr := dingotestutil.RequireReceive(
		t,
		reloadDone,
		dingotestutil.AsyncWait,
		"identity-changing reload completion",
	)
	require.ErrorContains(t, reloadErr, "cannot change pool or VRF identity")
	close(leader.release)
	require.NoError(t, dingotestutil.RequireReceive(
		t,
		forgeDone,
		dingotestutil.AsyncWait,
		"forge completion",
	))
	require.Equal(t, 1, leader.callCount())
	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
}

type forgerReentrantBuilder struct {
	callback    func() error
	callbackErr error
	block       ledger.Block
	cbor        []byte
	calls       int
}

func (b *forgerReentrantBuilder) BuildBlock(ctx context.Context,
	_ uint64,
	_ uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	if b.callback != nil {
		b.callbackErr = b.callback()
	}
	if b.callbackErr != nil {
		return nil, nil, b.callbackErr
	}
	return b.block, b.cbor, nil
}

func TestCheckAndForgeProductionRejectsReentrantBuilderReload(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(vrfPath, kesPath, opCertPath))
	require.NoError(t, creds.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	block := newForgerTestBlock(1, 2)
	builder := &forgerReentrantBuilder{
		block: block,
		cbor:  block.cbor,
		callback: func() error {
			if err := creds.LoadFromFiles(
				vrfPath,
				kesPath,
				opCertPath,
			); err != nil {
				return err
			}
			return creds.ValidateKESPeriod(
				synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
				0,
			)
		},
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &forgerTestSlotClock{
		currentSlot:       1,
		chainTipSlot:      0,
		slotsPerKESPeriod: 1,
	}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	forgeDone := make(chan error, 1)
	go func() {
		forgeDone <- forger.checkAndForgeProduction(context.Background())
	}()
	forgeErr := dingotestutil.RequireReceive(
		t,
		forgeDone,
		dingotestutil.AsyncWait,
		"reentrant builder reload completion",
	)
	require.ErrorContains(t, forgeErr, "credential generation changed")
	require.NoError(t, builder.callbackErr)
	require.Equal(t, 1, builder.calls)
	require.Zero(
		t,
		broadcaster.calls,
		"stale builder output must not be adopted",
	)
}

func TestCheckAndForgeProductionRejectsReentrantLeiosRevalidation(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	genesis := synthGenesis(1, 3, time.Second, time.Unix(0, 0))
	require.NoError(t, creds.ValidateKESPeriod(genesis, 0))

	builder := &forgerTestBuilder{
		block: newForgerTestBlock(1, 2),
	}
	broadcaster := &forgerTestBroadcaster{}
	leiosChecker := &forgerTestLeiosChecker{
		reason: "revalidated",
		callback: func() error {
			return creds.ValidateKESPeriod(genesis, 0)
		},
	}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 1,
		},
		LeiosProduceChecker: leiosChecker,
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosMempool:        forgerTestMempoolProvider{},
		LeiosTxValidator:    &mockTxValidator{},
	})
	require.NoError(t, err)

	forgeDone := make(chan error, 1)
	go func() {
		forgeDone <- forger.checkAndForgeProduction(context.Background())
	}()
	require.NoError(t, dingotestutil.RequireReceive(
		t,
		forgeDone,
		dingotestutil.AsyncWait,
		"reentrant Leios revalidation completion",
	))
	require.NoError(t, leiosChecker.callbackErr)
	require.Equal(t, 1, leiosChecker.calls)
	require.Zero(t, builder.calls, "stale Leios attempt must not build")
	require.Zero(
		t,
		broadcaster.calls,
		"stale Leios attempt must not be adopted",
	)
}

func TestCheckAndForgeProductionFailsClosedAfterKESRevalidation(t *testing.T) {
	creds := setupTestCredentials(t)
	require.NoError(t, creds.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	block := newForgerTestBlock(1, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	var logs bytes.Buffer
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(&logs, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 1,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	revalidationErr := creds.ValidateKESPeriod(
		synthGenesis(1, 1, time.Second, time.Unix(0, 0)),
		1,
	)
	require.ErrorContains(t, revalidationErr, "operational certificate expired")

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Zero(t, builder.calls, "failed revalidation must disable building")
	require.Zero(
		t,
		broadcaster.calls,
		"failed revalidation must disable broadcasting",
	)
	require.Contains(t, logs.String(), "KES protocol lifetime is not validated")
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.remainingKESPeriods),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.opCertExpiryKES),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

func TestNewBlockForgerRejectsUnvalidatedKESLifetime(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(vrfPath, kesPath, opCertPath))

	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			slotsPerKESPeriod: 1,
		},
	})
	require.ErrorContains(t, err, "validated KES protocol lifetime")
}

func TestNewBlockForgerRejectsInvalidOpCertGeneration(t *testing.T) {
	vrfPath, kesPath, _ := createTestKeys(t)
	corrupted := strings.Replace(testOpCertJSON, "89fc9e9f", "88fc9e9f", 1)
	require.NotEqual(t, testOpCertJSON, corrupted)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(
		vrfPath,
		kesPath,
		writeTestOpCert(t, corrupted),
	))
	require.ErrorContains(
		t,
		creds.ValidateKESPeriod(
			synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
			0,
		),
		"signature verification failed",
	)

	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			slotsPerKESPeriod: 1,
		},
	})
	require.ErrorContains(t, err, "operational certificate is not validated")
}

type forgerTestBuilder struct {
	block        ledger.Block
	cbor         []byte
	calls        int
	leiosCalls   int
	contextCalls int
	blockCtx     BlockContext
	leiosData    LeiosBlockData
	// onBuild, when set, runs at the moment a build entry point is
	// invoked, so a test can observe state as of block assembly.
	onBuild func()
}

func (b *forgerTestBuilder) noteBuild() {
	if b.onBuild != nil {
		b.onBuild()
	}
}

func (b *forgerTestBuilder) BuildBlock(context.Context,
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	b.noteBuild()
	return b.block, b.cbor, nil
}

func (b *forgerTestBuilder) BuildBlockWithLeios(ctx context.Context,
	_ uint64,
	_ uint64,
	leiosData LeiosBlockData,
) (ledger.Block, []byte, error) {
	b.leiosCalls++
	b.noteBuild()
	b.leiosData = leiosData
	return b.block, b.cbor, nil
}

// BuildBlockOnContext makes forgerTestBuilder an AlternativeBlockBuilder, so
// tests can wire the equal-slot alternative path. It records the context it
// was handed; the forger only reaches it when a test also supplies a
// ChainContext and a SiblingAdopter.
func (b *forgerTestBuilder) BuildBlockOnContext(ctx context.Context,
	_ uint64,
	_ uint64,
	leiosData LeiosBlockData,
	blockCtx BlockContext,
) (ledger.Block, []byte, error) {
	b.contextCalls++
	b.noteBuild()
	b.blockCtx = blockCtx
	b.leiosData = leiosData
	return b.block, b.cbor, nil
}

type forgerTestBroadcaster struct {
	err   error
	panic bool
	calls int
}

func (b *forgerTestBroadcaster) AddBlock(context.Context,
	ledger.Block,
	[]byte,
) error {
	b.calls++
	if b.panic {
		panic("broadcaster panic")
	}
	return b.err
}

// forgerTestPanicOnceLeader panics on its first ShouldProduceBlock
// call and reports leadership normally afterward, for exercising the
// forge cycle that follows a recovered panic.
type forgerTestPanicOnceLeader struct {
	calls int
}

func (l *forgerTestPanicOnceLeader) ShouldProduceBlock(uint64) bool {
	l.calls++
	if l.calls == 1 {
		panic("leader check panic")
	}
	return true
}

func (l *forgerTestPanicOnceLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, true
}

type forgerTestBlock struct {
	hash         lcommon.Blake2b256
	prevHash     lcommon.Blake2b256
	slot         uint64
	blockNumber  uint64
	cbor         []byte
	transactions []lcommon.Transaction
}

func newForgerTestBlock(slot, blockNumber uint64) *forgerTestBlock {
	return &forgerTestBlock{
		hash:        lcommon.NewBlake2b256(bytes.Repeat([]byte{0x01}, 32)),
		prevHash:    lcommon.NewBlake2b256(bytes.Repeat([]byte{0x02}, 32)),
		slot:        slot,
		blockNumber: blockNumber,
		cbor:        []byte{0x83, 0x01, 0x02},
	}
}

func (b *forgerTestBlock) Header() lcommon.BlockHeader { return b }

func (b *forgerTestBlock) Type() int { return int(babbage.BlockTypeBabbage) }
func (b *forgerTestBlock) Transactions() []lcommon.Transaction {
	return b.transactions
}
func (b *forgerTestBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}
func (b *forgerTestBlock) Hash() lcommon.Blake2b256 { return b.hash }

func (b *forgerTestBlock) PrevHash() lcommon.Blake2b256 { return b.prevHash }

func (b *forgerTestBlock) BlockNumber() uint64 { return b.blockNumber }
func (b *forgerTestBlock) SlotNumber() uint64  { return b.slot }

func (b *forgerTestBlock) IssuerVkey() lcommon.IssuerVkey { return lcommon.IssuerVkey{} }
func (b *forgerTestBlock) BlockBodySize() uint64          { return 0 }

func (b *forgerTestBlock) Era() lcommon.Era { return babbage.EraBabbage }
func (b *forgerTestBlock) Cbor() []byte     { return b.cbor }

func (b *forgerTestBlock) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

type forgerTestLeiosChecker struct {
	calls       int
	allowed     bool
	reason      string
	err         error
	callback    func() error
	callbackErr error
}

type forgerTestConfirmedTxRemover struct {
	hashes []string
}

func (r *forgerTestConfirmedTxRemover) RemoveTxsByHash(hashes []string) {
	r.hashes = append(r.hashes, hashes...)
}

func TestCheckAndForgeProductionRemovesConfirmedTransactions(t *testing.T) {
	creds := setupTestCredentials(t)
	tx, err := conway.NewConwayTransactionFromCbor(
		makeMinimalTxCbor(t, 0x42, 0),
	)
	require.NoError(t, err)
	block := newForgerTestBlock(10, 2)
	block.transactions = []lcommon.Transaction{tx}
	remover := &forgerTestConfirmedTxRemover{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster: &forgerTestBroadcaster{},
		ConfirmedTxs:     remover,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, []string{tx.Hash().String()}, remover.hashes)
}

func TestCheckAndForgeProductionUsesRetainedReconnectFrontier(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(114220801, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       114220801,
			chainTipSlot:      114220600,
			upstreamTipSlot:   114220800,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeSyncSkip),
	)
}

func TestCheckAndForgeProductionWaitsForEventPairedCorroboratedTarget(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(101, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       101,
			chainTipSlot:      100,
			upstreamTipSlot:   200,
			upstreamActive:    true,
			slotsPerKESPeriod: 100,
		},
		ForgeSyncToleranceSlots: 99,
		PromRegistry:            prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
}

func TestCheckAndForgeProductionProceedsWithoutUpstreamFrontier(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			// This is the value exposed after a close-before-switch event.
			upstreamTipSlot: 0,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
}

func (c *forgerTestLeiosChecker) MayProduceEndorserBlock(
	uint64,
) (bool, string, error) {
	c.calls++
	if c.callback != nil {
		c.callbackErr = c.callback()
		if c.callbackErr != nil {
			return false, "", c.callbackErr
		}
	}
	return c.allowed, c.reason, c.err
}

type forgerTestLeiosCaster struct {
	slot     uint64
	hash     []byte
	cbor     []byte
	txBodies [][]byte
}

func (c *forgerTestLeiosCaster) BroadcastEndorserBlock(
	slot uint64,
	hash []byte,
	cbor []byte,
	txBodies [][]byte,
) error {
	c.slot = slot
	c.hash = append([]byte(nil), hash...)
	c.cbor = append([]byte(nil), cbor...)
	c.txBodies = append([][]byte(nil), txBodies...)
	return nil
}

type forgerTestMempoolProvider struct {
	txs []MempoolTransaction
}

func (p forgerTestMempoolProvider) Transactions() []MempoolTransaction {
	return p.txs
}

type forgerTestLeiosCerts struct {
	eligible       []LeiosCertifiedEndorserBlock
	txHashes       []string
	txHashesOK     bool
	marked         []lcommon.Blake2b256
	markedSlots    []uint64
	gotEbSlot      uint64
	gotEbSlotCalls int
}

func (p *forgerTestLeiosCerts) EligibleCertifiedEndorserBlocks() []LeiosCertifiedEndorserBlock {
	return p.eligible
}

func (p *forgerTestLeiosCerts) CertifiedEndorserBlockTxHashes(
	_ lcommon.Blake2b256,
	ebSlot uint64,
) ([]string, bool) {
	p.gotEbSlot = ebSlot
	p.gotEbSlotCalls++
	return p.txHashes, p.txHashesOK
}

func (p *forgerTestLeiosCerts) MarkEndorserBlockEmbedded(
	ebHash lcommon.Blake2b256,
	ebSlot uint64,
) {
	p.marked = append(p.marked, ebHash)
	p.markedSlots = append(p.markedSlots, ebSlot)
}

type forgerTestLeiosParentAnnouncement struct {
	rbHash lcommon.Blake2b256
	hash   lcommon.Blake2b256
	ok     bool
	err    error
	calls  int
	// rbHashAfterFirst, when set, is returned from the second call onward.
	// This is how a test moves the chain tip underneath an already-resolved
	// Leios selection: the forger resolves the parent once and re-reads it
	// before building, so a different second answer is exactly a tip that
	// advanced while the Leios work was running.
	rbHashAfterFirst *lcommon.Blake2b256
}

func (p *forgerTestLeiosParentAnnouncement) ParentLeiosAnnouncement(ctx context.Context) (
	lcommon.Blake2b256,
	lcommon.Blake2b256,
	bool,
	error,
) {
	p.calls++
	if p.calls > 1 && p.rbHashAfterFirst != nil {
		return *p.rbHashAfterFirst, p.hash, p.ok, p.err
	}
	return p.rbHash, p.hash, p.ok, p.err
}

// TestCheckAndForgeProductionSkipsObserverWhenNotAdopted holds the
// contract that the blockForged observer publishes only after durable
// acceptance. The production observer republishes the block on the event
// bus and enqueues the Leios announcement that diffuses it to peers, so
// running it for a block AddBlock rejected would advertise a block this
// node never adopted.
//
// The forgeForged counter still increments before adoption, which is what
// the forge metrics require: build-versus-adopt remains observable through
// forgeForged and forgeCouldNot without publishing an unadopted block.
func TestCheckAndForgeProductionSkipsObserverWhenNotAdopted(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	blockCbor := []byte{0x83, 0xaa, 0xbb}
	builder := &forgerTestBuilder{
		block: block,
		cbor:  blockCbor,
	}
	innerBroadcaster := &forgerTestBroadcaster{
		err: errors.New("not adopted"),
	}
	var callOrder []string
	broadcaster := &trackingBroadcaster{
		inner: innerBroadcaster,
		onAdd: func() { callOrder = append(callOrder, "adopt") },
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockForged: func(
			ledger.Block,
			[]byte,
			time.Duration,
		) {
			callOrder = append(callOrder, "observe")
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to add block")

	assert.Equal(t, []string{"adopt"}, callOrder)
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, innerBroadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(0), testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

// TestCheckAndForgeProductionObservesForgedBlockAfterAdoption is the
// positive half of the contract: the observer runs, with the built block
// and CBOR, once AddBlock has accepted the block.
func TestCheckAndForgeProductionObservesForgedBlockAfterAdoption(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	blockCbor := []byte{0x83, 0xaa, 0xbb}
	builder := &forgerTestBuilder{
		block: block,
		cbor:  blockCbor,
	}
	innerBroadcaster := &forgerTestBroadcaster{}
	var callOrder []string
	broadcaster := &trackingBroadcaster{
		inner: innerBroadcaster,
		onAdd: func() { callOrder = append(callOrder, "adopt") },
	}
	var (
		observedBlock   ledger.Block
		observedCbor    []byte
		observedLatency time.Duration
	)

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockForged: func(
			block ledger.Block,
			cbor []byte,
			latency time.Duration,
		) {
			callOrder = append(callOrder, "observe")
			observedBlock = block
			observedCbor = append([]byte(nil), cbor...)
			observedLatency = latency
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Same(t, block, observedBlock)
	assert.Equal(t, blockCbor, observedCbor)
	assert.GreaterOrEqual(t, observedLatency, time.Duration(0))
	assert.Equal(t, []string{"adopt", "observe"}, callOrder)
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, innerBroadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversBlockForgedObserverPanic(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	blockCbor := []byte{0x83, 0xaa, 0xbb}
	builder := &forgerTestBuilder{
		block: block,
		cbor:  blockCbor,
	}
	broadcaster := &forgerTestBroadcaster{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockForged: func(
			ledger.Block,
			[]byte,
			time.Duration,
		) {
			panic("observer panic")
		},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversLeaderCheckPanic(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leader := &forgerTestPanicOnceLeader{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	// A panic from the leader checker must not escape checkAndForgeProduction
	// (which would otherwise crash the producer-loop goroutine); it is
	// treated as "not leader" for the slot, same as a checker that simply
	// returns false.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 1, leader.calls)
	assert.Equal(t, 0, builder.calls)
	assert.Equal(t, 0, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeNotLeader),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues("selection"),
		),
	)

	// The following forge cycle proceeds normally: worker accounting and
	// running state were not corrupted by the recovered panic.
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 2, leader.calls)
	assert.Equal(t, 1, builder.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversBlockValidatorPanic(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	validator := &forgerTestValidator{panic: true}

	forger, clock := newForgerWithValidator(
		t, block, nil, broadcaster, validator,
	)

	// A panic from the validator must not escape checkAndForgeProduction; it
	// is treated as a validation failure so the block is dropped rather than
	// adopted with unknown validity.
	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "self-validation failed")
	assert.Equal(t, 1, validator.calls)
	assert.Equal(t, 0, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeValidationFailed),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues("validation"),
		),
	)

	// The following forge cycle proceeds normally. It runs at the next
	// slot because the fence refuses a slot already signed for.
	validator.panic = false
	clock.currentSlot = 11
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 2, validator.calls)
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestCheckAndForgeProductionRecoversBlockBroadcasterPanic(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{panic: true}
	clock := &forgerTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	// A panic from the broadcaster must not escape checkAndForgeProduction;
	// it is treated as a publish failure, matching the existing error path
	// for a broadcaster that returns an error.
	err = forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to add block")
	assert.Equal(t, 1, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeForged))
	assert.Equal(t, float64(0), testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgePanicRecovered.WithLabelValues("publication"),
		),
	)

	// The following forge cycle proceeds normally. It runs at the next
	// slot because the fence refuses a slot already signed for.
	broadcaster.panic = false
	clock.currentSlot = 11
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	assert.Equal(t, 2, broadcaster.calls)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
}

func TestNewBlockForgerRejectsProductionLeiosWithoutTxValidator(t *testing.T) {
	creds := setupTestCredentials(t)
	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosMempool:        forgerTestMempoolProvider{},
	})
	require.EqualError(
		t,
		err,
		"production Leios forging requires transaction validator",
	)
}

func TestCheckAndForgeProductionAnnouncesForgedLeiosEB(t *testing.T) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leiosChecker := &forgerTestLeiosChecker{allowed: true}
	leiosCaster := &forgerTestLeiosCaster{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosProduceChecker: leiosChecker,
		LeiosEBBroadcaster:  leiosCaster,
		LeiosTxValidator:    &mockTxValidator{},
		LeiosMempool: forgerTestMempoolProvider{
			txs: []MempoolTransaction{
				{
					Hash: strings.Repeat("11", 32),
					Cbor: makeMinimalTxCbor(t, 0x11, 0),
					Type: conway.TxTypeConway,
				},
			},
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, leiosChecker.calls)
	require.NotEmpty(t, leiosCaster.hash)
	require.Equal(t, uint64(10), leiosCaster.slot)
	require.Equal(t, 1, builder.leiosCalls)
	require.NotNil(t, builder.leiosData.Announcement)
	require.Nil(t, builder.leiosData.Certificate)
	assert.Equal(
		t,
		leiosCaster.hash,
		builder.leiosData.Announcement.Hash.Bytes(),
	)
	assert.Equal(
		t,
		uint64(len(leiosCaster.cbor)),
		builder.leiosData.Announcement.Size,
	)
}

func TestCheckAndForgeProductionCertifiesLeiosEBAfterAdoption(t *testing.T) {
	for _, test := range []struct {
		name        string
		txHashesOK  bool
		canAnnounce bool
	}{
		{name: "closure available", txHashesOK: true, canAnnounce: true},
		{name: "closure unavailable", txHashesOK: false, canAnnounce: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			creds := setupTestCredentials(t)
			block := newForgerTestBlock(10, 2)
			builder := &forgerTestBuilder{block: block, cbor: block.cbor}
			broadcaster := &forgerTestBroadcaster{}
			ebHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x33}, 32))
			rbHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32))
			cert := &lcommon.LeiosEbCertificate{
				SlotNo:            9,
				EndorserBlockHash: ebHash,
				Signers:           []byte{0x80},
				AggregatedSignature: make(
					[]byte,
					lcommon.LeiosBlsSignatureSize,
				),
			}
			leiosCerts := &forgerTestLeiosCerts{
				txHashes:   []string{strings.Repeat("11", 32)},
				txHashesOK: test.txHashesOK,
				eligible: []LeiosCertifiedEndorserBlock{
					{
						SlotNo:            9,
						EndorserBlockHash: ebHash,
						Certificate:       cert,
						AnnouncingRbHash:  rbHash,
					},
				},
			}
			parent := &forgerTestLeiosParentAnnouncement{
				rbHash: rbHash, hash: ebHash, ok: true,
			}
			leiosChecker := &forgerTestLeiosChecker{allowed: true}
			leiosCaster := &forgerTestLeiosCaster{}

			forger, err := NewBlockForger(ForgerConfig{
				Mode: ModeProduction,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
				Credentials:      creds,
				LeaderChecker:    forgerTestLeader{},
				BlockBuilder:     builder,
				BlockBroadcaster: broadcaster,
				SlotClock: forgerTestSlotClock{
					currentSlot:       10,
					chainTipSlot:      9,
					slotsPerKESPeriod: 100,
				},
				LeiosCertificateProvider:        leiosCerts,
				LeiosParentAnnouncementProvider: parent,
				LeiosProduceChecker:             leiosChecker,
				LeiosEBBroadcaster:              leiosCaster,
				LeiosTxValidator:                &mockTxValidator{},
				LeiosMempool: forgerTestMempoolProvider{
					txs: []MempoolTransaction{
						{
							Hash: strings.Repeat("11", 32),
							Cbor: makeMinimalTxCbor(t, 0x11, 0),
							Type: conway.TxTypeConway,
						},
						{
							Hash: strings.Repeat("22", 32),
							Cbor: makeMinimalTxCbor(t, 0x22, 0),
							Type: conway.TxTypeConway,
						},
					},
				},
				PromRegistry: prometheus.NewRegistry(),
			})
			require.NoError(t, err)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			require.Equal(t, 1, builder.leiosCalls)
			require.Same(t, cert, builder.leiosData.Certificate)
			require.Equal(t, test.canAnnounce, leiosChecker.calls == 1)
			if test.canAnnounce {
				require.NotNil(t, builder.leiosData.Announcement)
				require.NotEmpty(t, leiosCaster.hash)
				require.Equal(
					t,
					[][]byte{makeMinimalTxCbor(t, 0x22, 0)},
					leiosCaster.txBodies,
				)
			} else {
				require.Nil(t, builder.leiosData.Announcement)
				require.Empty(t, leiosCaster.hash)
			}
			require.Equal(t, []lcommon.Blake2b256{ebHash}, leiosCerts.marked)
			require.Equal(t, []uint64{9}, leiosCerts.markedSlots)
			// Twice: once to resolve the certificate's parent, then again
			// before the build to ensure endorser-block production did not
			// move that parent. See buildBlockForSlot.
			require.Equal(t, 2, parent.calls)
			// CertifiedEndorserBlockTxHashes must be called with the
			// eligible certificate's own slot (9, from eb.SlotNo above), not
			// the forged ranking block's slot (10) or zero: the manifest is
			// content-addressed, so the same hash could be a distinct,
			// unrelated occurrence at another slot, and the wrong slot here
			// would resolve the wrong occurrence.
			require.Equal(t, 1, leiosCerts.gotEbSlotCalls)
			require.Equal(t, uint64(9), leiosCerts.gotEbSlot)
		})
	}
}

func TestCheckAndForgeProductionCertifiesOnlyParentAnnouncedLeiosEB(
	t *testing.T,
) {
	creds := setupTestCredentials(t)
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	wrongHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x22}, 32))
	parentHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x33}, 32))
	parentRbHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32))
	wrongCert := &lcommon.LeiosEbCertificate{
		SlotNo:              8,
		EndorserBlockHash:   wrongHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	parentCert := &lcommon.LeiosEbCertificate{
		SlotNo:              9,
		EndorserBlockHash:   parentHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	wrongContextCert := &lcommon.LeiosEbCertificate{
		SlotNo:              8,
		EndorserBlockHash:   parentHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	leiosCerts := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				SlotNo:            8,
				EndorserBlockHash: wrongHash,
				Certificate:       wrongCert,
				AnnouncingRbHash:  parentRbHash,
			},
			{
				SlotNo:            8,
				EndorserBlockHash: parentHash,
				Certificate:       wrongContextCert,
				AnnouncingRbHash: lcommon.NewBlake2b256(
					bytes.Repeat([]byte{0x55}, 32),
				),
			},
			{
				SlotNo:            9,
				EndorserBlockHash: parentHash,
				Certificate:       parentCert,
				AnnouncingRbHash:  parentRbHash,
			},
		},
	}
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: parentRbHash, hash: parentHash, ok: true,
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.leiosCalls)
	require.Nil(t, builder.leiosData.Announcement)
	require.Same(t, parentCert, builder.leiosData.Certificate)
	require.Equal(t, []lcommon.Blake2b256{parentHash}, leiosCerts.marked)
	require.Equal(t, []uint64{9}, leiosCerts.markedSlots)
	// Resolve, then re-check before the build. See leiosParentAnnouncement.
	require.Equal(t, 2, parent.calls)
}

// TestCheckAndForgeProductionDropsLeiosDataWhenTheParentMoves covers the gap
// between resolving parent-dependent Leios data and building the block that
// carries it.
//
// The certificate is selected for the endorser block the parent ranking block
// announced. The builder inherits none of that: it binds the block's parent
// from its own fresh chain-tip read. If the tip advances while the Leios work
// runs -- endorser-block production and mempool rebasing are not instant --
// the block ends up built on a new parent while carrying a certificate from
// the old parent's endorser-block lineage. No peer accepts that block, and
// this node has spent the slot's credentials signing it.
//
// The parent is therefore re-read immediately before the build. When it has
// moved, the Leios data is dropped and a plain ranking block is forged, and
// the embedded-endorser-block bookkeeping is dropped with it: leaving it set
// would record an endorser block as embedded in a block that does not carry
// it.
func TestCheckAndForgeProductionDropsLeiosDataWhenTheParentMoves(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	parentHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x33}, 32))
	parentRbHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32))
	// The tip the builder will actually bind, different from the one the
	// certificate was selected for.
	movedRbHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x66}, 32))
	parentCert := &lcommon.LeiosEbCertificate{
		SlotNo:              9,
		EndorserBlockHash:   parentHash,
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	leiosCerts := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				SlotNo:            9,
				EndorserBlockHash: parentHash,
				Certificate:       parentCert,
				AnnouncingRbHash:  parentRbHash,
			},
		},
	}
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash:           parentRbHash,
		hash:             parentHash,
		ok:               true,
		rbHashAfterFirst: &movedRbHash,
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    &forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		ForgeFence:       &fenceTestStore{},
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	// The parent was resolved once and re-checked once.
	require.Equal(t, 2, parent.calls)
	// A block was still forged -- dropping the Leios data costs the
	// certificate, not the slot.
	require.Equal(t, 1, builder.calls, "a plain ranking block is still built")
	require.Zero(
		t,
		builder.leiosCalls,
		"the Leios build path must not be taken with data resolved for the abandoned parent",
	)
	require.Nil(
		t,
		builder.leiosData.Certificate,
		"a certificate selected for the abandoned parent must not be carried",
	)
	require.Nil(t, builder.leiosData.Announcement)
	// The embedded-endorser-block bookkeeping went with it.
	require.Empty(
		t,
		leiosCerts.marked,
		"no endorser block may be recorded as embedded in a block that omits it",
	)
}

// forgerTestValidator is a controllable BlockValidator stub. Setting
// panic makes ValidateForgedBlock panic instead of returning err, to
// exercise forger.go's callback panic recovery.
type forgerTestValidator struct {
	err   error
	panic bool
	calls int
}

func (v *forgerTestValidator) ValidateForgedBlock(context.Context, ledger.Block, []byte) error {
	v.calls++
	if v.panic {
		panic("validator panic")
	}
	return v.err
}

// newForgerWithValidator returns the forger and its slot clock. The clock
// is mutable so a test that runs more than one forge cycle can advance the
// slot: the duplicate-slot fence refuses a slot the forger already used,
// as the real slot-aligned loop never revisits one.
func newForgerWithValidator(
	t *testing.T,
	block ledger.Block,
	blockCbor []byte,
	broadcaster *forgerTestBroadcaster,
	validator *forgerTestValidator,
) (*BlockForger, *forgerTestSlotClock) {
	t.Helper()
	creds := setupTestCredentials(t)
	clock := &forgerTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
	}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: blockCbor},
		BlockBroadcaster: broadcaster,
		BlockValidator:   validator,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, clock
}

// TestBlockValidatorPassesAllowsAdoption verifies that a passing validator
// does not prevent adoption: broadcaster is called and forgeAdopted increments.
func TestBlockValidatorPassesAllowsAdoption(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	validator := &forgerTestValidator{err: nil}

	forger, _ := newForgerWithValidator(t, block, nil, broadcaster, validator)
	err := forger.checkAndForgeProduction(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, validator.calls, "validator must be called once")
	assert.Equal(
		t,
		1,
		broadcaster.calls,
		"block must be adopted after passing validation",
	)
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

// TestBlockValidatorFailureDropsBlock verifies that a failing validator
// prevents adoption: broadcaster is NOT called, couldNotForge increments.
func TestBlockValidatorFailureDropsBlock(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	validator := &forgerTestValidator{
		err: errors.New("header crypto: invalid KES signature"),
	}

	forger, _ := newForgerWithValidator(t, block, nil, broadcaster, validator)
	err := forger.checkAndForgeProduction(context.Background())

	require.Error(t, err)
	require.ErrorContains(t, err, "self-validation failed")
	assert.Equal(t, 1, validator.calls, "validator must be called once")
	assert.Equal(
		t,
		0,
		broadcaster.calls,
		"block must NOT be adopted after failed validation",
	)
	assert.Equal(t, float64(0), testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeValidationFailed),
	)
}

// TestNilBlockValidatorSkipsValidation confirms that the default nil validator
// does not affect normal forging: block is adopted without any validation call.
func TestNilBlockValidatorSkipsValidation(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}

	creds := setupTestCredentials(t)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block},
		BlockBroadcaster: broadcaster,
		// BlockValidator intentionally omitted (nil = disabled)
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.NoError(t, err)
	assert.Equal(
		t,
		1,
		broadcaster.calls,
		"block must be adopted when validator is nil",
	)
}

// TestBlockValidatorCalledBeforeBroadcaster verifies ordering: the validator
// runs before AddBlock. If the validator fails, AddBlock must never be called.
func TestBlockValidatorCalledBeforeBroadcaster(t *testing.T) {
	block := newForgerTestBlock(10, 2)

	var callOrder []string
	broadcaster := &forgerTestBroadcaster{}
	// Override the underlying AddBlock via a tracking broadcaster
	trackingBroadcaster := &trackingBroadcaster{
		inner: broadcaster,
		onAdd: func() { callOrder = append(callOrder, "broadcast") },
	}
	trackingValidator := &trackingBlockValidator{
		onValidate: func() error {
			callOrder = append(callOrder, "validate")
			return errors.New("intentional failure")
		},
	}

	creds := setupTestCredentials(t)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block},
		BlockBroadcaster: trackingBroadcaster,
		BlockValidator:   trackingValidator,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	_ = forger.checkAndForgeProduction(context.Background())

	require.Equal(
		t,
		[]string{"validate"},
		callOrder,
		"validator must run before broadcaster; broadcaster must not run on failure",
	)
}

// trackingBroadcaster wraps a broadcaster and calls a hook on AddBlock.
type trackingBroadcaster struct {
	inner BlockBroadcaster
	onAdd func()
}

func (b *trackingBroadcaster) AddBlock(ctx context.Context, block ledger.Block, cbor []byte) error {
	b.onAdd()
	return b.inner.AddBlock(context.Background(), block, cbor)
}

// trackingBlockValidator calls a hook on ValidateForgedBlock.
type trackingBlockValidator struct {
	onValidate func() error
}

func (v *trackingBlockValidator) ValidateForgedBlock(context.Context,
	ledger.Block,
	[]byte,
) error {
	return v.onValidate()
}

// A forge already in progress when the node context is cancelled must not
// adopt or announce its block: cancellation is how a halted ledger stops the
// node, and a block built on that ledger must not enter the local chain.
func TestForgeDoesNotAdoptBlockWhenContextCancelledMidForge(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t, 200, 199, 199, &logs,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	builder.onBuild = cancel
	forged := 0
	forger.blockForged = func(ledger.Block, []byte, time.Duration) {
		forged++
	}

	err := forger.checkAndForgeProduction(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, builder.calls, "the forge must have reached the build")
	require.Zero(t, broadcaster.calls, "a block was adopted after cancellation")
	require.Zero(t, forged, "a block was announced after cancellation")
}
