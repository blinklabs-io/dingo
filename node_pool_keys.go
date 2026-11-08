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

package dingo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/blinklabs-io/dingo/ledger/forging"
)

// producerKeys is where one pool's block-producer credentials come from: its
// VRF key and operational certificate files, and one KES source -- a local key
// file, a local KES agent socket, or a remote KES agent.
type producerKeys struct {
	vrfKey                string
	kesKey                string
	opCert                string
	agentSocket           string
	agentMode             string
	agentAddress          string
	agentClientCertFile   string
	agentClientKeyFile    string
	agentServerSPKISHA256 string
}

// poolKeySource loads and validates one pool's credentials. client and
// cancel point at where that pool keeps its KES agent client and serve-key
// loop: the node's own fields for the first pool, a poolProducer's for the
// others.
type poolKeySource struct {
	n      *Node
	keys   producerKeys
	client *io.Closer
	cancel *context.CancelFunc
}

// primaryKeySource is the first pool's key source: the top-level key
// settings and the node's own KES agent fields.
func (n *Node) primaryKeySource() *poolKeySource {
	return &poolKeySource{
		n: n,
		keys: producerKeys{
			vrfKey:                n.config.shelleyVRFKey,
			kesKey:                n.config.shelleyKESKey,
			opCert:                n.config.shelleyOperationalCertificate,
			agentSocket:           n.config.shelleyKESAgentSocket,
			agentMode:             n.config.shelleyKESAgentMode,
			agentAddress:          n.config.shelleyKESAgentAddress,
			agentClientCertFile:   n.config.shelleyKESAgentClientCertFile,
			agentClientKeyFile:    n.config.shelleyKESAgentClientKeyFile,
			agentServerSPKISHA256: n.config.shelleyKESAgentServerSPKISHA256,
		},
		client: &n.kesAgentClient,
		cancel: &n.kesAgentCancel,
	}
}

func (n *Node) validateBlockProducerStartupAtSlot(
	currentSlot uint64,
) (*forging.PoolCredentials, error) {
	return n.primaryKeySource().validateBlockProducerStartupAtSlot(currentSlot)
}

func (n *Node) validateBlockProducerStartupForClock(
	slot uint64,
	supported bool,
) (*forging.PoolCredentials, error) {
	return n.primaryKeySource().validateBlockProducerStartupForClock(
		slot,
		supported,
	)
}

func (n *Node) loadBlockProducerCredentialsFromAgent(
	creds *forging.PoolCredentials,
	currentSlot uint64,
) error {
	return n.primaryKeySource().loadBlockProducerCredentialsFromAgent(
		creds,
		currentSlot,
	)
}

func (n *Node) kesAgentConfigured() bool {
	return n.primaryKeySource().kesAgentConfigured()
}

func (n *Node) closeKESAgentClient() {
	n.primaryKeySource().closeKESAgentClient()
}

func (ks *poolKeySource) validateBlockProducerStartupAtSlot(
	currentSlot uint64,
) (creds *forging.PoolCredentials, retErr error) {
	agentBacked := ks.kesAgentConfigured()
	if agentBacked {
		// Agent startup installs the client before the remaining credential
		// validation below. Do not leave that client and its serve-key loop
		// running when a later validation step rejects the credentials.
		defer func() {
			if retErr != nil {
				ks.closeKESAgentClient()
			}
		}()
	}
	creds, err := ks.validateBlockProducerCredentialMaterial(currentSlot)
	if err != nil {
		return nil, err
	}
	genesis, err := ks.n.blockProducerShelleyGenesis()
	if err != nil {
		return nil, err
	}
	if err := creds.ValidateKESPeriod(genesis, currentSlot); err != nil {
		return nil, fmt.Errorf("validate KES period: %w", err)
	}
	currentPeriod, err := forging.CurrentKESPeriodFromGenesis(
		genesis,
		currentSlot,
	)
	if err != nil {
		return nil, fmt.Errorf("compute current KES period: %w", err)
	}
	opCert := creds.GetOpCert()
	if opCert == nil {
		return nil, errors.New("block producer operational certificate is nil")
	}
	ks.n.config.logger.Info(
		"block producer credentials validated",
		"component", "node",
		"pool_id", creds.GetPoolID().String(),
		"current_slot", currentSlot,
		"current_kes_period", currentPeriod,
		"opcert_kes_period", opCert.KESPeriod,
		"opcert_counter", opCert.IssueNumber,
		"opcert_expiry_period", creds.OpCertExpiryPeriod(),
	)
	return creds, nil
}

// validateBlockProducerStartupForClock decides how to judge the operational
// certificate given a wall-clock slot and whether the confirmed era history
// actually supports it.
//
// When supported, this is the strict historical preflight: a certificate
// staged for the future or already expired fails startup here. When not
// supported, the KES-period plausibility check is deferred rather than
// skipped — credential material is still validated and the protocol lifetime
// is still armed, so the forger's per-slot gate has the data it needs to
// reject the certificate before Praos leader selection.
//
// Split out from validateBlockProducerStartup so both branches are reachable
// without a live LedgerState, the same reason
// validateBlockProducerStartupAtSlot and validateBlockProducerLedgerWithView
// exist separately.
func (ks *poolKeySource) validateBlockProducerStartupForClock(
	slot uint64,
	supported bool,
) (creds *forging.PoolCredentials, retErr error) {
	if supported {
		return ks.validateBlockProducerStartupAtSlot(slot)
	}
	if ks.kesAgentConfigured() {
		defer func() {
			if retErr != nil {
				ks.closeKESAgentClient()
			}
		}()
	}
	creds, err := ks.validateBlockProducerCredentialMaterial(slot)
	if err != nil {
		return nil, err
	}
	genesis, err := ks.n.blockProducerShelleyGenesis()
	if err != nil {
		return nil, err
	}
	if err := creds.ArmKesProtocolLifetime(genesis); err != nil {
		return nil, fmt.Errorf("arm KES protocol lifetime: %w", err)
	}
	opCert := creds.GetOpCert()
	if opCert == nil {
		return nil, errors.New("block producer operational certificate is nil")
	}
	var tipSlot uint64
	if ks.n.ledgerState != nil {
		tipSlot = ks.n.ledgerState.ChainTipSlot()
	}
	ks.n.config.logger.Warn(
		"block producer startup: confirmed era history does not span the current wall-clock slot yet; "+
			"deferring the operational-certificate KES-period plausibility check until the ledger catches up. "+
			"The opcert KES lifetime is re-checked per slot and forging cannot proceed while it fails; "+
			"sync progress additionally gates forging whenever an upstream peer is active.",
		"component",
		"node",
		"tip_slot",
		tipSlot,
		"opcert_kes_period",
		opCert.KESPeriod,
		"opcert_expiry_period",
		creds.OpCertExpiryPeriod(),
	)
	return creds, nil
}

// validateBlockProducerCredentialMaterial loads the pool credentials and
// validates the operational certificate's structure and cold-key signature,
// without judging it against any slot. The slot-dependent KES-period
// plausibility check lives in validateBlockProducerStartupAtSlot; callers
// needing the certificate armed but not yet placed in time combine this with
// PoolCredentials.ArmKesProtocolLifetime.
func (ks *poolKeySource) validateBlockProducerCredentialMaterial(
	currentSlot uint64,
) (*forging.PoolCredentials, error) {
	creds := forging.NewPoolCredentials()
	if ks.kesAgentConfigured() {
		if err := ks.loadBlockProducerCredentialsFromAgent(creds,
			currentSlot,
		); err != nil {
			return nil, fmt.Errorf(
				"load pool credentials from KES agent: %w",
				err,
			)
		}
	} else if err := creds.LoadFromFiles(
		ks.keys.vrfKey,
		ks.keys.kesKey,
		ks.keys.opCert,
	); err != nil {
		return nil, fmt.Errorf("load pool credentials: %w", err)
	}
	if err := creds.ValidateOpCert(); err != nil {
		return nil, fmt.Errorf("validate operational certificate: %w", err)
	}
	return creds, nil
}

// loadBlockProducerCredentialsFromAgent dials the configured KES agent and
// installs its material into creds, in either serve-key or sign mode. VRF and
// the operational certificate are always still read from local files; the
// KES agent protocol carries only the KES signing key (and, in serve-key
// mode, the opcert alongside it).
//
// It closes and replaces any previously dialed agent client first: this
// method also runs on a live-lifecycle rebuild
// (reinitializeBlockProducer -> validateBlockProducerStartup), and must not
// leak the prior connection or its background goroutine.
func (ks *poolKeySource) loadBlockProducerCredentialsFromAgent(
	creds *forging.PoolCredentials,
	currentSlot uint64,
) error {
	ks.closeKESAgentClient()

	mode := ks.keys.agentMode
	if ks.keys.agentAddress != "" {
		// A remote agent serves only sign mode; config validation rejects
		// any other mode with an address.
		mode = kesagent.ModeSign
	}
	if mode == "" {
		mode = kesagent.ModeServeKey
	}

	switch mode {
	case kesagent.ModeServeKey:
		return ks.startKESAgentServeKey(creds, currentSlot)
	case kesagent.ModeSign:
		return ks.startKESAgentSign(creds)
	default:
		return fmt.Errorf("unknown KES agent mode %q", mode)
	}
}

// startKESAgentServeKey blocks for the agent's initial key push, installs it,
// and starts a background loop that installs every subsequent push (a KES
// key rotation) for as long as the node runs.
func (ks *poolKeySource) startKESAgentServeKey(
	creds *forging.PoolCredentials,
	startupSlot uint64,
) error {
	opCertKey, err := forging.LoadOperationalCertificateFile(
		ks.keys.opCert,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to load operational certificate for KES agent serve-key mode: %w",
			err,
		)
	}
	client, err := kesagent.NewClient(kesagent.Config{
		SocketPath: ks.keys.agentSocket,
		Mode:       kesagent.ModeServeKey,
		Logger:     ks.n.config.logger,
		Metrics:    ks.n.kesAgentClientMetrics(),
	})
	if err != nil {
		return err
	}

	install := func(pk kesagent.PushedKey) error {
		if pk.OpCert == nil || !bytes.Equal(
			pk.OpCert.ColdVKey,
			opCertKey.OpCertColdVKey,
		) {
			return errors.New(
				"KES agent operational certificate cold key does not match local operational certificate",
			)
		}
		return creds.LoadFromAgentServeKey(
			ks.keys.vrfKey,
			agentMaterialFromPushedKey(pk),
		)
	}
	// Installing a push clears the validated KES protocol lifetime, exactly
	// as LoadFromFiles does, so no credential can inherit a policy that was
	// never checked against the material now installed. The initial push is
	// followed by validateBlockProducerStartupAtSlot's own ValidateOpCert /
	// ValidateKESPeriod, which re-establishes it; a push arriving later --
	// a KES evolution, an opcert rotation, or a re-push after a reconnect --
	// is not, so the loop must re-establish it itself. Without this,
	// credentialGeneration.kesSign refuses every signature after the first
	// mid-run push with "operational certificate is not validated" and the
	// node stops forging until it is restarted.
	//
	// Installed and validated in one call rather than two: as two, the
	// credentials are published with their lifetime cleared for the duration
	// of the validation, and a leader slot landing in that window is refused
	// for exactly the reason this callback exists to prevent. Every rotation
	// crosses it.
	loopInstall := func(pk kesagent.PushedKey) error {
		if pk.OpCert == nil || !bytes.Equal(
			pk.OpCert.ColdVKey,
			opCertKey.OpCertColdVKey,
		) {
			return errors.New(
				"KES agent operational certificate cold key does not match local operational certificate",
			)
		}
		genesis, err := ks.n.blockProducerShelleyGenesis()
		if err != nil {
			return err
		}
		return creds.LoadFromAgentServeKeyValidated(
			ks.keys.vrfKey,
			agentMaterialFromPushedKey(pk),
			genesis,
			ks.n.agentInstallSlot(startupSlot),
		)
	}

	// The initial push must succeed before startup can proceed -- the same
	// contract LoadFromFiles has (a load failure fails block-producer
	// startup outright) -- bounded so a dead agent fails startup rather than
	// hanging it.
	//nolint:contextcheck // ks.n.ctx is the node's lifecycle context; startup itself is bounded here
	ctx, cancel := context.WithTimeout(
		ks.n.blockProducerContext(),
		kesagent.DefaultHelloTimeout*2,
	)
	defer cancel()
	pk, err := client.AwaitPushedKey(ctx)
	if err != nil {
		_ = client.Close()
		return fmt.Errorf("await initial KES agent key push: %w", err)
	}
	// ledger/forging copies the key bytes it keeps, so this copy is dead as
	// soon as the install returns; Client.Run does the same for every later
	// push.
	installErr := install(pk)
	pk.Wipe()
	if installErr != nil {
		_ = client.Close()
		return installErr
	}

	(*ks.client) = client
	//nolint:contextcheck // background loop is bound to the node's lifecycle, not this call
	runCtx, runCancel := context.WithCancel(ks.n.blockProducerContext())
	(*ks.cancel) = runCancel
	go func() {
		if err := client.Run(runCtx, loopInstall); err != nil &&
			!errors.Is(err, context.Canceled) {
			ks.n.config.logger.Error(
				"kes agent serve-key loop exited",
				"error", err,
			)
		}
	}()
	return nil
}

// startKESAgentSign installs sign-mode credentials: VRF and the operational
// certificate from local files, KES signing delegated to the agent for the
// lifetime of the client. There is no background loop in this mode -- the
// agent evolves its own key internally on every Sign call.
func (ks *poolKeySource) startKESAgentSign(creds *forging.PoolCredentials) error {
	opCertKey, err := forging.LoadOperationalCertificateFile(
		ks.keys.opCert,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to load operational certificate for KES agent sign mode: %w",
			err,
		)
	}
	client, err := ks.newKESAgentSignClient(opCertKey)
	if err != nil {
		return err
	}
	if err := creds.LoadFromAgentSign(
		ks.keys.vrfKey,
		ks.keys.opCert,
		client,
	); err != nil {
		_ = client.Close()
		return err
	}
	(*ks.client) = client
	return nil
}

// newKESAgentSignClient dials nothing yet: it builds the remote client when
// shelleyKESAgentAddress is set, otherwise the local socket client.
func (ks *poolKeySource) newKESAgentSignClient(
	opCertKey *bursa.LoadedKey,
) (kesAgentSignClient, error) {
	if ks.keys.agentAddress == "" {
		return kesagent.NewClient(kesagent.Config{
			SocketPath:        ks.keys.agentSocket,
			Mode:              kesagent.ModeSign,
			SignTimeout:       ks.n.config.shelleyKESAgentSignTimeout,
			KESVKey:           opCertKey.VKey,
			OpCertStartPeriod: opCertKey.OpCertKesPeriod,
			Logger:            ks.n.config.logger,
			Metrics:           ks.n.kesAgentClientMetrics(),
		})
	}
	return kesagent.NewRemoteClient(kesagent.RemoteConfig{
		Address:           ks.keys.agentAddress,
		ClientCertFile:    ks.keys.agentClientCertFile,
		ClientKeyFile:     ks.keys.agentClientKeyFile,
		ServerSPKISHA256:  ks.keys.agentServerSPKISHA256,
		KESVKey:           opCertKey.VKey,
		OpCertStartPeriod: opCertKey.OpCertKesPeriod,
		OpCert:            opCertKey.RawCBOR,
		SignTimeout:       ks.n.config.shelleyKESAgentSignTimeout,
		Logger:            ks.n.config.logger,
		Metrics:           ks.n.kesAgentClientMetrics(),
	})
}

// kesAgentConfigured reports whether KES signing goes through an agent,
// local or remote, rather than a local key file.
func (ks *poolKeySource) kesAgentConfigured() bool {
	return ks.keys.agentSocket != "" ||
		ks.keys.agentAddress != ""
}

// closeKESAgentClient stops the serve-key background loop (if any) and
// closes the agent connection. Safe to call when neither exists.
func (ks *poolKeySource) closeKESAgentClient() {
	if (*ks.cancel) != nil {
		(*ks.cancel)()
		(*ks.cancel) = nil
	}
	if (*ks.client) != nil {
		if err := (*ks.client).Close(); err != nil {
			ks.n.config.logger.Warn(
				"failed to close KES agent client",
				"error", err,
			)
		}
		(*ks.client) = nil
	}
}
