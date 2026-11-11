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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Reload triggers, the trigger label of dingo_forge_credential_reloads_total.
const (
	reloadTriggerSignal = "signal"
	reloadTriggerFile   = "file"
	reloadTriggerAgent  = "agent"
)

// retiredKESAgentClientGrace is how long a KES agent client replaced by a
// reload stays open. A forge attempt that selected its credentials before the
// swap keeps the outgoing signer, which stays valid for the slot it was
// selected for (see forging.PoolCredentials.ReplaceWith), so the client is
// closed only once any such attempt has finished signing.
const retiredKESAgentClientGrace = 10 * time.Second

// agentOpCertRetryInterval bounds how often the same certificate offered by
// the agent is tried again after a reload with it failed. The client offers
// it at every admission, which can be many times a minute.
const agentOpCertRetryInterval = time.Minute

// errCredentialsUnchanged reports a reload that found the material already
// loaded; the reload succeeds without touching the live credentials.
var errCredentialsUnchanged = errors.New("block producer credentials unchanged")

// keyReloadState carries the in-place credential reload's own state: the
// agent's offers of a newer operational certificate, the background loops and
// the reload metrics.
type keyReloadState struct {
	offersOnce sync.Once
	offers     chan []byte

	mu          sync.Mutex
	loopsCancel context.CancelFunc
	lastOffer   [sha256.Size]byte
	lastOfferAt time.Time

	metricsOnce sync.Once
	reloads     *prometheus.CounterVec

	// signClientFactory, when set, builds the sign client a reload installs;
	// tests use it to supply a fake agent.
	signClientFactory func(*bursa.LoadedKey) (kesAgentSignClient, error)
}

// reloadCredentials is the one entry point every reload trigger uses: a
// SIGHUP, a credential file change, or the KES agent presenting a newer
// operational certificate (agentOpCert non-nil). It holds the lifecycle gates
// for the whole reload and records the outcome.
func (n *Node) reloadCredentials(trigger string, agentOpCert []byte) error {
	release, err := n.tryLifecycleGates()
	if err != nil {
		n.recordCredentialReload(trigger, err)
		return err
	}
	defer release()
	slot, supported, err := n.blockProducerStartupClock()
	if err == nil {
		if agentOpCert != nil {
			err = n.reloadAgentSignCredentials(
				slot,
				supported,
				n.validateBlockProducerLedger,
				agentOpCert,
			)
		} else {
			err = n.reloadBlockProducerCredentials(
				slot,
				supported,
				n.validateBlockProducerLedger,
			)
		}
	}
	n.recordCredentialReload(trigger, err)
	if errors.Is(err, errCredentialsUnchanged) {
		n.config.logger.Info(
			"block producer credentials unchanged; nothing to reload",
			"component", "node",
			"trigger", trigger,
		)
		return nil
	}
	return err
}

// reloadAgentSignCredentials replaces the live sign-mode credentials with the
// VRF key and an operational certificate -- the configured file, or
// agentOpCert when the agent presented a newer one -- signed through a new
// agent client that expects that certificate's KES key. Everything is checked
// before the swap: the certificate's cold-key signature and KES period, the
// ledger cross-check including the opcert counter rule, a counter above the
// loaded one, and that the agent admits a session for this certificate. A
// refusal leaves the live credentials and client in place.
//
// The VRF key cannot change here: a new VRF identity needs a new pool
// registration and a restart. The VRF key file is compared with the loaded
// identity first, so a changed VRF key is refused even when the certificate
// is unchanged, and before the agent is contacted.
func (n *Node) reloadAgentSignCredentials(
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
	agentOpCert []byte,
) error {
	live := n.blockProducerCreds.Load()
	if live == nil {
		return errors.New("block producer credentials are not loaded")
	}
	if !supported {
		return errors.New(
			"credential reload requires a wall-clock slot supported by confirmed era history",
		)
	}
	if err := checkReloadVRFKey(n.config.shelleyVRFKey, live); err != nil {
		return err
	}
	opCertKey, err := n.reloadOpCertKey(agentOpCert)
	if err != nil {
		return err
	}
	opCert := opCertFromLoadedKey(opCertKey)
	liveOpCert := live.GetOpCert()
	if sameOpCert(opCert, liveOpCert) {
		return errCredentialsUnchanged
	}
	if liveOpCert != nil && opCert.IssueNumber <= liveOpCert.IssueNumber {
		return fmt.Errorf(
			"operational certificate counter %d is not above the loaded counter %d; "+
				"a changed certificate needs a higher counter",
			opCert.IssueNumber,
			liveOpCert.IssueNumber,
		)
	}
	client, err := n.kesAgentSignClientFor(opCertKey)
	if err != nil {
		return fmt.Errorf("build KES agent client: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			_ = client.Close()
		}
	}()
	next := forging.NewPoolCredentials()
	// ReplaceWith empties next on success, so this only wipes a replacement
	// that was refused.
	defer next.Close()
	if err := next.LoadFromAgentSignOpCert(
		n.config.shelleyVRFKey,
		opCert,
		client,
	); err != nil {
		return fmt.Errorf("load reloaded credentials: %w", err)
	}
	if err := next.ValidateOpCert(); err != nil {
		return fmt.Errorf("validate operational certificate: %w", err)
	}
	genesis, err := n.blockProducerShelleyGenesis()
	if err != nil {
		return err
	}
	if err := next.ValidateKESPeriod(genesis, slot); err != nil {
		return fmt.Errorf("validate KES period: %w", err)
	}
	if err := ledgerCheck(next); err != nil {
		return fmt.Errorf("reloaded credentials failed ledger check: %w", err)
	}
	if err := checkKESAgentSignClient(client); err != nil {
		return fmt.Errorf(
			"KES agent does not serve the new certificate: %w",
			err,
		)
	}
	oldCounter, oldKESPeriod := opCertCounterAndPeriod(liveOpCert)
	if err := live.ReplaceWith(next); err != nil {
		return fmt.Errorf("install reloaded credentials: %w", err)
	}
	installed = true
	n.retireKESAgentClient(n.kesAgentClient)
	n.kesAgentClient = client
	newCounter, newKESPeriod := opCertCounterAndPeriod(live.GetOpCert())
	n.config.logger.Info(
		"block producer credentials reloaded",
		"component", "node",
		"pool_id", live.GetPoolID().String(),
		"source", opCertSource(agentOpCert),
		"old_opcert_counter", oldCounter,
		"new_opcert_counter", newCounter,
		"old_opcert_kes_period", oldKESPeriod,
		"new_opcert_kes_period", newKESPeriod,
		"opcert_expiry_period", live.OpCertExpiryPeriod(),
	)
	return nil
}

// checkReloadVRFKey refuses a VRF key file that no longer matches the loaded
// VRF identity. A sign-mode reload otherwise reads the VRF key only once the
// certificate has changed, so a VRF change alone would pass as unchanged.
func checkReloadVRFKey(path string, live *forging.PoolCredentials) error {
	vkey, err := forging.LoadVRFVerificationKeyFile(path)
	if err != nil {
		return fmt.Errorf("load VRF key: %w", err)
	}
	if !bytes.Equal(vkey, live.GetVRFVKey()) {
		return errors.New(
			"VRF key file no longer matches the loaded VRF key; " +
				"a VRF key change needs a new pool registration and a restart",
		)
	}
	return nil
}

// reloadOpCertKey reads the operational certificate a sign-mode reload
// installs: the agent's, when it offered one, or the configured file.
func (n *Node) reloadOpCertKey(agentOpCert []byte) (*bursa.LoadedKey, error) {
	if agentOpCert == nil {
		key, err := forging.LoadOperationalCertificateFile(
			n.config.shelleyOperationalCertificate,
		)
		if err != nil {
			return nil, fmt.Errorf("load operational certificate: %w", err)
		}
		return key, nil
	}
	dec, err := bursa.DecodeOpCert(agentOpCert)
	if err != nil {
		return nil, fmt.Errorf(
			"decode the KES agent's operational certificate: %w",
			err,
		)
	}
	return &bursa.LoadedKey{
		VKey:              dec.KESVKey,
		RawCBOR:           append([]byte(nil), agentOpCert...),
		OpCertIssueNumber: dec.IssueNumber,
		OpCertKesPeriod:   dec.KESPeriod,
		OpCertSignature:   dec.ColdSig,
		OpCertColdVKey:    dec.ColdVKey,
	}, nil
}

func opCertSource(agentOpCert []byte) string {
	if agentOpCert != nil {
		return "kes agent"
	}
	return "file"
}

func opCertFromLoadedKey(key *bursa.LoadedKey) *forging.OpCert {
	return &forging.OpCert{
		KESVKey:     append([]byte(nil), key.VKey...),
		IssueNumber: key.OpCertIssueNumber,
		KESPeriod:   key.OpCertKesPeriod,
		Signature:   append([]byte(nil), key.OpCertSignature...),
		ColdVKey:    append([]byte(nil), key.OpCertColdVKey...),
	}
}

func sameOpCert(a, b *forging.OpCert) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.IssueNumber == b.IssueNumber &&
		a.KESPeriod == b.KESPeriod &&
		bytes.Equal(a.KESVKey, b.KESVKey) &&
		bytes.Equal(a.Signature, b.Signature) &&
		bytes.Equal(a.ColdVKey, b.ColdVKey)
}

// kesAgentSignClientFor builds the sign client a reload installs. Tests
// replace it through keyReloadState.signClientFactory.
func (n *Node) kesAgentSignClientFor(
	opCertKey *bursa.LoadedKey,
) (kesAgentSignClient, error) {
	if n.keyReload.signClientFactory != nil {
		return n.keyReload.signClientFactory(opCertKey)
	}
	return n.newKESAgentSignClient(opCertKey)
}

// checkKESAgentSignClient confirms the agent behind a new sign client serves
// it before the client goes live. A remote client is admitted, which checks
// the agent reports this certificate and its KES key. The local socket
// protocol carries neither, so a local client is only checked to be reachable
// in sign mode; its signatures are still verified against the certificate's
// KES key on every sign.
func checkKESAgentSignClient(client kesAgentSignClient) error {
	if p, ok := client.(forging.RemoteKESPreparer); ok {
		return p.Prepare()
	}
	if r, ok := client.(interface{ CheckReady() error }); ok {
		return r.CheckReady()
	}
	return nil
}

// retireKESAgentClient closes a client a reload replaced, once a forge attempt
// that may still hold it has had time to finish.
func (n *Node) retireKESAgentClient(old io.Closer) {
	if old == nil {
		return
	}
	time.AfterFunc(retiredKESAgentClientGrace, func() {
		if err := old.Close(); err != nil {
			n.config.logger.Warn(
				"failed to close retired KES agent client",
				"error", err,
			)
		}
	})
}

// offerAgentOpCert is the remote client's OnNewerOpCert hook. It hands the
// certificate to the agent reload loop without blocking the admission, keeping
// only the newest offer.
func (n *Node) offerAgentOpCert(opCert []byte) {
	ch := n.agentOpCertOffers()
	for {
		select {
		case ch <- opCert:
			return
		default:
		}
		select {
		case <-ch:
		default:
		}
	}
}

func (n *Node) agentOpCertOffers() chan []byte {
	n.keyReload.offersOnce.Do(func() {
		n.keyReload.offers = make(chan []byte, 1)
	})
	return n.keyReload.offers
}

// startKeyReloadLoops starts the background reload triggers for the running
// block producer: the agent's newer-certificate offers, and the credential
// file watch when shelleyKeyWatchInterval is set. A second call replaces the
// loops of the first, so a live-lifecycle rebuild does not run two.
func (n *Node) startKeyReloadLoops() {
	ctx, cancel := context.WithCancel(n.blockProducerContext())
	n.keyReload.mu.Lock()
	if n.keyReload.loopsCancel != nil {
		n.keyReload.loopsCancel()
	}
	n.keyReload.loopsCancel = cancel
	n.keyReload.mu.Unlock()
	n.credentialReloadMetric()
	go n.agentOpCertLoop(ctx, func(opCert []byte) error {
		return n.reloadCredentials(reloadTriggerAgent, opCert)
	})
	if interval := n.config.shelleyKeyWatchInterval; interval > 0 {
		if n.kesAgentConfigured() && !n.kesAgentSignMode() {
			// A serve-key reload is always refused, so every file change
			// would only log and count a failure.
			n.config.logger.Warn(
				"shelleyKeyWatchInterval is ignored in KES agent serve-key mode, where the agent pushes key rotations",
				"component", "node",
			)
			return
		}
		go n.credentialFileWatchLoop(
			ctx,
			interval,
			n.watchedCredentialFiles(),
			func() error {
				return n.reloadCredentials(reloadTriggerFile, nil)
			},
		)
	}
}

// stopKeyReloadLoops stops the loops startKeyReloadLoops started.
func (n *Node) stopKeyReloadLoops() {
	n.keyReload.mu.Lock()
	defer n.keyReload.mu.Unlock()
	if n.keyReload.loopsCancel != nil {
		n.keyReload.loopsCancel()
		n.keyReload.loopsCancel = nil
	}
}

// agentOpCertLoop runs reload for each certificate the agent offers, skipping
// one tried within agentOpCertRetryInterval.
func (n *Node) agentOpCertLoop(
	ctx context.Context,
	reload func(opCert []byte) error,
) {
	offers := n.agentOpCertOffers()
	for {
		select {
		case <-ctx.Done():
			return
		case opCert := <-offers:
			if !n.shouldTryAgentOpCert(opCert, time.Now()) {
				continue
			}
			n.config.logger.Info(
				"KES agent presented a newer operational certificate; reloading",
				"component", "node",
			)
			if err := reload(opCert); err != nil {
				n.config.logger.Error(
					"reload with the KES agent's operational certificate failed; keeping the loaded credentials",
					"component", "node",
					"error", err,
				)
			}
		}
	}
}

// shouldTryAgentOpCert reports whether an offered certificate is new, or the
// same one last tried more than agentOpCertRetryInterval ago.
func (n *Node) shouldTryAgentOpCert(opCert []byte, now time.Time) bool {
	sum := sha256.Sum256(opCert)
	n.keyReload.mu.Lock()
	defer n.keyReload.mu.Unlock()
	if sum == n.keyReload.lastOffer &&
		now.Sub(n.keyReload.lastOfferAt) < agentOpCertRetryInterval {
		return false
	}
	n.keyReload.lastOffer = sum
	n.keyReload.lastOfferAt = now
	return true
}

// watchedCredentialFiles lists the files a reload reads in the configured
// mode: the VRF key and operational certificate, and the KES key when it is a
// local file.
func (n *Node) watchedCredentialFiles() []string {
	files := []string{
		n.config.shelleyVRFKey,
		n.config.shelleyOperationalCertificate,
	}
	if !n.kesAgentConfigured() {
		files = append(files, n.config.shelleyKESKey)
	}
	return files
}

// credentialFileWatchLoop polls the credential files every interval and
// reloads once a changed content has held for two polls in a row. Polling
// the content rather than watching for events sees every way a file is
// replaced, including the atomic symlink swap Kubernetes uses to update a
// mounted Secret, and needs no file notification support. Waiting a poll
// keeps a file still being written by hand from being read half done; a
// reload validates everything anyway and refuses a broken set. A failed
// reload stays pending and is retried once per interval until it succeeds or
// the files change.
func (n *Node) credentialFileWatchLoop(
	ctx context.Context,
	interval time.Duration,
	files []string,
	reload func() error,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	loaded := credentialFilesDigest(files)
	var pending [sha256.Size]byte
	havePending := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		current := credentialFilesDigest(files)
		if current == loaded {
			havePending = false
			continue
		}
		if !havePending || current != pending {
			pending = current
			havePending = true
			continue
		}
		n.config.logger.Info(
			"block producer credential files changed; reloading",
			"component", "node",
		)
		if err := reload(); err != nil {
			// Keep the change pending so the next poll retries it.
			n.config.logger.Error(
				"block producer credential reload failed; keeping the loaded credentials and retrying at the next poll",
				"component", "node",
				"error", err,
			)
			continue
		}
		havePending = false
		loaded = current
	}
}

// credentialFilesDigest hashes the files' contents in order. A file that
// cannot be read contributes its error text, so a file going missing is a
// change too, and the reload that follows reports it.
func credentialFilesDigest(files []string) [sha256.Size]byte {
	h := sha256.New()
	for _, f := range files {
		data, err := os.ReadFile(f) //nolint:gosec // configured key paths
		if err != nil {
			fmt.Fprintf(h, "%s\x00error:%v\x00", f, err)
			continue
		}
		sum := sha256.Sum256(data)
		fmt.Fprintf(h, "%s\x00", f)
		_, _ = h.Write(sum[:])
		wipeBytes(data)
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func wipeBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// recordCredentialReload counts a reload by trigger and result.
func (n *Node) recordCredentialReload(trigger string, err error) {
	reloads := n.credentialReloadMetric()
	if reloads == nil {
		return
	}
	result := "success"
	switch {
	case errors.Is(err, errCredentialsUnchanged):
		result = "unchanged"
	case err != nil:
		result = "failure"
	}
	reloads.WithLabelValues(trigger, result).Inc()
}

func (n *Node) credentialReloadMetric() *prometheus.CounterVec {
	if n.config.promRegistry == nil {
		return nil
	}
	n.keyReload.metricsOnce.Do(func() {
		factory := promauto.With(n.config.promRegistry)
		n.keyReload.reloads = factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_forge_credential_reloads_total",
				Help: "block producer credential reloads by trigger (signal, file, agent) and result (success, unchanged, failure)",
			},
			[]string{"trigger", "result"},
		)
		// Every series exists from the start, so rate() reads zero rather
		// than an empty vector before the first reload.
		for _, trigger := range []string{
			reloadTriggerSignal, reloadTriggerFile, reloadTriggerAgent,
		} {
			for _, result := range []string{"success", "unchanged", "failure"} {
				n.keyReload.reloads.WithLabelValues(trigger, result)
			}
		}
	})
	return n.keyReload.reloads
}

// Compile-time check that both sign clients can be prepared or probed.
var (
	_ forging.RemoteKESPreparer       = (*kesagent.RemoteClient)(nil)
	_ interface{ CheckReady() error } = (*kesagent.Client)(nil)
)
