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
	"slices"
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
// first pool's agent offers of a newer operational certificate, the
// background loops and the reload metrics. Every other pool keeps its offers
// in its poolProducer.
type keyReloadState struct {
	agentOffers agentOfferState

	mu          sync.Mutex
	loopsCancel context.CancelFunc

	metricsOnce sync.Once
	reloads     *prometheus.CounterVec

	// signClientFactory, when set, builds the sign client a reload installs;
	// tests use it to supply a fake agent.
	signClientFactory func(*bursa.LoadedKey) (kesAgentSignClient, error)
}

// agentOfferState is one pool's offers of a newer operational certificate
// from its KES agent, and the certificate last tried, so a refused one is not
// retried more than once per agentOpCertRetryInterval.
type agentOfferState struct {
	once   sync.Once
	offers chan []byte

	mu          sync.Mutex
	lastOffer   [sha256.Size]byte
	lastOfferAt time.Time
}

// offer is a remote client's OnNewerOpCert hook. It hands the certificate to
// the pool's agent reload loop without blocking the admission, keeping only
// the newest offer.
func (a *agentOfferState) offer(opCert []byte) {
	ch := a.channel()
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

func (a *agentOfferState) channel() chan []byte {
	a.once.Do(func() {
		a.offers = make(chan []byte, 1)
	})
	return a.offers
}

// shouldTry reports whether an offered certificate is new, or the same one
// last tried more than agentOpCertRetryInterval ago.
func (a *agentOfferState) shouldTry(opCert []byte, now time.Time) bool {
	sum := sha256.Sum256(opCert)
	a.mu.Lock()
	defer a.mu.Unlock()
	if sum == a.lastOffer &&
		now.Sub(a.lastOfferAt) < agentOpCertRetryInterval {
		return false
	}
	a.lastOffer = sum
	a.lastOfferAt = now
	return true
}

// reloadCredentials is the one entry point every reload trigger uses: a
// SIGHUP, a credential file change, or the first pool's KES agent presenting
// a newer operational certificate (agentOpCert non-nil).
func (n *Node) reloadCredentials(trigger string, agentOpCert []byte) error {
	return n.reloadCredentialsWith(
		trigger,
		func(slot uint64, supported bool) error {
			if agentOpCert == nil {
				return n.reloadEveryPool(
					slot,
					supported,
					n.validateBlockProducerLedger,
				)
			}
			err := n.reloadAgentSignCredentials(
				slot,
				supported,
				n.validateBlockProducerLedger,
				agentOpCert,
			)
			n.countAgentReloadFailure(n.blockProducerCreds.Load(), err)
			return err
		},
	)
}

// reloadPoolProducerAgentOpCert reloads one pool after the first with the
// newer operational certificate its own KES agent presented. No other pool is
// touched.
func (n *Node) reloadPoolProducerAgentOpCert(
	pp *poolProducer,
	agentOpCert []byte,
) error {
	return n.reloadCredentialsWith(
		reloadTriggerAgent,
		func(slot uint64, supported bool) error {
			n.multiPool.mu.Lock()
			live := pp.creds
			n.multiPool.mu.Unlock()
			if live == nil {
				return fmt.Errorf("pool %s is not forging", pp.poolID)
			}
			err := n.reloadPoolAgentSignCredentials(
				pp.keySource(n),
				live,
				slot,
				supported,
				n.validateBlockProducerLedger,
				agentOpCert,
			)
			n.countAgentReloadFailure(live, err)
			if err != nil {
				err = fmt.Errorf("pool %s: %w", pp.poolID, err)
			}
			return err
		},
	)
}

// countAgentReloadFailure counts a refused agent-offered reload against its
// pool when the node forges for several.
func (n *Node) countAgentReloadFailure(
	live *forging.PoolCredentials,
	err error,
) {
	if err == nil || errors.Is(err, errCredentialsUnchanged) ||
		live == nil || n.producerPoolCount() <= 1 {
		return
	}
	n.countReloadFailure(live.GetPoolID().String())
}

// reloadCredentialsWith holds the lifecycle gates for the whole of reload,
// passes it the reload clock and records the outcome under trigger.
func (n *Node) reloadCredentialsWith(
	trigger string,
	reload func(slot uint64, supported bool) error,
) error {
	release, err := n.tryLifecycleGates()
	if err != nil {
		n.recordCredentialReload(trigger, err)
		return err
	}
	defer release()
	slot, supported, err := n.blockProducerStartupClock()
	if err == nil {
		err = reload(slot, supported)
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

// reloadEveryPool reloads the first pool and, when the node forges for
// several, every other running pool. Each pool reloads on its own: one whose
// replacement is refused keeps forging with what it had and does not hold
// back the others.
func (n *Node) reloadEveryPool(
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
) error {
	primaryErr := n.reloadBlockProducerCredentials(slot, supported, ledgerCheck)
	if n.producerPoolCount() <= 1 {
		return primaryErr
	}
	if live := n.blockProducerCreds.Load(); primaryErr != nil && live != nil {
		primaryErr = fmt.Errorf(
			"pool %s: %w",
			live.GetPoolID().String(),
			primaryErr,
		)
	}
	return joinPoolReloads(append(
		[]error{primaryErr},
		n.reloadPoolProducerCredentials(slot, supported, ledgerCheck)...,
	))
}

// joinPoolReloads combines one reload result per pool. A pool whose
// credentials were already loaded is not a failure: the result is the other
// pools' failures, errCredentialsUnchanged when every pool was unchanged, and
// nil otherwise.
func joinPoolReloads(results []error) error {
	var errs []error
	unchanged := 0
	for _, err := range results {
		switch {
		case errors.Is(err, errCredentialsUnchanged):
			unchanged++
		case err != nil:
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if unchanged == len(results) {
		return errCredentialsUnchanged
	}
	return nil
}

// reloadAgentSignCredentials is reloadPoolAgentSignCredentials for the first
// pool.
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
	return n.reloadPoolAgentSignCredentials(
		n.primaryKeySource(),
		live,
		slot,
		supported,
		ledgerCheck,
		agentOpCert,
	)
}

// reloadPoolAgentSignCredentials replaces one pool's live sign-mode
// credentials with the VRF key and an operational certificate -- the configured file, or
// agentOpCert when the agent presented a newer one -- signed through a new
// agent client that expects that certificate's KES key. Everything is checked
// before the swap: the certificate's cold-key signature and KES period, the
// ledger cross-check including the opcert counter rule, a counter above the
// loaded one, and that the agent admits a session for this certificate. A
// refusal leaves the live credentials and client in place.
//
// The VRF key cannot change here: ReplaceWith refuses a new pool or VRF
// identity, which needs a new pool registration and a restart.
func (n *Node) reloadPoolAgentSignCredentials(
	ks *poolKeySource,
	live *forging.PoolCredentials,
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
	agentOpCert []byte,
) error {
	if !supported {
		return errors.New(
			"credential reload requires a wall-clock slot supported by confirmed era history",
		)
	}
	opCertKey, err := ks.reloadOpCertKey(agentOpCert)
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
	client, err := n.kesAgentSignClientFor(ks, opCertKey)
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
		ks.keys.vrfKey,
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
	n.retireKESAgentClient(*ks.client)
	*ks.client = client
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

// reloadOpCertKey reads the operational certificate a sign-mode reload
// installs: the agent's, when it offered one, or the configured file.
func (ks *poolKeySource) reloadOpCertKey(
	agentOpCert []byte,
) (*bursa.LoadedKey, error) {
	if agentOpCert == nil {
		key, err := forging.LoadOperationalCertificateFile(ks.keys.opCert)
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
	ks *poolKeySource,
	opCertKey *bursa.LoadedKey,
) (kesAgentSignClient, error) {
	if n.keyReload.signClientFactory != nil {
		return n.keyReload.signClientFactory(opCertKey)
	}
	return ks.newKESAgentSignClient(opCertKey)
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

// offerAgentOpCert is the first pool's OnNewerOpCert hook.
func (n *Node) offerAgentOpCert(opCert []byte) {
	n.keyReload.agentOffers.offer(opCert)
}

func (n *Node) agentOpCertOffers() chan []byte {
	return n.keyReload.agentOffers.channel()
}

// startKeyReloadLoops starts the background reload triggers for the running
// block producer: each pool's agent newer-certificate offers, and the credential
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
	n.multiPool.mu.Lock()
	producers := slices.Clone(n.multiPool.producers)
	n.multiPool.mu.Unlock()
	for _, pp := range producers {
		go n.agentOfferLoop(
			ctx,
			pp.agentOffers.channel(),
			pp.agentOffers.shouldTry,
			func(opCert []byte) error {
				return n.reloadPoolProducerAgentOpCert(pp, opCert)
			},
			"pool_id", pp.poolID,
		)
	}
	if interval := n.config.shelleyKeyWatchInterval; interval > 0 {
		if ks := n.primaryKeySource(); ks.kesAgentConfigured() &&
			!ks.kesAgentSignMode() {
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

// agentOpCertLoop runs reload for each certificate the first pool's agent
// offers.
func (n *Node) agentOpCertLoop(
	ctx context.Context,
	reload func(opCert []byte) error,
) {
	n.agentOfferLoop(
		ctx,
		n.agentOpCertOffers(),
		n.shouldTryAgentOpCert,
		reload,
	)
}

// agentOfferLoop runs reload for each certificate one pool's agent offers on
// offers, skipping one shouldTry rejects. logAttrs name the pool in its log
// lines.
func (n *Node) agentOfferLoop(
	ctx context.Context,
	offers <-chan []byte,
	shouldTry func(opCert []byte, now time.Time) bool,
	reload func(opCert []byte) error,
	logAttrs ...any,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case opCert := <-offers:
			if !shouldTry(opCert, time.Now()) {
				continue
			}
			n.config.logger.Info(
				"KES agent presented a newer operational certificate; reloading",
				append([]any{"component", "node"}, logAttrs...)...,
			)
			if err := reload(opCert); err != nil {
				n.config.logger.Error(
					"reload with the KES agent's operational certificate failed; keeping the loaded credentials",
					append(
						[]any{"component", "node", "error", err},
						logAttrs...,
					)...,
				)
			}
		}
	}
}

// shouldTryAgentOpCert is agentOfferState.shouldTry for the first pool.
func (n *Node) shouldTryAgentOpCert(opCert []byte, now time.Time) bool {
	return n.keyReload.agentOffers.shouldTry(opCert, now)
}

// watchedCredentialFiles lists the files a reload reads in the configured
// mode: the VRF key and operational certificate, and the KES key when it is a
// local file.
func (n *Node) watchedCredentialFiles() []string {
	ks := n.primaryKeySource()
	files := []string{ks.keys.vrfKey, ks.keys.opCert}
	if !ks.kesAgentConfigured() {
		files = append(files, ks.keys.kesKey)
	}
	return files
}

// credentialFileWatchLoop polls the credential files every interval and
// reloads once a changed content has held for two polls in a row. Polling
// the content rather than watching for events sees every way a file is
// replaced, including the atomic symlink swap Kubernetes uses to update a
// mounted Secret, and needs no file notification support. Waiting a poll
// keeps a file still being written by hand from being read half done; a
// reload validates everything anyway and refuses a broken set.
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
		havePending = false
		loaded = current
		n.config.logger.Info(
			"block producer credential files changed; reloading",
			"component", "node",
		)
		if err := reload(); err != nil {
			n.config.logger.Error(
				"block producer credential reload failed; keeping the loaded credentials",
				"component", "node",
				"error", err,
			)
		}
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
