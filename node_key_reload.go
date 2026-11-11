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
// a newer operational certificate (agentOpCert non-nil). A file change skips
// serve-key pools, which cannot reload; a SIGHUP reports their refusal.
//
// Each sign-mode pool's KES agent is asked to admit that pool's new
// certificate before the lifecycle gates are taken. That check can wait on
// the network for up to the client's hello timeout, and the chainsync and
// chain-switch handlers drop their work while liveLifecycleMu is held (see
// checkSettledComponents). The check only involves the agent and a client
// nothing else uses yet, so it needs no gate; everything it was checked
// against is read again under the gates before the swap.
func (n *Node) reloadCredentials(trigger string, agentOpCert []byte) error {
	if agentOpCert != nil {
		return n.reloadAgentOpCert(n.primaryReloadTarget(), agentOpCert)
	}
	preps := n.prepareSignReloads()
	defer preps.close()
	return n.reloadCredentialsWith(
		trigger,
		func(slot uint64, supported bool) error {
			return n.installEveryPool(
				slot,
				supported,
				n.validateBlockProducerLedger,
				trigger == reloadTriggerFile,
				preps,
			)
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
	return n.reloadAgentOpCert(n.poolProducerReloadTarget(pp), agentOpCert)
}

// reloadTarget is the one pool an agent-offered reload replaces the
// credentials of: its key source, which also names the slot its KES agent
// client lives in, and its live credentials.
type reloadTarget struct {
	ks *poolKeySource
	// poolID names the pool in errors; it is empty for the first pool,
	// whose errors are reported as they are.
	poolID string
	// live returns the pool's live credentials, or why the pool cannot
	// reload now.
	live func() (*forging.PoolCredentials, error)
}

func (n *Node) primaryReloadTarget() reloadTarget {
	return reloadTarget{
		ks: n.primaryKeySource(),
		live: func() (*forging.PoolCredentials, error) {
			live := n.blockProducerCreds.Load()
			if live == nil {
				return nil, errors.New(
					"block producer credentials are not loaded",
				)
			}
			return live, nil
		},
	}
}

// poolProducerReloadTarget is pp's reloadTarget. pp must still be one of the
// node's producers and forging: a live rebuild replaces the producers, and a
// client installed into a replaced one would never be used or closed.
func (n *Node) poolProducerReloadTarget(pp *poolProducer) reloadTarget {
	return reloadTarget{
		ks:     pp.keySource(n),
		poolID: pp.poolID,
		live: func() (*forging.PoolCredentials, error) {
			n.multiPool.mu.Lock()
			defer n.multiPool.mu.Unlock()
			if pp.creds == nil || !slices.Contains(n.multiPool.producers, pp) {
				return nil, errors.New("the pool is not forging")
			}
			return pp.creds, nil
		},
	}
}

func (t reloadTarget) wrap(err error) error {
	if err == nil || t.poolID == "" {
		return err
	}
	return fmt.Errorf("pool %s: %w", t.poolID, err)
}

// reloadAgentOpCert reloads t with the operational certificate its KES agent
// offered: the agent admits it before the lifecycle gates are taken, and it
// is installed under them. A refusal is counted against the pool when the
// node forges for several.
func (n *Node) reloadAgentOpCert(t reloadTarget, agentOpCert []byte) error {
	live, err := t.live()
	if err != nil {
		return n.finishCredentialReload(reloadTriggerAgent, t.wrap(err))
	}
	prep := n.prepareSignReload(t.ks, live, agentOpCert)
	defer prep.close()
	if prep.err != nil {
		n.countAgentReloadFailure(live, prep.err)
		return n.finishCredentialReload(reloadTriggerAgent, t.wrap(prep.err))
	}
	return n.reloadCredentialsWith(
		reloadTriggerAgent,
		func(slot uint64, supported bool) error {
			live, err := t.live()
			if err != nil {
				return t.wrap(err)
			}
			err = n.installSignReload(
				t.ks,
				live,
				slot,
				supported,
				n.validateBlockProducerLedger,
				prep,
			)
			n.countAgentReloadFailure(live, err)
			return t.wrap(err)
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

// reloadCredentialsWith holds the lifecycle gates while reload validates and
// installs the new credentials, passes it the reload clock and records the
// outcome under trigger.
func (n *Node) reloadCredentialsWith(
	trigger string,
	reload func(slot uint64, supported bool) error,
) error {
	release, err := n.tryLifecycleGates()
	if err != nil {
		return n.finishCredentialReload(trigger, err)
	}
	defer release()
	slot, supported, err := n.blockProducerStartupClock()
	if err == nil {
		err = reload(slot, supported)
	}
	return n.finishCredentialReload(trigger, err)
}

// finishCredentialReload records a reload's outcome and reports an unchanged
// one as success.
func (n *Node) finishCredentialReload(trigger string, err error) error {
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
// several, every other running pool, preparing the sign-mode pools' reloads
// and installing them in one go. reloadCredentials runs the two halves itself
// so the agents are checked outside the lifecycle gates.
func (n *Node) reloadEveryPool(
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
	skipServeKey bool,
) error {
	preps := n.prepareSignReloads()
	defer preps.close()
	return n.installEveryPool(slot, supported, ledgerCheck, skipServeKey, preps)
}

// installEveryPool reloads the first pool and, when the node forges for
// several, every other running pool, installing the sign-mode reloads preps
// holds. The caller holds the lifecycle gates. Each pool reloads on its own:
// one whose replacement is refused keeps forging with what it had and does
// not hold back the others. With skipServeKey a serve-key pool is not
// reloaded and reports itself unchanged instead of refused.
func (n *Node) installEveryPool(
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
	skipServeKey bool,
	preps *signReloadPreps,
) error {
	primaryErr := errCredentialsUnchanged
	if !skipServeKey || !n.primaryKeySource().servesKeys() {
		primaryErr = n.reloadPrimaryCredentials(
			slot,
			supported,
			ledgerCheck,
			preps.primary,
		)
	}
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
		n.reloadPoolProducerCredentials(
			slot,
			supported,
			ledgerCheck,
			skipServeKey,
			preps,
		)...,
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

// signReloadPrep is one sign-mode pool's reload as prepared before the
// lifecycle gates: the reload its agent admitted, or why there is none.
type signReloadPrep struct {
	reload *agentSignReload
	err    error
}

// close closes the prepared client unless it was installed.
func (p signReloadPrep) close() {
	if p.reload != nil {
		p.reload.close()
	}
}

// signReloadPreps is a reload's sign-mode pools prepared before the lifecycle
// gates: the first pool's, and each running pool's after it, by producer. A
// pool that is not in sign mode has none.
type signReloadPreps struct {
	primary signReloadPrep
	pools   map[*poolProducer]signReloadPrep
}

// prepareSignReloads prepares the reload of every sign-mode pool that is
// forging. Each pool is prepared on its own: one whose agent refuses does not
// keep the others from preparing.
func (n *Node) prepareSignReloads() *signReloadPreps {
	preps := &signReloadPreps{}
	if ks := n.primaryKeySource(); ks.signsThroughAgent() {
		if live := n.blockProducerCreds.Load(); live != nil {
			preps.primary = n.prepareSignReload(ks, live, nil)
		}
	}
	for _, t := range n.runningPoolProducers() {
		ks := t.pp.keySource(n)
		if !ks.signsThroughAgent() {
			continue
		}
		if preps.pools == nil {
			preps.pools = make(map[*poolProducer]signReloadPrep)
		}
		preps.pools[t.pp] = n.prepareSignReload(ks, t.live, nil)
	}
	return preps
}

// close closes every prepared client that was not installed.
func (p *signReloadPreps) close() {
	p.primary.close()
	for _, prep := range p.pools {
		prep.close()
	}
}

// pool returns pp's prepared reload; the zero value when pp had none.
func (p *signReloadPreps) pool(pp *poolProducer) signReloadPrep {
	return p.pools[pp]
}

// agentSignReload is a sign-mode reload whose certificate the agent has
// admitted, waiting to be installed under the lifecycle gates.
type agentSignReload struct {
	agentOpCert []byte
	opCertKey   *bursa.LoadedKey
	opCert      *forging.OpCert
	client      kesAgentSignClient
	installed   bool
}

// close closes the prepared client unless it was installed.
func (r *agentSignReload) close() {
	if !r.installed {
		_ = r.client.Close()
	}
}

// reloadAgentSignCredentials replaces the first pool's live sign-mode
// credentials with the VRF key and an operational certificate -- the
// configured file, or agentOpCert when the agent presented a newer one --
// signed through a new agent client that expects that certificate's KES key.
// It is prepareSignReload followed by installSignReload; the reload triggers
// run the two halves themselves so the agent check happens outside the gates.
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
	ks := n.primaryKeySource()
	prep := n.prepareSignReload(ks, live, agentOpCert)
	defer prep.close()
	return n.installSignReload(ks, live, slot, supported, ledgerCheck, prep)
}

// prepareSignReload is prepareAgentSignReload as a signReloadPrep.
func (n *Node) prepareSignReload(
	ks *poolKeySource,
	live *forging.PoolCredentials,
	agentOpCert []byte,
) signReloadPrep {
	r, err := n.prepareAgentSignReload(ks, live, agentOpCert)
	return signReloadPrep{reload: r, err: err}
}

// installSignReload installs a prepared reload into ks's pool, or reports
// why it could not be prepared. A sign-mode pool with nothing prepared
// started forging after the reload was prepared, and is left to the next one.
func (n *Node) installSignReload(
	ks *poolKeySource,
	live *forging.PoolCredentials,
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
	prep signReloadPrep,
) error {
	if prep.err != nil {
		return prep.err
	}
	if prep.reload == nil {
		return errors.New(
			"the pool started forging while the reload was prepared; reload again",
		)
	}
	return n.installAgentSignReload(
		ks,
		live,
		slot,
		supported,
		ledgerCheck,
		prep.reload,
	)
}

// prepareAgentSignReload reads the certificate a sign-mode reload of ks's
// pool installs, refuses it early when it cannot be installed, and has the
// pool's agent admit a session for it through a new client. It takes no
// lifecycle gate: it reads only the configuration, the key files and the
// live credentials, which are safe to read concurrently.
//
// The VRF key cannot change here: a new VRF identity needs a new pool
// registration and a restart. The VRF key file is compared with the loaded
// identity first, so a changed VRF key is refused even when the certificate
// is unchanged, and before the agent is contacted.
func (n *Node) prepareAgentSignReload(
	ks *poolKeySource,
	live *forging.PoolCredentials,
	agentOpCert []byte,
) (*agentSignReload, error) {
	if err := checkReloadVRFKey(ks.keys.vrfKey, live); err != nil {
		return nil, err
	}
	opCertKey, err := ks.reloadOpCertKey(agentOpCert)
	if err != nil {
		return nil, err
	}
	opCert := opCertFromLoadedKey(opCertKey)
	if err := checkReloadOpCertCounter(opCert, live.GetOpCert()); err != nil {
		return nil, err
	}
	client, err := n.kesAgentSignClientFor(ks, opCertKey)
	if err != nil {
		return nil, fmt.Errorf("build KES agent client: %w", err)
	}
	if err := checkKESAgentSignClient(client); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf(
			"KES agent does not serve the new certificate: %w",
			err,
		)
	}
	return &agentSignReload{
		agentOpCert: agentOpCert,
		opCertKey:   opCertKey,
		opCert:      opCert,
		client:      client,
	}, nil
}

// installAgentSignReload validates a prepared reload of ks's pool and swaps
// it into live and the pool's client slot. The caller holds the lifecycle
// gates. Everything is checked before the swap: that the certificate file
// still holds the certificate the agent admitted, the certificate's cold-key
// signature and KES period, the ledger cross-check including the opcert
// counter rule, and a counter above the loaded one. A refusal leaves the live
// credentials and client in place.
func (n *Node) installAgentSignReload(
	ks *poolKeySource,
	live *forging.PoolCredentials,
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
	r *agentSignReload,
) error {
	if !supported {
		return errors.New(
			"credential reload requires a wall-clock slot supported by confirmed era history",
		)
	}
	if r.agentOpCert == nil {
		current, err := ks.reloadOpCertKey(nil)
		if err != nil {
			return err
		}
		if !bytes.Equal(current.RawCBOR, r.opCertKey.RawCBOR) {
			return errors.New(
				"operational certificate file changed during the reload; reload again",
			)
		}
	}
	liveOpCert := live.GetOpCert()
	if err := checkReloadOpCertCounter(r.opCert, liveOpCert); err != nil {
		return err
	}
	next := forging.NewPoolCredentials()
	// ReplaceWith empties next on success, so this only wipes a replacement
	// that was refused.
	defer next.Close()
	if err := next.LoadFromAgentSignOpCert(
		ks.keys.vrfKey,
		r.opCert,
		r.client,
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
	oldCounter, oldKESPeriod := opCertCounterAndPeriod(liveOpCert)
	if err := live.ReplaceWith(next); err != nil {
		return fmt.Errorf("install reloaded credentials: %w", err)
	}
	r.installed = true
	n.retireKESAgentClient(*ks.client)
	*ks.client = r.client
	newCounter, newKESPeriod := opCertCounterAndPeriod(live.GetOpCert())
	n.config.logger.Info(
		"block producer credentials reloaded",
		"component", "node",
		"pool_id", live.GetPoolID().String(),
		"source", opCertSource(r.agentOpCert),
		"old_opcert_counter", oldCounter,
		"new_opcert_counter", newCounter,
		"old_opcert_kes_period", oldKESPeriod,
		"new_opcert_kes_period", newKESPeriod,
		"opcert_expiry_period", live.OpCertExpiryPeriod(),
	)
	return nil
}

// checkReloadOpCertCounter reports an unchanged certificate, and refuses a
// changed one whose counter is not above the loaded one.
func checkReloadOpCertCounter(opCert, liveOpCert *forging.OpCert) error {
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
		if n.everyPoolServesKeys() {
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
// mode, for every pool that can reload: the VRF key and operational
// certificate, and the KES key when it is a local file. A serve-key pool's
// files are left out, since its reload is always refused. A change to any of
// them reloads every pool but the serve-key ones; a pool whose files did not
// change reports itself unchanged.
func (n *Node) watchedCredentialFiles() []string {
	var files []string
	for _, ks := range n.configuredPoolKeySources() {
		if ks.servesKeys() {
			continue
		}
		files = append(files, ks.keys.vrfKey, ks.keys.opCert)
		if !ks.kesAgentConfigured() {
			files = append(files, ks.keys.kesKey)
		}
	}
	return files
}

// everyPoolServesKeys reports whether every pool's KES key comes from an
// agent in serve-key mode, where a reload is always refused.
func (n *Node) everyPoolServesKeys() bool {
	for _, ks := range n.configuredPoolKeySources() {
		if !ks.servesKeys() {
			return false
		}
	}
	return true
}

// servesKeys reports whether the pool's KES key comes from an agent in
// serve-key mode, where a reload is always refused.
func (ks *poolKeySource) servesKeys() bool {
	return ks.kesAgentConfigured() && !ks.kesAgentSignMode()
}

// signsThroughAgent reports whether the pool's KES agent signs in sign mode,
// where a reload checks the new certificate with the agent first.
func (ks *poolKeySource) signsThroughAgent() bool {
	return ks.kesAgentConfigured() && ks.kesAgentSignMode()
}

// configuredPoolKeySources is a key source per configured pool, the first
// pool's first, for reading their settings only.
func (n *Node) configuredPoolKeySources() []*poolKeySource {
	pools := n.config.blockProducerPools
	if len(pools) == 0 {
		return []*poolKeySource{n.primaryKeySource()}
	}
	sources := make([]*poolKeySource, 0, len(pools))
	for _, p := range pools {
		sources = append(sources, &poolKeySource{
			n:    n,
			keys: producerKeysFromPool(p),
		})
	}
	return sources
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
