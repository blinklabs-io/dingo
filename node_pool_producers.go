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
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/blinklabs-io/dingo/ledger/forging"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
)

// poolProducerRetryInterval is how often a pool whose KES source was
// unreachable at startup is tried again.
const poolProducerRetryInterval = 30 * time.Second

// multiPoolState is what forging for several pools adds to a node. The first
// pool keeps the node's own forging fields (blockForger, leaderElection,
// blockProducerCreds, kesAgentClient); every other pool is a poolProducer.
type multiPoolState struct {
	// mu guards producers' running state and the retry loop, which the
	// forging path, readiness probes and the retry goroutine all read.
	mu        sync.Mutex
	producers []*poolProducer
	arbiter   *poolSlotArbiter
	own       *ownForgedBlocks
	// retryCancel and retryDone stop the loop retrying skipped pools.
	retryCancel context.CancelFunc
	retryDone   chan struct{}

	reloadFailures     *prometheus.CounterVec
	reloadFailuresOnce sync.Once
}

// poolProducer forges for one pool after the first: its own credentials, KES
// agent client, leader election and block forger.
type poolProducer struct {
	index  int
	keys   producerKeys
	poolID string

	kesAgentClient  io.Closer
	kesAgentCancel  context.CancelFunc
	kesAgentMetrics *kesagent.Metrics
	// kesAgentMetricsMu guards kesAgentMetrics' lazy creation: a sign-mode
	// reload builds its client outside the lifecycle gates.
	kesAgentMetricsMu sync.Mutex
	// agentOffers are this pool's own agent offers of a newer operational
	// certificate; they reload this pool only.
	agentOffers agentOfferState

	creds    *forging.PoolCredentials
	election interface{ Stop() error }
	forger   *forging.BlockForger
	// startErr is why the pool is not forging, while it is skipped.
	startErr error
}

// poolIDFromOpCertFile reads the pool ID a pool's operational certificate
// names: the hash of its cold verification key. The file is local in every
// KES mode, so the ID is known before any agent is reached.
func poolIDFromOpCertFile(path string) (string, error) {
	opCert, err := forging.LoadOperationalCertificateFile(path)
	if err != nil {
		return "", err
	}
	if len(opCert.OpCertColdVKey) == 0 {
		return "", fmt.Errorf(
			"operational certificate %q has no cold verification key",
			path,
		)
	}
	return lcommon.PoolId(
		lcommon.Blake2b224Hash(opCert.OpCertColdVKey),
	).String(), nil
}

// poolMetricsRegistry is where one pool's forging, leader-election and KES
// agent client metrics register: the node's registry for a single pool, and
// otherwise that registry with a pool_id label, so the pools' series stay
// apart instead of colliding on one name.
func (n *Node) poolMetricsRegistry(poolID string) prometheus.Registerer {
	reg := n.config.promRegistry
	if reg == nil || n.producerPoolCount() <= 1 {
		return reg
	}
	return prometheus.WrapRegistererWith(
		prometheus.Labels{"pool_id": poolID},
		reg,
	)
}

// slotArbiterFor returns the arbiter the forger of poolID consults, or nil
// when the node forges for one pool.
func (n *Node) slotArbiterFor(poolID string) forging.SlotArbiter {
	if n.multiPool.arbiter == nil {
		return nil
	}
	return n.multiPool.arbiter.forPool(poolID)
}

// addArbiterMember records a pool whose forger is running with the arbiter.
func (n *Node) addArbiterMember(
	poolID string,
	creds *forging.PoolCredentials,
	built poolForging,
) {
	if n.multiPool.arbiter == nil {
		return
	}
	n.multiPool.arbiter.add(arbiterMember{
		poolID: poolID,
		creds:  creds,
		leader: built.election,
		praos:  built.praos,
	})
}

// initMultiPool prepares the state shared by several pools, once per node:
// the arbiter, the schedule compute gate and one poolProducer per pool after
// the first. A single-pool node gets none of it.
func (n *Node) initMultiPool() error {
	pools := n.config.blockProducerPools
	if len(pools) <= 1 {
		return nil
	}
	n.multiPool.mu.Lock()
	defer n.multiPool.mu.Unlock()
	if n.multiPool.arbiter != nil {
		return nil
	}
	producers := make([]*poolProducer, 0, len(pools)-1)
	// Two entries for one pool would forge it twice, each with its own
	// forge fence, so every slot it leads would get two blocks.
	seen := make(map[string]int, len(pools))
	for i, p := range pools {
		poolID, err := poolIDFromOpCertFile(p.ShelleyOperationalCertificate)
		if err != nil {
			return fmt.Errorf(
				"blockProducerPools[%d]: read pool ID: %w",
				i,
				err,
			)
		}
		if first, dup := seen[poolID]; dup {
			return fmt.Errorf(
				"blockProducerPools[%d] and blockProducerPools[%d] are both pool %s",
				first,
				i,
				poolID,
			)
		}
		seen[poolID] = i
		if i == 0 {
			continue
		}
		producers = append(producers, &poolProducer{
			index:  i,
			keys:   producerKeysFromPool(p),
			poolID: poolID,
		})
	}
	// One zero series per pool from startup, so rate() over the reload
	// failure counter is defined before the first failure.
	if counter := n.reloadFailureCounter(); counter != nil {
		for poolID := range seen {
			counter.WithLabelValues(poolID)
		}
	}
	n.multiPool.producers = producers
	n.multiPool.arbiter = newPoolSlotArbiter(n.config.logger)
	n.multiPool.own = &ownForgedBlocks{}
	// Each schedule computation proves every slot of an epoch on one core;
	// half the cores keeps an epoch boundary from starving the rest of the
	// node while a few dozen pools catch up.
	n.scheduleComputeGate = make(chan struct{}, max(1, runtime.NumCPU()/2))
	return nil
}

// prepareMultiPool readies the shared multi-pool state for a (re)start of
// the block producer: the pools must be refused where only one is supported,
// and the arbiter must read the ledger the forgers are about to use.
func (n *Node) prepareMultiPool() error {
	if n.producerPoolCount() > 1 &&
		(n.config.leiosForgingEnabled() ||
			n.config.leiosVoteSigningKeyFile != "") {
		return errors.New(
			"blockProducerPools lists more than one pool, which Leios forging and voting do not support",
		)
	}
	if err := n.initMultiPool(); err != nil {
		return err
	}
	if n.multiPool.arbiter != nil {
		n.multiPool.arbiter.setView(ledgerArbiterView{ls: n.ledgerState})
	}
	return nil
}

// keySource is this producer's poolKeySource.
func (pp *poolProducer) keySource(n *Node) *poolKeySource {
	return &poolKeySource{
		n:             n,
		keys:          pp.keys,
		client:        &pp.kesAgentClient,
		cancel:        &pp.kesAgentCancel,
		onNewerOpCert: pp.agentOffers.offer,
		metrics: func() *kesagent.Metrics {
			pp.kesAgentMetricsMu.Lock()
			defer pp.kesAgentMetricsMu.Unlock()
			if pp.kesAgentMetrics == nil {
				if reg := n.poolMetricsRegistry(pp.poolID); reg != nil {
					pp.kesAgentMetrics = kesagent.NewMetrics(reg)
				}
			}
			return pp.kesAgentMetrics
		},
	}
}

// startPoolProducers starts every pool after the first. A pool whose KES
// source cannot be reached is skipped and retried in the background; any
// other failure -- keys that do not validate, a ledger mismatch -- stops
// startup, as it does for the first pool.
func (n *Node) startPoolProducers(ctx context.Context) error {
	if n.multiPool.arbiter == nil {
		return nil
	}
	skipped := false
	for _, pp := range n.multiPool.producers {
		err := n.startPoolProducer(ctx, pp)
		if err == nil {
			continue
		}
		if !errors.Is(err, errKESSourceUnavailable) {
			// Leave none of the pools started so far behind a failed start.
			_ = n.stopPoolProducers()
			return fmt.Errorf("pool %s: %w", pp.poolID, err)
		}
		skipped = true
		n.config.logger.Warn(
			"block producer pool skipped: its KES source is unreachable; retrying in the background",
			"component",
			"node",
			"pool_id",
			pp.poolID,
			"retry_interval",
			poolProducerRetryInterval,
			"error",
			err,
		)
	}
	if skipped {
		n.startPoolProducerRetry(ctx)
	}
	return nil
}

// startPoolProducer loads, checks and starts one pool after the first. On
// failure nothing of it is left running and its startErr says why.
func (n *Node) startPoolProducer(
	ctx context.Context,
	pp *poolProducer,
) (retErr error) {
	ks := pp.keySource(n)
	defer func() {
		n.multiPool.mu.Lock()
		pp.startErr = retErr
		n.multiPool.mu.Unlock()
	}()
	slot, supported, err := n.blockProducerStartupClock()
	if err != nil {
		return err
	}
	//nolint:contextcheck // the KES agent loop is bound to the node lifecycle context, not this call
	creds, err := ks.validateBlockProducerStartupForClock(slot, supported)
	if err != nil {
		return fmt.Errorf("block producer startup validation failed: %w", err)
	}
	defer func() {
		if retErr != nil {
			ks.closeKESAgentClient()
			creds.Close()
		}
	}()
	if got := creds.GetPoolID().String(); got != pp.poolID {
		return fmt.Errorf(
			"credentials are for pool %s, operational certificate names %s",
			got,
			pp.poolID,
		)
	}
	if err := n.validateBlockProducerLedger(creds); err != nil {
		return fmt.Errorf(
			"block producer credentials failed ledger check: %w",
			err,
		)
	}
	built, err := n.newPoolForger(
		ctx,
		creds,
		n.poolMetricsRegistry(pp.poolID),
		n.slotArbiterFor(pp.poolID),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize block forger: %w", err)
	}
	n.multiPool.mu.Lock()
	pp.creds = creds
	pp.election = built.election
	pp.forger = built.forger
	n.multiPool.mu.Unlock()
	n.addArbiterMember(pp.poolID, creds, built)
	n.config.logger.Info(
		"block forger started in production mode with leader election",
		"pool_id", pp.poolID,
	)
	return nil
}

// stopPoolProducer stops one pool after the first, in the order the first
// pool's stop uses: forger, KES agent client, election, then the keys.
func (n *Node) stopPoolProducer(pp *poolProducer) {
	if n.multiPool.arbiter != nil {
		n.multiPool.arbiter.remove(pp.poolID)
	}
	n.multiPool.mu.Lock()
	forger, election, creds := pp.forger, pp.election, pp.creds
	pp.forger, pp.election, pp.creds = nil, nil, nil
	n.multiPool.mu.Unlock()
	if forger != nil {
		forger.Stop()
	}
	pp.keySource(n).closeKESAgentClient()
	if election != nil {
		logErrIfNotNil(
			n.config.logger,
			"failed to stop leader election during cleanup",
			election.Stop(),
		)
	}
	if creds != nil {
		creds.Close()
	}
}

// stopPoolProducers stops the retry loop and every pool after the first.
func (n *Node) stopPoolProducers() error {
	n.stopPoolProducerRetry()
	n.multiPool.mu.Lock()
	producers := n.multiPool.producers
	n.multiPool.mu.Unlock()
	// The ledger keeps asking ownForgedBlocks about stopped forgers until the
	// next wiring; their slot trackers still answer for the blocks they made.
	for _, pp := range producers {
		n.stopPoolProducer(pp)
	}
	return nil
}

// startPoolProducerRetry tries each skipped pool again every
// poolProducerRetryInterval until it starts or the node stops. A pool it
// starts runs under ctx, the block producer's lifetime, not the retry loop's.
func (n *Node) startPoolProducerRetry(ctx context.Context) {
	n.multiPool.mu.Lock()
	defer n.multiPool.mu.Unlock()
	if n.multiPool.retryCancel != nil {
		return
	}
	retryCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	n.multiPool.retryCancel, n.multiPool.retryDone = cancel, done
	go func() {
		defer close(done)
		ticker := time.NewTicker(poolProducerRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-retryCtx.Done():
				return
			case <-ticker.C:
				if n.retrySkippedPoolProducers(ctx) {
					return
				}
			}
		}
	}()
}

// retrySkippedPoolProducers makes one pass over the skipped pools and reports
// whether none is left. A pass is skipped while a startup, restore or
// truncate holds the lifecycle gates; they rebuild the pools themselves.
func (n *Node) retrySkippedPoolProducers(ctx context.Context) bool {
	release, err := n.tryLifecycleGates()
	if err != nil {
		return false
	}
	defer release()
	n.multiPool.mu.Lock()
	var skipped []*poolProducer
	for _, pp := range n.multiPool.producers {
		if pp.forger == nil {
			skipped = append(skipped, pp)
		}
	}
	n.multiPool.mu.Unlock()
	remaining := 0
	started := false
	for _, pp := range skipped {
		if err := n.startPoolProducer(ctx, pp); err != nil {
			remaining++
			n.config.logger.Warn(
				"block producer pool still not started",
				"component", "node",
				"pool_id", pp.poolID,
				"error", err,
			)
			continue
		}
		started = true
	}
	if started {
		n.wireForgedBlockTracking()
	}
	return remaining == 0
}

// stopPoolProducerRetry stops the retry loop and waits for its pass to end.
func (n *Node) stopPoolProducerRetry() {
	n.multiPool.mu.Lock()
	cancel, done := n.multiPool.retryCancel, n.multiPool.retryDone
	n.multiPool.retryCancel, n.multiPool.retryDone = nil, nil
	n.multiPool.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// wireForgedBlockTracking tells the ledger and the equivocation detector which
// blocks and pools are this node's own. With one pool the ledger reads the
// forger directly, as it always has; with several it reads every running
// forger through ownForgedBlocks.
func (n *Node) wireForgedBlockTracking() {
	if n.ledgerState == nil || n.blockForger == nil {
		return
	}
	if n.multiPool.own == nil {
		n.ledgerState.SetForgedBlockChecker(n.blockForger.SlotTracker())
		n.ledgerState.SetForgingEnabled(true)
		n.ledgerState.SetSlotBattleRecorder(n.blockForger)
		return
	}
	forgers := []*forging.BlockForger{n.blockForger}
	poolIDs := []string{}
	if creds := n.blockProducerCreds.Load(); creds != nil {
		poolIDs = append(poolIDs, creds.GetPoolID().String())
	}
	n.multiPool.mu.Lock()
	for _, pp := range n.multiPool.producers {
		poolIDs = append(poolIDs, pp.poolID)
		if pp.forger != nil {
			forgers = append(forgers, pp.forger)
		}
	}
	n.multiPool.mu.Unlock()
	n.multiPool.own.set(forgers)
	n.equivocation.setSelfPoolIDs(poolIDs...)
	n.ledgerState.SetForgedBlockChecker(n.multiPool.own)
	n.ledgerState.SetForgingEnabled(true)
	n.ledgerState.SetSlotBattleRecorder(n.multiPool.own)
}

// ownForgedBlocks answers the ledger's slot battle questions for every pool
// this node forges for: a block is the node's own if any of its forgers made
// it, and a battle it loses is counted on that forger.
type ownForgedBlocks struct {
	mu      sync.RWMutex
	forgers []*forging.BlockForger
	// last is the first forger with a block in the slot ForgedBlockHashes
	// last found one in. The ledger records a slot battle right after asking
	// about the slot, so the battle is counted against a pool whose block
	// fought it.
	last *forging.BlockForger
}

func (o *ownForgedBlocks) set(forgers []*forging.BlockForger) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.forgers = forgers
	o.last = nil
}

// ForgedBlockHashes returns every pool's block for slot. Two pools that both
// lead the slot forge it when the slot arbiter fails open, so the slot can
// hold two blocks of the node's own, with different hashes, and the ledger
// must recognise either one.
func (o *ownForgedBlocks) ForgedBlockHashes(slot uint64) [][]byte {
	o.mu.RLock()
	forgers := o.forgers
	o.mu.RUnlock()
	var hashes [][]byte
	var first *forging.BlockForger
	for _, f := range forgers {
		if hash, ok := f.SlotTracker().WasForgedByUs(slot); ok {
			if first == nil {
				first = f
			}
			hashes = append(hashes, hash)
		}
	}
	if first != nil {
		o.mu.Lock()
		o.last = first
		o.mu.Unlock()
	}
	return hashes
}

func (o *ownForgedBlocks) RecordSlotBattle() {
	o.mu.RLock()
	f := o.last
	if f == nil && len(o.forgers) > 0 {
		f = o.forgers[0]
	}
	o.mu.RUnlock()
	if f != nil {
		f.RecordSlotBattle()
	}
}

// poolProducersReady reports every pool after the first that cannot forge,
// or nil.
func (n *Node) poolProducersReady() error {
	n.multiPool.mu.Lock()
	defer n.multiPool.mu.Unlock()
	var errs []error
	for _, pp := range n.multiPool.producers {
		if pp.forger == nil {
			reason := pp.startErr
			if reason == nil {
				reason = errors.New("block forger is not initialized")
			}
			errs = append(errs, fmt.Errorf("pool %s: %w", pp.poolID, reason))
			continue
		}
		if err := blockProducerReadiness(pp.forger); err != nil {
			errs = append(errs, fmt.Errorf("pool %s: %w", pp.poolID, err))
		}
	}
	return errors.Join(errs...)
}

// runningPool is a pool after the first that is forging, and its live
// credentials.
type runningPool struct {
	pp   *poolProducer
	live *forging.PoolCredentials
}

// runningPoolProducers lists the pools after the first that are forging.
func (n *Node) runningPoolProducers() []runningPool {
	n.multiPool.mu.Lock()
	defer n.multiPool.mu.Unlock()
	var pools []runningPool
	for _, pp := range n.multiPool.producers {
		if pp.creds != nil {
			pools = append(pools, runningPool{pp: pp, live: pp.creds})
		}
	}
	return pools
}

// reloadPoolProducerCredentials reloads every running pool after the first
// through its own key source and returns one result per pool, each error
// naming its pool. A sign-mode pool installs the reload preps holds for it. A
// pool that fails keeps its credentials and is counted; the others still
// reload. With skipServeKey a serve-key pool is not reloaded and reports
// itself unchanged.
func (n *Node) reloadPoolProducerCredentials(
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
	skipServeKey bool,
	preps *signReloadPreps,
) []error {
	pools := n.runningPoolProducers()
	results := make([]error, 0, len(pools))
	for _, t := range pools {
		ks := t.pp.keySource(n)
		if skipServeKey && ks.servesKeys() {
			results = append(results, errCredentialsUnchanged)
			continue
		}
		err := n.reloadPoolCredentials(
			ks,
			t.live,
			slot,
			supported,
			ledgerCheck,
			preps.pool(t.pp),
		)
		if err != nil {
			err = fmt.Errorf("pool %s: %w", t.pp.poolID, err)
		}
		results = append(results, err)
	}
	return results
}

// countReloadFailure counts a credential reload that left poolID's keys as
// they were.
func (n *Node) countReloadFailure(poolID string) {
	if counter := n.reloadFailureCounter(); counter != nil {
		counter.WithLabelValues(poolID).Inc()
	}
}

// reloadFailureCounter registers the credential reload failure counter once
// and returns it, or nil without a registry. The counter lives as long as the
// node, so it registers on the registry a live rebuild does not clear.
func (n *Node) reloadFailureCounter() *prometheus.CounterVec {
	n.multiPool.reloadFailuresOnce.Do(func() {
		reg := n.retainedComponentPromRegistry()
		if reg == nil {
			return
		}
		counter := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_block_producer_credential_reload_failures_total",
				Help: "credential reloads refused or failed, by pool; the pool keeps forging with the credentials it had",
			},
			[]string{"pool_id"},
		)
		if err := reg.Register(counter); err != nil {
			n.config.logger.Warn(
				"failed to register the credential reload failure counter",
				"component", "node",
				"error", err,
			)
			return
		}
		n.multiPool.reloadFailures = counter
	})
	return n.multiPool.reloadFailures
}
