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
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLeiosEndorserBlockProducerToPeer verifies the producer-to-peer path on
// a private Conway-to-Dijkstra network: a producer offers an EB over
// LeiosNotify and diffuses a validated vote after on-chain voting registration
// becomes usable; the relay observes the announcing and certifying ranking
// blocks, serves the EB over LeiosFetch, and applies an EB transaction output.
func TestLeiosEndorserBlockProducerToPeer(t *testing.T) {
	if os.Getenv("DEVNET_LEIOS_ENABLED") != "1" {
		t.Skip("requires run-tests.sh --leios")
	}

	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	require.NotNil(t, cfg.DijkstraHardForkAtEpoch,
		"Leios spec must activate Dijkstra")
	require.Equal(t, uint64(1), *cfg.DijkstraHardForkAtEpoch,
		"Leios spec must cross from Conway to Dijkstra at epoch 1")

	endpoints := devnet.LoadEndpoints()
	require.Len(t, endpoints, 4)
	relay := endpoints[len(endpoints)-1]
	require.Equal(t, "relay", relay.Role)

	ntcAddrs := devnet.DingoNtcAddrs()
	relayNtcAddr, ok := ntcAddrs[relay.Name]
	require.True(t, ok, "relay NtC address is not configured")

	ctl, err := devnet.NewNodeControl(t.Logf)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	observers := devnet.StartObservers(
		ctx,
		endpoints,
		cfg.NetworkMagic,
		t.Logf,
	)
	defer observers.Stop()
	group := observers.Group()
	producerObservations := newLeiosProducerObservations()
	producerErrors := make(chan error, len(endpoints))
	producerMonitors := make([]*devnet.LeiosEndorserBlockOfferMonitor, 0, len(endpoints)-1)
	for _, endpoint := range endpoints {
		if endpoint.Role != "producer" {
			continue
		}
		monitor, err := devnet.WatchLeiosEndorserBlockOffers(
			ctx,
			endpoint.Address,
			cfg.NetworkMagic,
		)
		require.NoErrorf(t, err,
			"could not observe producer %s LeiosNotify offers", endpoint.Name)
		producerMonitors = append(producerMonitors, monitor)
		go collectLeiosProducerObservations(
			ctx,
			endpoint.Name,
			monitor,
			producerObservations,
			producerErrors,
		)
	}
	require.NotEmpty(t, producerMonitors, "no producer endpoints are configured")
	defer func() {
		for _, monitor := range producerMonitors {
			monitor.Stop()
		}
	}()

	transactions, err := devnet.WatchLeiosEndorserBlockOffers(
		ctx,
		relay.Address,
		cfg.NetworkMagic,
	)
	require.NoError(t, err, "could not observe relay LeiosNotify offers")
	defer transactions.Stop()

	services := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Container != "" {
			services = append(services, endpoint.Container)
		}
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		captureCtx, captureCancel := context.WithTimeout(
			context.Background(), time.Minute,
		)
		defer captureCancel()
		ctl.CaptureFailureArtifacts(
			captureCtx,
			"leios-producer-peer",
			group.Snapshots(),
			services,
		)
	})

	readyCtx, cancelReady := context.WithTimeout(ctx, 2*time.Minute)
	require.NoError(t, group.Await(readyCtx,
		"every Leios node has started streaming headers",
		func(snapshots []devnet.ChainSnapshot) bool {
			for _, snapshot := range snapshots {
				if !snapshot.Connected || snapshot.RollForwards == 0 {
					return false
				}
			}
			return true
		}), "Leios devnet did not become ready")
	cancelReady()

	forkSlot := cfg.EpochLength * *cfg.DijkstraHardForkAtEpoch
	forkCtx, cancelFork := context.WithTimeout(ctx, 3*time.Minute)
	err = group.Await(forkCtx,
		"every node observes Conway before and Dijkstra after the scheduled hard fork",
		func(snapshots []devnet.ChainSnapshot) bool {
			for _, snapshot := range snapshots {
				var conwayBeforeFork, dijkstraAfterFork bool
				for _, header := range snapshot.Headers {
					if header.Era == "Conway" && header.Slot < forkSlot {
						conwayBeforeFork = true
					}
					if header.Era == "Dijkstra" && header.Slot >= forkSlot {
						dijkstraAfterFork = true
					}
				}
				if !conwayBeforeFork || !dijkstraAfterFork {
					return false
				}
			}
			return true
		})
	cancelFork()
	require.NoError(t, err,
		"devnet did not cross the configured Conway-to-Dijkstra boundary")

	var announcingHeader devnet.ObservedHeader
	var certifyingHeader devnet.ObservedHeader
	var offeringProducer string
	err = awaitLeiosCanonicalTriple(ctx, group, producerErrors,
		"relay adopts a certifying ranking block for an offered and voted endorser block",
		func(snapshots []devnet.ChainSnapshot) bool {
			var found bool
			announcingHeader, certifyingHeader, offeringProducer, found =
				producerObservations.canonicalCertifiedTriple(
					snapshots,
					relay.Name,
				)
			return found
		})
	require.NoError(t, err,
		"relay never adopted a CertRB for an EB offered and voted by producers")
	require.Positive(t, announcingHeader.LeiosAnnouncementSize,
		"announcing ranking header must carry a sized Leios announcement")
	point := pcommon.Point{
		Slot: announcingHeader.Slot,
		Hash: append([]byte(nil), announcingHeader.LeiosAnnouncementHash...),
	}

	t.Logf("producer %s offered voted EB %x at slot %d",
		offeringProducer, point.Hash, point.Slot)

	txOfferCtx, cancelTxOffer := context.WithTimeout(ctx, 2*time.Minute)
	referenceCurrent := func() bool {
		return leiosReferenceOnCanonicalChain(group.Snapshots(), relay.Name, certifyingHeader)
	}
	transactionOffers := leiosTransactionOfferMatcher{}
	err = transactionOffers.await(
		txOfferCtx,
		transactions.TransactionOffers,
		transactions.Errors,
		point,
		referenceCurrent,
	)
	cancelTxOffer()
	require.NoError(t, err,
		"relay did not offer the EB transactions before the fetch request")

	bodies, err := devnet.FetchLeiosEndorserBlock(
		ctx,
		relay.Address,
		cfg.NetworkMagic,
		point,
	)
	require.NoError(t, err,
		"relay did not serve the announced EB manifest and bodies")
	require.NotEmpty(t, bodies,
		"relay served an EB without transaction bodies")

	applyCtx, cancelApply := context.WithTimeout(ctx, 2*time.Minute)
	err = awaitLeiosTransactionOutputsApplied(
		applyCtx,
		relayNtcAddr,
		cfg.NetworkMagic,
		bodies,
		referenceCurrent,
	)
	cancelApply()
	require.NoError(t, err,
		"relay did not apply any certified EB transaction output")
	t.Logf(
		"all nodes crossed Conway-to-Dijkstra at slot %d; producer %s offered EB %x at slot %d; relay %s certified it in ranking block %x at slot %d, served %d bodies, and applied their outputs",
		forkSlot,
		offeringProducer,
		point.Hash,
		point.Slot,
		relay.Name,
		certifyingHeader.Hash,
		certifyingHeader.Slot,
		len(bodies),
	)
}

type leiosPointKey struct {
	slot uint64
	hash lcommon.Blake2b256
}

const leiosProducerObservationLimit = 4096

type leiosProducerObservations struct {
	mu         sync.RWMutex
	offers     map[leiosPointKey]string
	offerOrder []leiosPointKey
	offerNext  int
	votes      map[lcommon.Blake2b256]map[string]struct{}
	voteOrder  []lcommon.Blake2b256
	voteNext   int
}

func newLeiosProducerObservations() *leiosProducerObservations {
	return &leiosProducerObservations{
		offers:     make(map[leiosPointKey]string),
		offerOrder: make([]leiosPointKey, 0, leiosProducerObservationLimit),
		votes:      make(map[lcommon.Blake2b256]map[string]struct{}),
		voteOrder:  make([]lcommon.Blake2b256, 0, leiosProducerObservationLimit),
	}
}

func (o *leiosProducerObservations) addOffer(source string, point pcommon.Point) {
	if len(point.Hash) != lcommon.Blake2b256Size {
		return
	}
	key := leiosPointKey{slot: point.Slot}
	copy(key.hash[:], point.Hash)
	o.mu.Lock()
	if _, exists := o.offers[key]; !exists {
		if len(o.offerOrder) == leiosProducerObservationLimit {
			delete(o.offers, o.offerOrder[o.offerNext])
			o.offerOrder[o.offerNext] = key
			o.offerNext = (o.offerNext + 1) % leiosProducerObservationLimit
		} else {
			o.offerOrder = append(o.offerOrder, key)
		}
	}
	o.offers[key] = source
	o.mu.Unlock()
}

func (o *leiosProducerObservations) addVote(
	source string,
	vote lcommon.LeiosPrototypeVote,
) {
	o.mu.Lock()
	if o.votes[vote.AnnouncingRbHash] == nil {
		if len(o.voteOrder) == leiosProducerObservationLimit {
			delete(o.votes, o.voteOrder[o.voteNext])
			o.voteOrder[o.voteNext] = vote.AnnouncingRbHash
			o.voteNext = (o.voteNext + 1) % leiosProducerObservationLimit
		} else {
			o.voteOrder = append(o.voteOrder, vote.AnnouncingRbHash)
		}
		o.votes[vote.AnnouncingRbHash] = make(map[string]struct{})
	}
	o.votes[vote.AnnouncingRbHash][source] = struct{}{}
	o.mu.Unlock()
}

func (o *leiosProducerObservations) canonicalCertifiedTriple(
	snapshots []devnet.ChainSnapshot,
	relayName string,
) (devnet.ObservedHeader, devnet.ObservedHeader, string, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, snapshot := range snapshots {
		if snapshot.Node != relayName {
			continue
		}
		for certIndex := len(snapshot.Headers) - 1; certIndex > 0; certIndex-- {
			certifying := snapshot.Headers[certIndex]
			if !certifying.LeiosCertified {
				continue
			}
			announcing := snapshot.Headers[certIndex-1]
			if !bytes.Equal(certifying.PrevHash, announcing.Hash) ||
				len(announcing.Hash) != lcommon.Blake2b256Size ||
				len(announcing.LeiosAnnouncementHash) != lcommon.Blake2b256Size {
				continue
			}
			var rbHash lcommon.Blake2b256
			copy(rbHash[:], announcing.Hash)
			if _, ok := o.votes[rbHash]; !ok {
				continue
			}
			point := leiosPointKey{slot: announcing.Slot}
			copy(point.hash[:], announcing.LeiosAnnouncementHash)
			source, ok := o.offers[point]
			if ok {
				return announcing, certifying, source, true
			}
		}
	}
	return devnet.ObservedHeader{}, devnet.ObservedHeader{}, "", false
}

func collectLeiosProducerObservations(
	ctx context.Context,
	source string,
	monitor *devnet.LeiosEndorserBlockOfferMonitor,
	observations *leiosProducerObservations,
	errors chan<- error,
) {
	for {
		select {
		case point := <-monitor.Offers:
			observations.addOffer(source, point)
		case vote := <-monitor.PrototypeVotes:
			observations.addVote(source, vote)
		case err := <-monitor.Errors:
			if err != nil {
				select {
				case errors <- fmt.Errorf("producer %s LeiosNotify: %w", source, err):
				default:
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func awaitLeiosCanonicalTriple(
	ctx context.Context,
	group *devnet.ChainGroup,
	errors <-chan error,
	description string,
	predicate func([]devnet.ChainSnapshot) bool,
) error {
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- group.Await(waitCtx, description, predicate)
	}()
	select {
	case err := <-done:
		return err
	case err := <-errors:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func awaitLeiosTransactionOutputsAppliedLegacy(
	ctx context.Context,
	addr string,
	magic uint32,
	bodies [][]byte,
) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		applied, err := devnet.LeiosTransactionOutputsApplied(
			ctx,
			addr,
			magic,
			bodies,
		)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func awaitLeiosTransactionOffer(
	ctx context.Context,
	offers <-chan pcommon.Point,
	errors <-chan error,
	want pcommon.Point,
) error {
	for {
		select {
		case point := <-offers:
			if point.Slot == want.Slot && bytes.Equal(point.Hash, want.Hash) {
				return nil
			}
		case err := <-errors:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type leiosOfferKey struct {
	slot uint64
	hash string
}

type leiosProducerOfferMatcher struct {
	manifests    map[leiosOfferKey]pcommon.Point
	transactions map[leiosOfferKey]struct{}
}

func (m *leiosProducerOfferMatcher) await(
	ctx context.Context,
	manifestOffers <-chan pcommon.Point,
	transactionOffers <-chan pcommon.Point,
) (pcommon.Point, error) {
	if m.manifests == nil {
		m.manifests = make(map[leiosOfferKey]pcommon.Point)
	}
	if m.transactions == nil {
		m.transactions = make(map[leiosOfferKey]struct{})
	}
	for {
		select {
		case point, ok := <-manifestOffers:
			if !ok {
				return pcommon.Point{}, errors.New(
					"producer LeiosNotify manifest offer stream closed",
				)
			}
			key := leiosOfferKey{slot: point.Slot, hash: string(point.Hash)}
			if _, ok := m.transactions[key]; ok {
				delete(m.transactions, key)
				point.Hash = append([]byte(nil), point.Hash...)
				return point, nil
			}
			point.Hash = append([]byte(nil), point.Hash...)
			m.manifests[key] = point
		case point, ok := <-transactionOffers:
			if !ok {
				return pcommon.Point{}, errors.New(
					"producer LeiosNotify transaction offer stream closed",
				)
			}
			key := leiosOfferKey{slot: point.Slot, hash: string(point.Hash)}
			if manifest, ok := m.manifests[key]; ok {
				delete(m.manifests, key)
				return manifest, nil
			}
			m.transactions[key] = struct{}{}
		case <-ctx.Done():
			return pcommon.Point{}, ctx.Err()
		}
	}
}

func leiosScenarioError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return err
}

var errLeiosReferenceRolledBack = errors.New(
	"Leios referencing block rolled back",
)

func leiosReferencingHeader(
	snapshots []devnet.ChainSnapshot,
	node string,
	point pcommon.Point,
) (devnet.ObservedHeader, bool) {
	// The ranking header carries the announced EB content hash and size, but
	// not the slot from its LeiosNotify point. The slot lower bound rejects an
	// older occurrence of repeated content. The subsequent exact-point
	// transaction offer binds the selected occurrence; an eligible later
	// header with the same hash names the same validated EB content.
	for _, snapshot := range snapshots {
		if snapshot.Node != node {
			continue
		}
		for _, header := range snapshot.Headers {
			if header.Slot >= point.Slot && bytes.Equal(
				header.LeiosAnnouncementHash,
				point.Hash,
			) {
				return header, true
			}
		}
		return devnet.ObservedHeader{}, false
	}
	return devnet.ObservedHeader{}, false
}

func leiosReferenceOnCanonicalChain(
	snapshots []devnet.ChainSnapshot,
	node string,
	reference devnet.ObservedHeader,
) bool {
	for _, snapshot := range snapshots {
		if snapshot.Node != node {
			continue
		}
		hash, ok := snapshot.HashAt(reference.Slot)
		return ok && bytes.Equal(hash, reference.Hash)
	}
	return false
}

func awaitLeiosTransactionOutputsApplied(
	ctx context.Context,
	addr string,
	magic uint32,
	bodies [][]byte,
	referenceCurrent func() bool,
) error {
	return awaitLeiosApplication(
		ctx,
		func(ctx context.Context) (bool, error) {
			return devnet.LeiosTransactionOutputsApplied(
				ctx,
				addr,
				magic,
				bodies,
			)
		},
		referenceCurrent,
	)
}

func awaitLeiosApplication(
	ctx context.Context,
	appliedNow func(context.Context) (bool, error),
	referenceCurrent func() bool,
) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !referenceCurrent() {
			return errLeiosReferenceRolledBack
		}
		applied, err := appliedNow(ctx)
		if err != nil {
			return err
		}
		if !referenceCurrent() {
			return errLeiosReferenceRolledBack
		}
		if applied {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

type leiosTransactionOfferMatcher struct {
	offers map[leiosOfferKey]struct{}
}

func (m *leiosTransactionOfferMatcher) await(
	ctx context.Context,
	offers <-chan pcommon.Point,
	errorStream <-chan error,
	want pcommon.Point,
	referenceCurrent func() bool,
) error {
	if m.offers == nil {
		m.offers = make(map[leiosOfferKey]struct{})
	}
	wantKey := leiosOfferKey{slot: want.Slot, hash: string(want.Hash)}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !referenceCurrent() {
			return errLeiosReferenceRolledBack
		}
		if _, ok := m.offers[wantKey]; ok {
			if !referenceCurrent() {
				return errLeiosReferenceRolledBack
			}
			delete(m.offers, wantKey)
			return nil
		}
		select {
		case point, ok := <-offers:
			if !ok {
				return errors.New("relay LeiosNotify transaction offer stream closed")
			}
			key := leiosOfferKey{slot: point.Slot, hash: string(point.Hash)}
			m.offers[key] = struct{}{}
			if key == wantKey {
				if !referenceCurrent() {
					return errLeiosReferenceRolledBack
				}
				delete(m.offers, wantKey)
				return nil
			}
		case err, ok := <-errorStream:
			if !ok {
				return errors.New("relay LeiosNotify error stream closed")
			}
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
