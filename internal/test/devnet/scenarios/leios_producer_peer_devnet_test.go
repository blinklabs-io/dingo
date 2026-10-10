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
// a private Dijkstra network: a producer offers an EB over LeiosNotify and
// diffuses a validated vote after on-chain voting registration becomes usable;
// the relay observes the exact announcing and certifying ranking blocks,
// serves the EB over LeiosFetch, and applies at least one EB transaction output.
func TestLeiosEndorserBlockProducerToPeer(t *testing.T) {
	if os.Getenv("DEVNET_LEIOS_ENABLED") != "1" {
		t.Skip("requires run-tests.sh --leios")
	}

	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	require.NotNil(t, cfg.DijkstraHardForkAtEpoch,
		"Leios spec must activate Dijkstra")
	require.Zero(t, *cfg.DijkstraHardForkAtEpoch,
		"Leios spec must start in Dijkstra")

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

	var announcingHeader devnet.ObservedHeader
	var offeringProducer string
	err = awaitLeiosCanonicalTriple(ctx, group, producerErrors,
		"relay adopts a voted announcing ranking block offered by a producer",
		func(snapshots []devnet.ChainSnapshot) bool {
			var found bool
			announcingHeader, offeringProducer, found =
				producerObservations.canonicalTriple(snapshots, relay.Name)
			return found
		})
	require.NoError(t, err,
		"relay never adopted a voted ranking block for an EB offered by a producer")
	require.Positive(t, announcingHeader.LeiosAnnouncementSize,
		"announcing ranking header must carry a sized Leios announcement")
	point := pcommon.Point{
		Slot: announcingHeader.Slot,
		Hash: append([]byte(nil), announcingHeader.LeiosAnnouncementHash...),
	}

	t.Logf("producer %s offered voted EB %x at slot %d",
		offeringProducer, point.Hash, point.Slot)

	txOfferCtx, cancelTxOffer := context.WithTimeout(ctx, 2*time.Minute)
	err = awaitLeiosTransactionOffer(
		txOfferCtx,
		transactions.TransactionOffers,
		transactions.Errors,
		point,
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

	var certifyingHeader devnet.ObservedHeader
	err = group.Await(ctx,
		fmt.Sprintf("relay adopts CertRB extending announcing ranking block %x",
			announcingHeader.Hash),
		func(snapshots []devnet.ChainSnapshot) bool {
			for _, snapshot := range snapshots {
				if snapshot.Node != relay.Name {
					continue
				}
				for _, header := range snapshot.Headers {
					if header.LeiosCertified && bytes.Equal(
						header.PrevHash,
						announcingHeader.Hash,
					) {
						certifyingHeader = header
						return true
					}
				}
			}
			return false
		})
	require.NoError(t, err,
		"relay never adopted a CertRB for the voted announcing ranking block")

	applyCtx, cancelApply := context.WithTimeout(ctx, 2*time.Minute)
	err = awaitLeiosTransactionOutputsApplied(
		applyCtx,
		relayNtcAddr,
		cfg.NetworkMagic,
		bodies,
	)
	cancelApply()
	require.NoError(t, err,
		"relay did not apply any certified EB transaction output")
	t.Logf(
		"producer %s offered EB %x at slot %d; relay %s certified it in ranking block %x at slot %d, served %d bodies, and applied their outputs",
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

func (o *leiosProducerObservations) canonicalTriple(
	snapshots []devnet.ChainSnapshot,
	relayName string,
) (devnet.ObservedHeader, string, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, snapshot := range snapshots {
		if snapshot.Node != relayName {
			continue
		}
		for i := len(snapshot.Headers) - 1; i >= 0; i-- {
			header := snapshot.Headers[i]
			if len(header.Hash) != lcommon.Blake2b256Size ||
				len(header.LeiosAnnouncementHash) != lcommon.Blake2b256Size {
				continue
			}
			var rbHash lcommon.Blake2b256
			copy(rbHash[:], header.Hash)
			if _, ok := o.votes[rbHash]; !ok {
				continue
			}
			point := leiosPointKey{slot: header.Slot}
			copy(point.hash[:], header.LeiosAnnouncementHash)
			source, ok := o.offers[point]
			if ok {
				return header, source, true
			}
		}
	}
	return devnet.ObservedHeader{}, "", false
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

func awaitLeiosTransactionOutputsApplied(
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
