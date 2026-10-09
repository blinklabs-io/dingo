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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLeiosEndorserBlockProducerToPeer verifies the producer-to-peer path on
// a private Dijkstra network: a producer offers an EB over LeiosNotify, the
// non-forging relay serves its manifest and complete transaction bodies over
// LeiosFetch, a ranking-block header observed from that relay references the
// same EB, and at least one EB transaction output is present in the relay's
// queried ledger state.
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
	producer := endpoints[0]
	relay := endpoints[len(endpoints)-1]
	require.Equal(t, "producer", producer.Role)
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

	monitor, err := devnet.WatchLeiosEndorserBlockOffers(
		ctx,
		producer.Address,
		cfg.NetworkMagic,
	)
	require.NoError(t, err, "could not observe producer LeiosNotify offers")
	defer monitor.Stop()
	scenarioCtx, cancelScenario := context.WithCancelCause(ctx)
	defer cancelScenario(nil)
	go func() {
		select {
		case err, ok := <-monitor.Errors:
			if !ok {
				cancelScenario(errors.New(
					"producer LeiosNotify error stream closed",
				))
				return
			}
			if err != nil {
				cancelScenario(fmt.Errorf(
					"producer LeiosNotify connection failed: %w",
					err,
				))
			}
		case <-scenarioCtx.Done():
		}
	}()
	producerOffers := leiosProducerOfferMatcher{}
	relayTransactionOffers := leiosTransactionOfferMatcher{}

	for {
		point, err := producerOffers.await(
			scenarioCtx,
			monitor.Offers,
			monitor.TransactionOffers,
		)
		require.NoError(t, leiosScenarioError(scenarioCtx, err),
			"no producer EB was certified, referenced, fetched, and applied")
		t.Logf("producer %s offered EB %x at slot %d", producer.Name, point.Hash, point.Slot)
		var referencingHeader devnet.ObservedHeader
		err = group.Await(scenarioCtx,
			fmt.Sprintf("relay references announced EB %x at slot %d",
				point.Hash, point.Slot),
			func(snapshots []devnet.ChainSnapshot) bool {
				var found bool
				referencingHeader, found = leiosReferencingHeader(
					snapshots,
					relay.Name,
					point,
				)
				return found
			})
		require.NoError(t, leiosScenarioError(scenarioCtx, err),
			"relay never selected a ranking block referencing the offered EB")
		require.Positive(t, referencingHeader.LeiosAnnouncementSize,
			"ranking header must carry a sized Leios announcement")
		t.Logf("relay selected ranking block %x at slot %d for EB %x", referencingHeader.Hash, referencingHeader.Slot, point.Hash)
		referenceCurrent := func() bool {
			return leiosReferenceOnCanonicalChain(
				group.Snapshots(),
				relay.Name,
				referencingHeader,
			)
		}

		txOfferCtx, cancelTxOffer := context.WithTimeout(scenarioCtx, 2*time.Minute)
		err = relayTransactionOffers.await(
			txOfferCtx,
			transactions.TransactionOffers,
			transactions.Errors,
			point,
			referenceCurrent,
		)
		cancelTxOffer()
		if errors.Is(err, errLeiosReferenceRolledBack) {
			t.Logf(
				"relay ranking block %x at slot %d rolled back before its EB transaction offer was observed",
				referencingHeader.Hash,
				referencingHeader.Slot,
			)
			continue
		}
		require.NoError(t, leiosScenarioError(scenarioCtx, err),
			"relay did not offer the EB transactions before the fetch request")
		t.Logf("relay offered transactions for EB %x", point.Hash)

		bodies, err := devnet.FetchLeiosEndorserBlock(
			scenarioCtx,
			relay.Address,
			cfg.NetworkMagic,
			point,
		)
		require.NoError(t, leiosScenarioError(scenarioCtx, err),
			"relay did not serve the announced EB manifest and bodies")
		require.NotEmpty(t, bodies,
			"relay served an EB without transaction bodies")
		t.Logf("relay served %d verified EB transaction bodies", len(bodies))

		applyCtx, cancelApply := context.WithTimeout(scenarioCtx, 2*time.Minute)
		err = awaitLeiosTransactionOutputsApplied(
			applyCtx,
			relayNtcAddr,
			cfg.NetworkMagic,
			bodies,
			referenceCurrent,
		)
		cancelApply()
		if errors.Is(err, errLeiosReferenceRolledBack) {
			t.Logf(
				"relay ranking block %x at slot %d rolled back before its EB output was observed",
				referencingHeader.Hash,
				referencingHeader.Slot,
			)
			continue
		}
		require.NoError(t, leiosScenarioError(scenarioCtx, err),
			"relay did not apply any fetched EB transaction output")
		t.Logf(
			"producer %s offered EB %x at slot %d; relay %s served %d bodies and applied their outputs in ranking block %x at slot %d",
			producer.Name,
			point.Hash,
			point.Slot,
			relay.Name,
			len(bodies),
			referencingHeader.Hash,
			referencingHeader.Slot,
		)
		return
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
