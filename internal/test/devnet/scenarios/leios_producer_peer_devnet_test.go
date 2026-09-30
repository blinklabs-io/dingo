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

	for {
		select {
		case <-ctx.Done():
			require.NoError(t, ctx.Err(),
				"no producer EB was certified, referenced, fetched, and applied")
		case err := <-monitor.Errors:
			require.NoError(t, err, "producer LeiosNotify connection failed")
		case point := <-monitor.Offers:
			t.Logf("producer %s offered EB %x at slot %d", producer.Name, point.Hash, point.Slot)
			var referencingHeader devnet.ObservedHeader
			err := group.Await(ctx,
				fmt.Sprintf("relay references announced EB %x at slot %d",
					point.Hash, point.Slot),
				func(snapshots []devnet.ChainSnapshot) bool {
					for _, snapshot := range snapshots {
						if snapshot.Node != relay.Name {
							continue
						}
						for _, header := range snapshot.Headers {
							if bytes.Equal(
								header.LeiosAnnouncementHash,
								point.Hash,
							) {
								referencingHeader = header
								return true
							}
						}
					}
					return false
				})
			require.NoError(t, err,
				"relay never selected a ranking block referencing the offered EB")
			require.Positive(t, referencingHeader.LeiosAnnouncementSize,
				"ranking header must carry a sized Leios announcement")
			t.Logf("relay selected ranking block %x at slot %d for EB %x", referencingHeader.Hash, referencingHeader.Slot, point.Hash)

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
			t.Logf("relay offered transactions for EB %x", point.Hash)

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
			t.Logf("relay served %d verified EB transaction bodies", len(bodies))

			applyCtx, cancelApply := context.WithTimeout(ctx, 2*time.Minute)
			err = awaitLeiosTransactionOutputsApplied(
				applyCtx,
				relayNtcAddr,
				cfg.NetworkMagic,
				bodies,
			)
			cancelApply()
			require.NoError(t, err,
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
