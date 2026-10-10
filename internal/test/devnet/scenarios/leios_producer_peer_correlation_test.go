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
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestLeiosProducerObservationsCorrelateAcrossNodes(t *testing.T) {
	t.Parallel()

	rbHash := lcommon.Blake2b256(bytes.Repeat([]byte{0x11}, lcommon.Blake2b256Size))
	ebHash := bytes.Repeat([]byte{0x22}, lcommon.Blake2b256Size)
	observations := newLeiosProducerObservations()
	observations.addOffer("producer-a", pcommon.Point{Slot: 42, Hash: ebHash})
	observations.addVote("producer-b",
		lcommon.LeiosPrototypeVote{AnnouncingRbHash: rbHash})

	header, source, ok := observations.canonicalTriple(
		[]devnet.ChainSnapshot{{
			Node: "relay",
			Headers: []devnet.ObservedHeader{{
				Slot:                  42,
				Hash:                  rbHash.Bytes(),
				LeiosAnnouncementHash: ebHash,
				LeiosAnnouncementSize: 1,
			}},
		}},
		"relay",
	)
	require.True(t, ok)
	require.Equal(t, "producer-a", source)
	require.Equal(t, uint64(42), header.Slot)
}

func TestLeiosProducerObservationsKeepRepeatedHashOccurrencesDistinct(t *testing.T) {
	t.Parallel()

	rbHash := lcommon.Blake2b256(bytes.Repeat([]byte{0x33}, lcommon.Blake2b256Size))
	ebHash := bytes.Repeat([]byte{0x44}, lcommon.Blake2b256Size)
	snapshots := []devnet.ChainSnapshot{{
		Node: "relay",
		Headers: []devnet.ObservedHeader{{
			Slot:                  80,
			Hash:                  rbHash.Bytes(),
			LeiosAnnouncementHash: ebHash,
			LeiosAnnouncementSize: 1,
		}},
	}}
	observations := newLeiosProducerObservations()
	observations.addVote("producer-b",
		lcommon.LeiosPrototypeVote{AnnouncingRbHash: rbHash})
	observations.addOffer("producer-a", pcommon.Point{Slot: 79, Hash: ebHash})

	_, _, ok := observations.canonicalTriple(snapshots, "relay")
	require.False(t, ok, "same EB content at another slot is a different occurrence")

	observations.addOffer("producer-b", pcommon.Point{Slot: 80, Hash: ebHash})
	header, source, ok := observations.canonicalTriple(snapshots, "relay")
	require.True(t, ok)
	require.Equal(t, "producer-b", source)
	require.Equal(t, uint64(80), header.Slot)
}

func TestLeiosProducerObservationsEvictOldestUniqueKeys(t *testing.T) {
	t.Parallel()

	observations := newLeiosProducerObservations()
	oldRb, oldEb := leiosObservationHashes(1)
	observations.addOffer("producer-a", pcommon.Point{Slot: 1, Hash: oldEb[:]})
	observations.addVote("producer-a",
		lcommon.LeiosPrototypeVote{AnnouncingRbHash: oldRb})
	for range 10 {
		observations.addOffer("producer-b", pcommon.Point{Slot: 1, Hash: oldEb[:]})
		observations.addVote("producer-b",
			lcommon.LeiosPrototypeVote{AnnouncingRbHash: oldRb})
	}
	for i := uint64(2); i <= leiosProducerObservationLimit; i++ {
		rbHash, ebHash := leiosObservationHashes(i)
		observations.addOffer("producer-a", pcommon.Point{Slot: i, Hash: ebHash[:]})
		observations.addVote("producer-a",
			lcommon.LeiosPrototypeVote{AnnouncingRbHash: rbHash})
	}

	require.Equal(t, leiosProducerObservationLimit, len(observations.offers))
	require.Equal(t, leiosProducerObservationLimit, len(observations.votes))
	header, source, ok := observations.canonicalTriple(
		leiosObservationSnapshots(1, oldRb, oldEb), "relay")
	require.True(t, ok, "duplicate keys must not consume retention capacity")
	require.Equal(t, "producer-b", source, "duplicate offer updates its source")
	require.Equal(t, uint64(1), header.Slot)

	newRb, newEb := leiosObservationHashes(leiosProducerObservationLimit + 1)
	observations.addOffer("producer-c", pcommon.Point{
		Slot: leiosProducerObservationLimit + 1,
		Hash: newEb[:],
	})
	observations.addVote("producer-c",
		lcommon.LeiosPrototypeVote{AnnouncingRbHash: newRb})

	require.Equal(t, leiosProducerObservationLimit, len(observations.offers))
	require.Equal(t, leiosProducerObservationLimit, len(observations.votes))
	_, _, ok = observations.canonicalTriple(
		leiosObservationSnapshots(1, oldRb, oldEb), "relay")
	require.False(t, ok, "oldest unique keys must be evicted")
	header, source, ok = observations.canonicalTriple(
		leiosObservationSnapshots(leiosProducerObservationLimit+1, newRb, newEb),
		"relay",
	)
	require.True(t, ok, "most recent canonical triple must remain retained")
	require.Equal(t, "producer-c", source)
	require.Equal(t, uint64(leiosProducerObservationLimit+1), header.Slot)
}

func leiosObservationHashes(
	value uint64,
) (lcommon.Blake2b256, lcommon.Blake2b256) {
	var rbHash lcommon.Blake2b256
	var ebHash lcommon.Blake2b256
	binary.BigEndian.PutUint64(rbHash[len(rbHash)-8:], value)
	binary.BigEndian.PutUint64(ebHash[len(ebHash)-8:], value)
	ebHash[0] = 1
	return rbHash, ebHash
}

func leiosObservationSnapshots(
	slot uint64,
	rbHash lcommon.Blake2b256,
	ebHash lcommon.Blake2b256,
) []devnet.ChainSnapshot {
	return []devnet.ChainSnapshot{{
		Node: "relay",
		Headers: []devnet.ObservedHeader{{
			Slot:                  slot,
			Hash:                  rbHash.Bytes(),
			LeiosAnnouncementHash: ebHash.Bytes(),
			LeiosAnnouncementSize: 1,
		}},
	}}
}
