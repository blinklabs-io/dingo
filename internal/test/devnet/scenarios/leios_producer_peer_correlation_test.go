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

	certHash := bytes.Repeat([]byte{0x33}, lcommon.Blake2b256Size)
	header, certifying, source, ok := observations.canonicalCertifiedTriple(
		[]devnet.ChainSnapshot{{
			Node: "relay",
			Headers: []devnet.ObservedHeader{{
				Slot:                  42,
				Hash:                  rbHash.Bytes(),
				LeiosAnnouncementHash: ebHash,
				LeiosAnnouncementSize: 1,
			}, {
				Slot:           43,
				Hash:           certHash,
				PrevHash:       rbHash.Bytes(),
				LeiosCertified: true,
			}},
		}},
		"relay",
	)
	require.True(t, ok)
	require.Equal(t, "producer-a", source)
	require.Equal(t, uint64(42), header.Slot)
	require.Equal(t, uint64(43), certifying.Slot)
}

func TestLeiosProducerObservationsWaitForMatchingCertifyingChild(t *testing.T) {
	t.Parallel()

	strandedRb, strandedEb := leiosObservationHashes(10)
	certifiedRb, certifiedEb := leiosObservationHashes(20)
	observations := newLeiosProducerObservations()
	for _, candidate := range []struct {
		rb   lcommon.Blake2b256
		eb   lcommon.Blake2b256
		slot uint64
	}{
		{rb: strandedRb, eb: strandedEb, slot: 10},
		{rb: certifiedRb, eb: certifiedEb, slot: 20},
	} {
		observations.addOffer("producer-a", pcommon.Point{
			Slot: candidate.slot,
			Hash: candidate.eb.Bytes(),
		})
		observations.addVote("producer-b", lcommon.LeiosPrototypeVote{
			AnnouncingRbHash: candidate.rb,
		})
	}

	plainChildHash := bytes.Repeat([]byte{0x77}, lcommon.Blake2b256Size)
	snapshot := devnet.ChainSnapshot{
		Node: "relay",
		Headers: []devnet.ObservedHeader{{
			Slot:                  10,
			Hash:                  strandedRb.Bytes(),
			LeiosAnnouncementHash: strandedEb.Bytes(),
			LeiosAnnouncementSize: 1,
		}, {
			Slot:     11,
			Hash:     plainChildHash,
			PrevHash: strandedRb.Bytes(),
		}},
	}

	_, _, _, ok := observations.canonicalCertifiedTriple(
		[]devnet.ChainSnapshot{snapshot},
		"relay",
	)
	require.False(t, ok,
		"an offered and voted announcement followed by a plain RB is not the scenario candidate")

	snapshot.Headers = append(snapshot.Headers,
		devnet.ObservedHeader{
			Slot:                  20,
			Hash:                  certifiedRb.Bytes(),
			PrevHash:              plainChildHash,
			LeiosAnnouncementHash: certifiedEb.Bytes(),
			LeiosAnnouncementSize: 1,
		},
		devnet.ObservedHeader{
			Slot:           21,
			Hash:           bytes.Repeat([]byte{0x88}, lcommon.Blake2b256Size),
			PrevHash:       certifiedRb.Bytes(),
			LeiosCertified: true,
		},
	)
	announcing, certifying, source, ok := observations.canonicalCertifiedTriple(
		[]devnet.ChainSnapshot{snapshot},
		"relay",
	)
	require.True(t, ok)
	require.Equal(t, certifiedRb.Bytes(), announcing.Hash)
	require.Equal(t, announcing.Hash, certifying.PrevHash)
	require.Equal(t, "producer-a", source)
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

	_, _, _, ok := observations.canonicalCertifiedTriple(snapshots, "relay")
	require.False(t, ok, "same EB content at another slot is a different occurrence")

	observations.addOffer("producer-b", pcommon.Point{Slot: 80, Hash: ebHash})
	snapshots[0].Headers = append(snapshots[0].Headers, devnet.ObservedHeader{
		Slot:           81,
		Hash:           bytes.Repeat([]byte{0x55}, lcommon.Blake2b256Size),
		PrevHash:       rbHash.Bytes(),
		LeiosCertified: true,
	})
	header, _, source, ok := observations.canonicalCertifiedTriple(snapshots, "relay")
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
	header, _, source, ok := observations.canonicalCertifiedTriple(
		leiosCertifiedObservationSnapshots(1, oldRb, oldEb), "relay")
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
	_, _, _, ok = observations.canonicalCertifiedTriple(
		leiosCertifiedObservationSnapshots(1, oldRb, oldEb), "relay")
	require.False(t, ok, "oldest unique keys must be evicted")
	header, _, source, ok = observations.canonicalCertifiedTriple(
		leiosCertifiedObservationSnapshots(leiosProducerObservationLimit+1, newRb, newEb),
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

func leiosCertifiedObservationSnapshots(
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
		}, {
			Slot:           slot + 1,
			Hash:           bytes.Repeat([]byte{0xcc}, lcommon.Blake2b256Size),
			PrevHash:       rbHash.Bytes(),
			LeiosCertified: true,
		}},
	}}
}
