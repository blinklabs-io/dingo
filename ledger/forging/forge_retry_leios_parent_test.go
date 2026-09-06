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

package forging

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// parentSwapBuilder fails its first attempt with errParentChangedDuringBuild
// and swaps the parent announcement underneath the forge while doing so, so
// the retry runs against a different parent than the one the Leios
// certificate was selected for. It records the Leios data handed to every
// attempt.
type parentSwapBuilder struct {
	block ledger.Block
	cbor  []byte
	calls int
	seen  []LeiosBlockData
	// seenEmpty records whether each attempt was the empty-body
	// fallback, in the same order as seen.
	seenEmpty []bool
	onFirst   func()
	failOnce  bool
	// failNonEmpty fails every attempt allowed to carry transactions, so
	// the forge is driven all the way to the empty-body fallback.
	failNonEmpty bool
}

func (b *parentSwapBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	return nil, nil, errParentChangedDuringBuild
}

func (b *parentSwapBuilder) buildBlockWithCredentialGeneration(
	_ uint64,
	_ uint64,
	leios LeiosBlockData,
	_ *credentialGeneration,
	constraints blockSelectionConstraints,
) (ledger.Block, []byte, error) {
	b.calls++
	b.seen = append(b.seen, leios)
	b.seenEmpty = append(b.seenEmpty, constraints.emptyBody)
	if constraints.emptyBody {
		return b.block, b.cbor, nil
	}
	if b.calls == 1 {
		if b.onFirst != nil {
			b.onFirst()
		}
		if b.failOnce || b.failNonEmpty {
			return nil, nil, errParentChangedDuringBuild
		}
	}
	if b.failNonEmpty {
		return nil, nil, errParentChangedDuringBuild
	}
	return b.block, b.cbor, nil
}

var _ credentialGenerationBlockBuilder = (*parentSwapBuilder)(nil)

func leiosTestCertificate(
	ebHash lcommon.Blake2b256,
	slot uint64,
) *lcommon.LeiosEbCertificate {
	return &lcommon.LeiosEbCertificate{
		SlotNo:            slot,
		EndorserBlockHash: ebHash,
		Signers:           []byte{0x80},
		AggregatedSignature: make(
			[]byte,
			lcommon.LeiosBlsSignatureSize,
		),
	}
}

func leiosHash(b byte) lcommon.Blake2b256 {
	return lcommon.NewBlake2b256(bytes.Repeat([]byte{b}, 32))
}

// TestForgeReResolvesLeiosDataWhenParentChanges is the correctness half of
// the in-slot retry. The Leios certificate a ranking block carries is
// selected for one specific parent -- leiosBlockDataForSlot matches the
// certified endorser block against the parent's announcement -- so retrying
// against a new parent while reusing the old parent's certificate would
// commit a block to a certificate that does not belong to it.
func TestForgeReResolvesLeiosDataWhenParentChanges(t *testing.T) {
	oldParentRb := leiosHash(0xA1)
	oldEb := leiosHash(0xB1)
	newParentRb := leiosHash(0xA2)

	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: oldParentRb,
		hash:   oldEb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				EndorserBlockHash: oldEb,
				AnnouncingRbHash:  oldParentRb,
				SlotNo:            9,
				Certificate:       leiosTestCertificate(oldEb, 9),
			},
		},
	}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:    block,
		cbor:     block.cbor,
		failOnce: true,
		onFirst: func() {
			// A peer block lands: the chain now has a different parent,
			// which announced no endorser block this node holds a
			// certificate for.
			parent.rbHash = newParentRb
			parent.hash = leiosHash(0xB2)
		},
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.seen, 2, "the forge must retry after a parent change")
	require.NotNil(
		t,
		builder.seen[0].Certificate,
		"the first attempt carries the certificate selected for the old parent",
	)
	require.Nil(
		t,
		builder.seen[1].Certificate,
		"the retry must not reuse the old parent's certificate",
	)
	require.Empty(
		t,
		certs.marked,
		"no endorser block was embedded, so none may be marked embedded",
	)
}

// TestForgeDropsStaleAnnouncementWhenParentChanges covers the other
// parent-dependent field. The announcement names an endorser block this
// node selected and broadcast against the previous parent's certified
// closure; carrying it onto a block with a different parent would commit
// to an exclusion set that no longer holds.
func TestForgeDropsStaleAnnouncementWhenParentChanges(t *testing.T) {
	oldParentRb := leiosHash(0xC1)
	oldEb := leiosHash(0xD1)
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: oldParentRb,
		hash:   oldEb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:    block,
		cbor:     block.cbor,
		failOnce: true,
		onFirst:  func() { parent.rbHash = leiosHash(0xC2) },
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  &forgerTestLeiosCaster{},
		LeiosTxValidator:    &sessionMockTxValidator{},
		LeiosMempool: forgerTestMempoolProvider{
			txs: leiosAnnouncementTxs(t),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.seen, 2)
	require.NotNil(
		t,
		builder.seen[0].Announcement,
		"the first attempt announces the endorser block forged for the old parent",
	)
	require.Nil(
		t,
		builder.seen[1].Announcement,
		"the retry must not announce an endorser block selected for the old parent",
	)
}

// TestForgeKeepsLeiosDataWhenParentIsUnchanged is the negative case: a
// retry driven by a generation bump that did not move the parent must keep
// the certificate and the announcement it already resolved, rather than
// throwing away a valid endorser block.
func TestForgeKeepsLeiosDataWhenParentIsUnchanged(t *testing.T) {
	parentRb := leiosHash(0xE1)
	eb := leiosHash(0xF1)
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: parentRb,
		hash:   eb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				EndorserBlockHash: eb,
				AnnouncingRbHash:  parentRb,
				SlotNo:            9,
				Certificate:       leiosTestCertificate(eb, 9),
			},
		},
		txHashes:   []string{},
		txHashesOK: true,
	}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:    block,
		cbor:     block.cbor,
		failOnce: true,
	}

	logs := &strings.Builder{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.seen, 2)
	require.NotNil(t, builder.seen[1].Certificate)
	require.NotContains(
		t,
		logs.String(),
		"leios payload re-resolved",
		"an unchanged parent is not a re-resolution: reporting one would "+
			"make the warning that flags a real parent swap unreadable",
	)
	require.Equal(
		t,
		builder.seen[0].Certificate,
		builder.seen[1].Certificate,
		"an unchanged parent keeps the certificate already selected",
	)
	require.Equal(
		t,
		[]lcommon.Blake2b256{eb},
		certs.marked,
		"the embedded endorser block is still marked after the retry",
	)
}

func leiosAnnouncementTxs(t *testing.T) []MempoolTransaction {
	t.Helper()
	return []MempoolTransaction{
		{
			Hash: "1111111111111111111111111111111111111111111111111111111111111111",
			Cbor: makeMinimalTxCbor(t, 0x11, 0),
			Type: conway.TxTypeConway,
		},
	}
}

// TestForgeReResolvesLeiosDataForEmptyFallback closes the same hole on the
// other recovery path. The empty-body fallback is a fresh build against
// whatever the chain tip is now, so it needs its Leios payload resolved
// against that parent just as a retry does -- an empty block carrying a
// certificate that belongs to a different parent is no better than a full
// one.
func TestForgeReResolvesLeiosDataForEmptyFallback(t *testing.T) {
	oldParentRb := leiosHash(0x71)
	oldEb := leiosHash(0x81)
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: oldParentRb,
		hash:   oldEb,
		ok:     true,
	}
	certs := &forgerTestLeiosCerts{
		eligible: []LeiosCertifiedEndorserBlock{
			{
				EndorserBlockHash: oldEb,
				AnnouncingRbHash:  oldParentRb,
				SlotNo:            9,
				Certificate:       leiosTestCertificate(oldEb, 9),
			},
		},
	}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:        block,
		cbor:         block.cbor,
		failNonEmpty: true,
		onFirst: func() {
			parent.rbHash = leiosHash(0x72)
			parent.hash = leiosHash(0x82)
		},
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			// No slot time left, so the first failure goes straight to
			// the empty-body fallback.
			slotEnd: time.Now(),
		},
		LeiosCertificateProvider:        certs,
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.seen, 2)
	require.False(t, builder.seenEmpty[0])
	require.True(t, builder.seenEmpty[1], "the second attempt is the fallback")
	require.NotNil(t, builder.seen[0].Certificate)
	require.Nil(
		t,
		builder.seen[1].Certificate,
		"the empty fallback must not carry the old parent's certificate",
	)
	require.Empty(t, certs.marked)
}

// TestForgeKeepsAnnouncementWhenParentIsUnchanged is the announcement half
// of the negative case. A retry driven by a generation bump that left the
// parent alone must keep the endorser block this slot already forged and
// broadcast, rather than discarding a valid announcement.
func TestForgeKeepsAnnouncementWhenParentIsUnchanged(t *testing.T) {
	parentRb := leiosHash(0x91)
	parent := &forgerTestLeiosParentAnnouncement{
		rbHash: parentRb,
		hash:   leiosHash(0x92),
		ok:     true,
	}
	block := newForgerTestBlock(10, 2)
	builder := &parentSwapBuilder{
		block:    block,
		cbor:     block.cbor,
		failOnce: true,
	}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		LeiosProduceChecker:             &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:              &forgerTestLeiosCaster{},
		LeiosTxValidator:                &sessionMockTxValidator{},
		LeiosMempool:                    forgerTestMempoolProvider{txs: leiosAnnouncementTxs(t)},
		LeiosCertificateProvider:        &forgerTestLeiosCerts{},
		LeiosParentAnnouncementProvider: parent,
		PromRegistry:                    prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.seen, 2)
	require.NotNil(t, builder.seen[0].Announcement)
	require.Equal(
		t,
		builder.seen[0].Announcement,
		builder.seen[1].Announcement,
		"an unchanged parent keeps the endorser block already announced",
	)
}
