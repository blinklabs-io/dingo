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
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubKESSigner fails UpdateKESPeriod the way a KES agent client does when the
// agent never delivered a key: ErrNoKeyYet on every won slot.
type stubKESSigner struct {
	updateErr error
	opCert    *OpCert
}

func (s *stubKESSigner) KESSign(uint64, []byte) ([]byte, error) {
	return nil, errors.New("no key")
}

func (s *stubKESSigner) UpdateKESPeriod(uint64) error { return s.updateErr }

func (s *stubKESSigner) GetOpCert() *OpCert { return s.opCert }

func (s *stubKESSigner) OpCertExpiryPeriod() uint64 { return 0 }

func (s *stubKESSigner) PeriodsRemaining(uint64) uint64 { return 0 }

// TestKESUpdateFailureCountsAsCouldNotForge pins that a slot lost to KES key
// evolution is counted.
//
// A node whose KES agent never delivered a key wins a slot, fails
// UpdateKESPeriod, and returns. Without the counter that slot incremented only
// Forge_node_is_leader, so Forge_could_not_forge stayed at zero while every won
// slot was forfeited — the one metric an operator would alert on said nothing
// was wrong.
func TestKESUpdateFailureCountsAsCouldNotForge(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	creds := setupTestCredentials(t)

	forger, err := NewBlockForger(ForgerConfig{
		Mode:        ModeProduction,
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials: creds,
		KESSigner: &stubKESSigner{
			updateErr: errors.New("no KES key received from agent yet"),
		},
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block},
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to update KES period")
	assert.Equal(
		t,
		0,
		broadcaster.calls,
		"no block may be broadcast when the KES key could not be evolved",
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeNodeIsLeader),
		"the slot was won",
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a slot lost to KES evolution must count as could-not-forge",
	)
}

// TestSignBlockHeaderWithoutASignerReturnsAnError pins the guard against a
// typed nil.
//
// NewBlockForger fell back to cfg.Credentials unconditionally, so a nil
// *PoolCredentials landed in the KESSigner interface: f.kes was non-nil while
// its value was nil, the "KES signer not configured" guard never fired, and
// SignBlockHeader panicked on a nil-pointer dereference instead of returning an
// error. Reachable for an external consumer constructing a forger in neither
// ModeDev nor ModeProduction, which is the only path that does not require
// credentials up front.
func TestSignBlockHeaderWithoutASignerReturnsAnError(t *testing.T) {
	forger, err := NewBlockForger(ForgerConfig{
		// Neither ModeDev nor ModeProduction: no credential requirement, so a
		// forger with no signer at all is constructible.
		Mode:   Mode(99),
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	sig, err := forger.SignBlockHeader(1, []byte("header"))
	require.Error(t, err)
	require.ErrorContains(t, err, "KES signer not configured")
	assert.Nil(t, sig)
}

// TestLoadSecretKeyWipesFileBytes pins that the cardano-cli envelope read from
// disk is zeroed before loadSecretKeyFromFile returns.
//
// The envelope's cborHex field holds the KES signing key in the clear, so the
// unwiped buffer left a decodable copy of the pool's signing key in freed heap
// memory for the life of the process. The buffer is local and unreachable
// afterwards, so the only way to observe the wipe is to hand the loader a buffer
// the test keeps a reference to.
func TestLoadSecretKeyWipesFileBytes(t *testing.T) {
	_, kesPath, _ := createTestKeys(t)

	var retained []byte
	original := secretFileBytes
	secretFileBytes = func(r io.Reader) ([]byte, error) {
		data, err := original(r)
		retained = data
		return data, err
	}
	t.Cleanup(func() { secretFileBytes = original })

	key, err := loadSecretKeyFromFile(kesPath)
	require.NoError(t, err)
	// The parsed key is unaffected: LoadKeyFromBytes decodes cborHex into its
	// own allocation.
	require.Len(t, key.SKey, kes.CardanoKesSecretKeySize)
	require.NotEmpty(t, retained, "the file reader seam was not used")
	require.Equal(
		t,
		make([]byte, len(retained)),
		retained,
		"key file bytes were not wiped; the envelope holds the KES signing key",
	)
}
