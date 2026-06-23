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

package midnightindexer

import (
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"sync"
	"testing"

	fxcbor "github.com/fxamacker/cbor/v2"

	"github.com/blinklabs-io/dingo/database/models"
	dingoEvent "github.com/blinklabs-io/dingo/event"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// mockStore is an in-memory implementation of Store for testing.
type mockStore struct {
	mu                sync.Mutex
	initialCandidates []models.Utxo
	govDatums         []*models.MidnightGovernanceDatum
	ariadneParams     []*models.MidnightAriadneParams
	epochCandidates   []*models.MidnightEpochCandidates
}

func (s *mockStore) GetMidnightCandidates(string) ([]models.Utxo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.initialCandidates, nil
}

func (s *mockStore) InsertMidnightGovernanceDatum(d *models.MidnightGovernanceDatum) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.govDatums = append(s.govDatums, d)
	return nil
}

func (s *mockStore) GetLatestMidnightAriadneParams() (*models.MidnightAriadneParams, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ariadneParams) == 0 {
		return nil, nil
	}
	return s.ariadneParams[len(s.ariadneParams)-1], nil
}

func (s *mockStore) UpsertMidnightAriadneParams(p *models.MidnightAriadneParams) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ariadneParams = append(s.ariadneParams, p)
	return nil
}

func (s *mockStore) UpsertMidnightEpochCandidates(ec *models.MidnightEpochCandidates) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epochCandidates = append(s.epochCandidates, ec)
	return nil
}

// testLogger returns a discard logger for test isolation.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testPolicyHex generates a simple 28-byte policy ID hex string for tests.
func testPolicyHex(fill byte) string {
	b := make([]byte, 28)
	for i := range b {
		b[i] = fill
	}
	return hex.EncodeToString(b)
}

// testAddress is a real mainnet base address used across tests.
const testAddress = "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd"

// testAddress2 is a distinct mainnet address for council / candidate tests.
const testAddress2 = "addr1qx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3n0d3vllmyqwsx5wktcd8cc3sq835lu7drv2xwl2wywfgse35a3x"

// newTestIndexer builds an Indexer with given config and a mock store,
// pre-loading the store's initial ariadne datum into in-memory state.
func newTestIndexer(t *testing.T, cfg Config) (*Indexer, *mockStore) {
	t.Helper()
	store := &mockStore{}
	cfg.Store = store
	if cfg.Logger == nil {
		cfg.Logger = testLogger()
	}
	idx, err := New(cfg)
	require.NoError(t, err)
	return idx, store
}

// TestGovernanceInsertGuarantee verifies that two governance outputs in the same
// scan both produce independent DB rows (always INSERT, never overwrite).
func TestGovernanceInsertGuarantee(t *testing.T) {
	t.Parallel()

	policyHex := testPolicyHex(0xAA)
	idx, store := newTestIndexer(t, Config{
		TechnicalCommitteeAddress:  testAddress,
		TechnicalCommitteePolicyID: policyHex,
	})

	policy, err := hex.DecodeString(policyHex)
	require.NoError(t, err)

	datum1 := []byte{0x01, 0x02}
	datum2 := []byte{0x03, 0x04}

	output1, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(testAddress).
		WithLovelace(1_000_000).
		WithAssets(mockledger.Asset{PolicyId: policy, AssetName: []byte("t"), Amount: 1}).
		WithDatum(datum1).
		Build()
	require.NoError(t, err)

	output2, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(testAddress).
		WithLovelace(1_000_000).
		WithAssets(mockledger.Asset{PolicyId: policy, AssetName: []byte("t"), Amount: 1}).
		WithDatum(datum2).
		Build()
	require.NoError(t, err)

	txHash := make([]byte, 32)
	idx.mu.Lock()
	idx.processOutput(100, txHash, 0, output1)
	idx.processOutput(100, txHash, 1, output2)
	idx.mu.Unlock()

	require.Len(t, store.govDatums, 2,
		"both governance outputs must produce separate DB rows")
	require.Equal(t, models.MidnightGovernanceDatumTypeTechnicalCommittee, store.govDatums[0].DatumType)
	require.Equal(t, models.MidnightGovernanceDatumTypeTechnicalCommittee, store.govDatums[1].DatumType)
	require.Equal(t, uint64(100), store.govDatums[0].BlockNumber)
}

// TestGovernanceTechnicalCommitteeAndCouncilDistinct verifies that technical
// committee and council outputs are stored with their respective datum types.
func TestGovernanceTechnicalCommitteeAndCouncilDistinct(t *testing.T) {
	t.Parallel()

	tcPolicyHex := testPolicyHex(0xBB)
	councilPolicyHex := testPolicyHex(0xCC)

	idx, store := newTestIndexer(t, Config{
		TechnicalCommitteeAddress:  testAddress,
		TechnicalCommitteePolicyID: tcPolicyHex,
		CouncilAddress:             testAddress2,
		CouncilPolicyID:            councilPolicyHex,
	})

	tcPolicy, _ := hex.DecodeString(tcPolicyHex)
	councilPolicy, _ := hex.DecodeString(councilPolicyHex)
	datum := []byte{0x18, 0x2A}
	txHash := make([]byte, 32)

	tcOutput, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(testAddress).
		WithLovelace(1_000_000).
		WithAssets(mockledger.Asset{PolicyId: tcPolicy, AssetName: []byte("t"), Amount: 1}).
		WithDatum(datum).
		Build()
	require.NoError(t, err)

	councilOutput, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(testAddress2).
		WithLovelace(1_000_000).
		WithAssets(mockledger.Asset{PolicyId: councilPolicy, AssetName: []byte("t"), Amount: 1}).
		WithDatum(datum).
		Build()
	require.NoError(t, err)

	idx.mu.Lock()
	idx.processOutput(200, txHash, 0, tcOutput)
	idx.processOutput(200, txHash, 1, councilOutput)
	idx.mu.Unlock()

	require.Len(t, store.govDatums, 2)
	types := map[string]bool{}
	for _, d := range store.govDatums {
		types[d.DatumType] = true
	}
	require.True(t, types[models.MidnightGovernanceDatumTypeTechnicalCommittee])
	require.True(t, types[models.MidnightGovernanceDatumTypeCouncil])
}

// TestAriadneDeduplicate verifies that the same datum is not written twice
// and that a changed datum does produce a new row.
func TestAriadneDeduplicate(t *testing.T) {
	t.Parallel()

	policyHex := testPolicyHex(0xDD)
	idx, store := newTestIndexer(t, Config{
		PermissionedCandidatePolicy: policyHex,
	})

	policy, _ := hex.DecodeString(policyHex)
	datumA := []byte{0x01}
	datumB := []byte{0x02}
	txHash := make([]byte, 32)

	buildAriadneOutput := func(datum []byte) *mockledger.MockTransactionOutput {
		out, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(testAddress).
			WithLovelace(1_000_000).
			WithAssets(mockledger.Asset{PolicyId: policy, AssetName: []byte("p"), Amount: 1}).
			WithDatum(datum).
			Build()
		require.NoError(t, err)
		return out.(*mockledger.MockTransactionOutput)
	}

	idx.mu.Lock()
	idx.processOutput(1, txHash, 0, buildAriadneOutput(datumA))
	idx.mu.Unlock()
	require.Len(t, store.ariadneParams, 1, "first unique datum must be stored")

	// Same datum again — must be deduplicated.
	idx.mu.Lock()
	idx.processOutput(2, txHash, 0, buildAriadneOutput(datumA))
	idx.mu.Unlock()
	require.Len(t, store.ariadneParams, 1, "duplicate datum must not produce a second row")

	// Different datum — must be written.
	idx.mu.Lock()
	idx.processOutput(3, txHash, 0, buildAriadneOutput(datumB))
	idx.mu.Unlock()
	require.Len(t, store.ariadneParams, 2, "changed datum must be stored")
}

// TestCandidateChurn verifies the in-memory candidate set add/remove lifecycle
// and that the epoch snapshot captures the correct state.
func TestCandidateChurn(t *testing.T) {
	t.Parallel()

	idx, store := newTestIndexer(t, Config{
		CommitteeCandidateAddress: testAddress,
	})

	txHash1 := make([]byte, 32)
	txHash1[0] = 0x01
	txHash2 := make([]byte, 32)
	txHash2[0] = 0x02

	datum := []byte{0x01}

	buildCandidateOutput := func() *mockledger.MockTransactionOutput {
		out, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(testAddress).
			WithLovelace(2_000_000).
			WithDatum(datum).
			Build()
		require.NoError(t, err)
		return out.(*mockledger.MockTransactionOutput)
	}

	idx.mu.Lock()
	// Add two candidate UTxOs.
	idx.processOutput(1, txHash1, 0, buildCandidateOutput())
	idx.processOutput(1, txHash2, 0, buildCandidateOutput())
	require.Len(t, idx.candidates, 2)

	// Spend the first one.
	idx.removeCandidate(txHash1, 0)
	require.Len(t, idx.candidates, 1, "spent candidate must be removed from the in-memory set")
	idx.mu.Unlock()

	// Epoch transition must snapshot the remaining candidate.
	idx.handleEpochTransition(dingoEvent.Event{
		Data: dingoEvent.EpochTransitionEvent{
			PreviousEpoch: 5,
			NewEpoch:      6,
		},
	})

	require.Len(t, store.epochCandidates, 1)
	require.Equal(t, uint64(5), store.epochCandidates[0].Epoch)
	require.NotEmpty(t, store.epochCandidates[0].CandidatesCbor)
	var entries []candidateEntry
	require.NoError(t, fxcbor.Unmarshal(
		store.epochCandidates[0].CandidatesCbor,
		&entries,
	))
	require.Equal(t, []candidateEntry{{
		TxHash:      txHash2,
		OutputIndex: 0,
		Datum:       datum,
	}}, entries)

	// After the epoch transition the tracked epoch must be updated.
	idx.mu.Lock()
	require.Equal(t, uint64(6), idx.currentEpoch)
	idx.mu.Unlock()
}

// TestCandidateEmptySnapshot verifies that an epoch transition with no
// candidates produces an empty-array snapshot rather than being skipped.
func TestCandidateEmptySnapshot(t *testing.T) {
	t.Parallel()

	idx, store := newTestIndexer(t, Config{
		CommitteeCandidateAddress: testAddress,
	})

	idx.handleEpochTransition(dingoEvent.Event{
		Data: dingoEvent.EpochTransitionEvent{
			PreviousEpoch: 0,
			NewEpoch:      1,
		},
	})

	require.Len(t, store.epochCandidates, 1,
		"even with no candidates the snapshot row must be written")
	require.NotNil(t, store.epochCandidates[0].CandidatesCbor)
}

func TestCandidateSetHydratedOnStart(t *testing.T) {
	t.Parallel()

	eventBus := dingoEvent.NewEventBus(nil, testLogger())
	t.Cleanup(eventBus.Stop)
	idx, store := newTestIndexer(t, Config{
		EventBus:                  eventBus,
		CommitteeCandidateAddress: testAddress,
	})
	txHash := make([]byte, 32)
	txHash[0] = 0x42
	store.initialCandidates = []models.Utxo{{
		TxId:      txHash,
		OutputIdx: 7,
		Datum:     []byte{0x01},
	}}

	require.NoError(t, idx.Start(context.Background()))
	t.Cleanup(idx.Stop)

	idx.mu.Lock()
	defer idx.mu.Unlock()
	require.Equal(t, []byte{0x01}, idx.candidates[candidateKey{
		TxHash:      [32]byte{0x42},
		OutputIndex: 7,
	}])
}
