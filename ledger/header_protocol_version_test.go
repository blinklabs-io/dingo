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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shelleyGenesisJSON returns a minimal Shelley genesis JSON document
// carrying the network identity fields used by tests. Both networkId
// ("Mainnet" or "Testnet") and networkMagic are populated so tests can
// independently exercise either selector.
func shelleyGenesisJSON(networkId string, magic uint32) string {
	return fmt.Sprintf(`{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z",
		"networkId": %q,
		"networkMagic": %d
	}`, networkId, magic)
}

func newLedgerStateForNetwork(t *testing.T, networkId string, magic uint32) *LedgerState {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON(networkId, magic)),
	))
	return &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

// shelleyHeaderWithMajor builds a Shelley-family header (covers Shelley,
// Allegra, Mary, Alonzo) with the given protocol major version.
func shelleyHeaderWithMajor(t *testing.T, major uint64) *shelley.ShelleyBlockHeader {
	t.Helper()
	return &shelley.ShelleyBlockHeader{
		Body: shelley.ShelleyBlockHeaderBody{
			ProtoMajorVersion: major,
		},
	}
}

// babbageHeaderWithMajor builds a Babbage-family header (covers Babbage,
// Conway) with the given protocol major version.
func babbageHeaderWithMajor(t *testing.T, major uint64) *babbage.BabbageBlockHeader {
	t.Helper()
	return &babbage.BabbageBlockHeader{
		Body: babbage.BabbageBlockHeaderBody{
			ProtoVersion: babbage.BabbageProtoVersion{
				Major: major,
			},
		},
	}
}

// dijkstraHeaderWithMajor builds a Dijkstra header (a distinct concrete
// type that embeds a Babbage header) with the given protocol major
// version.
func dijkstraHeaderWithMajor(t *testing.T, major uint64) *dijkstra.DijkstraBlockHeader {
	t.Helper()
	return &dijkstra.DijkstraBlockHeader{
		BabbageBlockHeader: *babbageHeaderWithMajor(t, major),
	}
}

func TestHeaderProtocolMajor_Shelley(t *testing.T) {
	got, ok := HeaderProtocolMajor(shelleyHeaderWithMajor(t, 2))
	require.True(t, ok)
	assert.Equal(t, uint(2), got)
}

func TestHeaderProtocolMajor_Allegra(t *testing.T) {
	h := &allegra.AllegraBlockHeader{
		ShelleyBlockHeader: *shelleyHeaderWithMajor(t, 3),
	}
	got, ok := HeaderProtocolMajor(h)
	require.True(t, ok)
	assert.Equal(t, uint(3), got)
}

func TestHeaderProtocolMajor_Mary(t *testing.T) {
	h := &mary.MaryBlockHeader{
		ShelleyBlockHeader: *shelleyHeaderWithMajor(t, 4),
	}
	got, ok := HeaderProtocolMajor(h)
	require.True(t, ok)
	assert.Equal(t, uint(4), got)
}

func TestHeaderProtocolMajor_Alonzo(t *testing.T) {
	h := &alonzo.AlonzoBlockHeader{
		ShelleyBlockHeader: *shelleyHeaderWithMajor(t, 6),
	}
	got, ok := HeaderProtocolMajor(h)
	require.True(t, ok)
	assert.Equal(t, uint(6), got)
}

func TestHeaderProtocolMajor_Babbage(t *testing.T) {
	got, ok := HeaderProtocolMajor(babbageHeaderWithMajor(t, 8))
	require.True(t, ok)
	assert.Equal(t, uint(8), got)
}

func TestHeaderProtocolMajor_Conway(t *testing.T) {
	h := &conway.ConwayBlockHeader{
		BabbageBlockHeader: *babbageHeaderWithMajor(t, 10),
	}
	got, ok := HeaderProtocolMajor(h)
	require.True(t, ok)
	assert.Equal(t, uint(10), got)
}

func TestHeaderProtocolMajor_Dijkstra(t *testing.T) {
	// Dijkstra headers are a distinct concrete type from Babbage/Conway.
	// Without an explicit case the type switch falls through to default
	// (ok=false), which disables the "too high" check for Dijkstra blocks.
	got, ok := HeaderProtocolMajor(dijkstraHeaderWithMajor(t, 12))
	require.True(t, ok)
	assert.Equal(t, uint(12), got)
}

func TestHeaderProtocolMajor_Byron(t *testing.T) {
	// Byron headers do not carry a ProtVer in the Praos sense, so the
	// extractor reports the absence rather than fabricating a value.
	h := &byron.ByronMainBlockHeader{}
	_, ok := HeaderProtocolMajor(h)
	assert.False(t, ok)
}

func TestValidateHeaderProtocolVersion_MainnetEqual(t *testing.T) {
	// Header pvMajor == current pvMajor is always accepted.
	err := ValidateHeaderProtocolVersion(
		babbageHeaderWithMajor(t, 10),
		10,
		true,
	)
	require.NoError(t, err)
}

func TestValidateHeaderProtocolVersion_MainnetOneAhead(t *testing.T) {
	// Header pvMajor == current pvMajor + 1 is accepted (block producer
	// is one version ahead, ready to hard fork).
	err := ValidateHeaderProtocolVersion(
		babbageHeaderWithMajor(t, 11),
		10,
		true,
	)
	require.NoError(t, err)
}

func TestValidateHeaderProtocolVersion_MainnetTooHigh(t *testing.T) {
	// Header pvMajor more than one ahead of current is rejected on mainnet.
	err := ValidateHeaderProtocolVersion(
		babbageHeaderWithMajor(t, 12),
		10,
		true,
	)
	require.Error(t, err)
	var typed *HeaderProtocolVersionTooHighError
	require.True(t, errors.As(err, &typed))
	assert.Equal(t, uint(12), typed.Supplied)
	assert.Equal(t, uint(11), typed.Expected)
}

func TestValidateHeaderProtocolVersion_TestnetTooHighPreDijkstra(t *testing.T) {
	// On testnets, while pre-Dijkstra (current major < 12), a header
	// with arbitrarily high pvMajor is accepted. This is the relaxation
	// from cardano-ledger PR 5785.
	err := ValidateHeaderProtocolVersion(
		babbageHeaderWithMajor(t, 99),
		10,
		false,
	)
	require.NoError(t, err)
}

func TestValidateHeaderProtocolVersion_TestnetTooHighAtDijkstra(t *testing.T) {
	// On testnets, once current pvMajor reaches Dijkstra (12), the check
	// becomes mainnet-equivalent: header more than one ahead is rejected.
	err := ValidateHeaderProtocolVersion(
		babbageHeaderWithMajor(t, 14),
		12,
		false,
	)
	require.Error(t, err)
	var typed *HeaderProtocolVersionTooHighError
	require.True(t, errors.As(err, &typed))
	assert.Equal(t, uint(14), typed.Supplied)
	assert.Equal(t, uint(13), typed.Expected)
}

func TestValidateHeaderProtocolVersion_TestnetOneAheadAtDijkstra(t *testing.T) {
	// At Dijkstra on a testnet, header == current+1 is still accepted.
	err := ValidateHeaderProtocolVersion(
		babbageHeaderWithMajor(t, 13),
		12,
		false,
	)
	require.NoError(t, err)
}

func TestValidateHeaderProtocolVersion_DijkstraTooHigh(t *testing.T) {
	// Regression: a Dijkstra-era header more than one ahead of current
	// pparams must be rejected. Before HeaderProtocolMajor had an explicit
	// Dijkstra case, this header fell through to default (ok=false) and the
	// BBODY "HeaderProtVerTooHigh" check was silently skipped, letting a
	// malicious peer's header with an arbitrarily high protocol major
	// version pass on a node running the Dijkstra era.
	err := ValidateHeaderProtocolVersion(
		dijkstraHeaderWithMajor(t, 99),
		12,
		true,
	)
	require.Error(t, err)
	var typed *HeaderProtocolVersionTooHighError
	require.True(t, errors.As(err, &typed))
	assert.Equal(t, uint(99), typed.Supplied)
	assert.Equal(t, uint(13), typed.Expected)
}

func TestValidateHeaderProtocolVersion_DijkstraOneAhead(t *testing.T) {
	// A Dijkstra header exactly one ahead of current is still accepted.
	err := ValidateHeaderProtocolVersion(
		dijkstraHeaderWithMajor(t, 13),
		12,
		true,
	)
	require.NoError(t, err)
}

func TestValidateHeaderProtocolVersion_Byron(t *testing.T) {
	// Byron headers are skipped entirely - they do not carry a ProtVer
	// in the Praos sense.
	err := ValidateHeaderProtocolVersion(
		&byron.ByronMainBlockHeader{},
		10,
		true,
	)
	require.NoError(t, err)
}

func TestHeaderProtocolVersionTooHighError_Message(t *testing.T) {
	err := &HeaderProtocolVersionTooHighError{
		Supplied: 12,
		Expected: 11,
	}
	// Message should mention both values so operators can diagnose.
	msg := err.Error()
	assert.Contains(t, msg, "12")
	assert.Contains(t, msg, "11")
}

// Sanity check: the Block interface accessor (Header()) round-trips
// through HeaderProtocolMajor for the typical case where callers pass
// a full block via block.Header().
func TestHeaderProtocolMajor_ViaBlockHeader(t *testing.T) {
	var h lcommon.BlockHeader = babbageHeaderWithMajor(t, 8)
	got, ok := HeaderProtocolMajor(h)
	require.True(t, ok)
	assert.Equal(t, uint(8), got)
}

func TestLedgerStateIsMainnet_True(t *testing.T) {
	ls := newLedgerStateForNetwork(t, "Mainnet", byron.MainnetProtocolMagic)
	got, err := ls.isMainnet()
	require.NoError(t, err)
	assert.True(t, got)
}

func TestLedgerStateIsMainnet_FalseOnPreprod(t *testing.T) {
	// Preprod genesis uses networkId=Testnet, magic=1.
	ls := newLedgerStateForNetwork(t, "Testnet", 1)
	got, err := ls.isMainnet()
	require.NoError(t, err)
	assert.False(t, got)
}

func TestLedgerStateIsMainnet_FalseOnDevnet(t *testing.T) {
	// Devnet genesis uses networkId=Testnet, magic=42.
	ls := newLedgerStateForNetwork(t, "Testnet", 42)
	got, err := ls.isMainnet()
	require.NoError(t, err)
	assert.False(t, got)
}

// TestLedgerStateIsMainnet_FailClosedWhenConfigMissing pins the
// fail-closed contract: when CardanoNodeConfig is absent, isMainnet
// must return an error rather than silently picking a default. The
// caller is expected to surface this as a validation failure so blocks
// don't slip past the header-version check on a partially initialized
// state.
func TestLedgerStateIsMainnet_FailClosedWhenConfigMissing(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	_, err := ls.isMainnet()
	require.Error(t, err)
}

// TestLedgerStateIsMainnet_FailClosedWhenGenesisMissing is the same
// fail-closed guarantee for the case where CardanoNodeConfig is
// present but Shelley genesis has not been loaded.
func TestLedgerStateIsMainnet_FailClosedWhenGenesisMissing(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: &cardano.CardanoNodeConfig{},
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	_, err := ls.isMainnet()
	require.Error(t, err)
}

// TestLedgerStateIsMainnet_FailClosedOnUnknownNetworkId guards against
// a Shelley genesis with a networkId field that's neither "Mainnet"
// nor "Testnet" (typo, future enum value, corrupted file). We refuse
// to guess in either direction.
func TestLedgerStateIsMainnet_FailClosedOnUnknownNetworkId(t *testing.T) {
	ls := newLedgerStateForNetwork(t, "Bogus", 42)
	_, err := ls.isMainnet()
	require.Error(t, err)
}

// TestLedgerStateIsMainnet_NetworkIdNotMagic pins the discriminator to
// shelley-genesis networkId rather than networkMagic. A configuration
// claiming networkId=Testnet must be treated as testnet even if it
// happens to share mainnet's magic value, and a Mainnet-tagged genesis
// must be honored even with a non-canonical magic. This is the literal
// port of cardano-ledger's `netId == Mainnet` predicate.
func TestLedgerStateIsMainnet_NetworkIdNotMagic(t *testing.T) {
	t.Run("Testnet wins over mainnet magic", func(t *testing.T) {
		ls := newLedgerStateForNetwork(
			t, "Testnet", byron.MainnetProtocolMagic,
		)
		got, err := ls.isMainnet()
		require.NoError(t, err)
		assert.False(t, got)
	})
	t.Run("Mainnet wins over non-canonical magic", func(t *testing.T) {
		ls := newLedgerStateForNetwork(t, "Mainnet", 999)
		got, err := ls.isMainnet()
		require.NoError(t, err)
		assert.True(t, got)
	})
}

// TestLedgerStateValidateBlockHeaderProtocolVersion_FailClosedOnMissingConfig
// confirms the fail-closed contract reaches the wiring layer:
// validateBlockHeaderProtocolVersion must return an error rather than
// validating against an undeterminable network identity.
func TestLedgerStateValidateBlockHeaderProtocolVersion_FailClosedOnMissingConfig(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
		},
	}
	header := &conway.ConwayBlockHeader{
		BabbageBlockHeader: *babbageHeaderWithMajor(t, 10),
	}
	err := ls.validateBlockHeaderProtocolVersion(header, pp)
	require.Error(t, err)
}

func TestLedgerStateValidateBlockHeaderProtocolVersion_MainnetRejects(t *testing.T) {
	ls := newLedgerStateForNetwork(t, "Mainnet", byron.MainnetProtocolMagic)
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
			Minor: 0,
		},
	}
	header := &conway.ConwayBlockHeader{
		BabbageBlockHeader: *babbageHeaderWithMajor(t, 12),
	}
	err := ls.validateBlockHeaderProtocolVersion(header, pp)
	require.Error(t, err)
	var typed *HeaderProtocolVersionTooHighError
	require.True(t, errors.As(err, &typed))
}

func TestLedgerStateValidateBlockHeaderProtocolVersion_DijkstraRejects(t *testing.T) {
	// End-to-end: a node in the Dijkstra era (pparams major 12) on mainnet
	// must reject a Dijkstra header whose protocol major is more than one
	// ahead, exercising the GetProtocolVersion Dijkstra case, isMainnet,
	// and the HeaderProtocolMajor Dijkstra case together.
	ls := newLedgerStateForNetwork(t, "Mainnet", byron.MainnetProtocolMagic)
	pp := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 12,
				Minor: 0,
			},
		},
	}
	header := dijkstraHeaderWithMajor(t, 99)
	err := ls.validateBlockHeaderProtocolVersion(header, pp)
	require.Error(t, err)
	var typed *HeaderProtocolVersionTooHighError
	require.True(t, errors.As(err, &typed))
}

func TestLedgerStateValidateBlockHeaderProtocolVersion_TestnetAllows(t *testing.T) {
	ls := newLedgerStateForNetwork(t, "Testnet", 42)
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
			Minor: 0,
		},
	}
	header := &conway.ConwayBlockHeader{
		BabbageBlockHeader: *babbageHeaderWithMajor(t, 99),
	}
	require.NoError(t, ls.validateBlockHeaderProtocolVersion(header, pp))
}
