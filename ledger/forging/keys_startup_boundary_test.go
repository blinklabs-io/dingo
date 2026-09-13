package forging

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type startupBoundaryParams struct {
	mockPParamsProvider
}

func (provider *startupBoundaryParams) ProtocolParamsForSlot(
	slot uint64,
) lcommon.ProtocolParameters {
	if slot >= 100 {
		return &babbage.BabbageProtocolParameters{}
	}
	return provider.pparams
}

func TestStartupOpCertCounterAtEraBoundary(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		slot      uint64
		counter   uint64
		wantError string
	}{
		{"tpraos stale", 99, 4, "below last seen"},
		{"tpraos equal", 99, 5, ""},
		{"tpraos next", 99, 6, ""},
		{"tpraos gap", 99, 7, ""},
		{"praos stale", 100, 4, "below last seen"},
		{"praos equal", 100, 5, ""},
		{"praos next", 100, 6, ""},
		{"praos gap at boundary", 100, 7, "skips ahead"},
		{"praos gap after boundary", 101, 7, "skips ahead"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			credentials := newCredsForLedger(t)
			credentials.opCert.IssueNumber = testCase.counter
			view := &fakeLedgerView{
				registered: true,
				regVRFHash: lcommon.Blake2b256Hash(credentials.vrfVKey),
				seqFound:   true,
				latestSeq:  5,
			}
			params := &startupBoundaryParams{
				mockPParamsProvider: mockPParamsProvider{
					pparams: &alonzo.AlonzoProtocolParameters{},
				},
			}
			result, err := credentials.ValidateAgainstLedgerAtSlot(
				view, params, testCase.slot,
			)
			require.True(t, result.Registered)
			require.True(t, result.VRFMatched)
			require.NoError(t, result.EraUnevaluable)
			if testCase.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, testCase.wantError)
			}
		})
	}
}

// funcParamsProvider is a nil-able non-pointer kind implementing
// ProtocolParamsProvider, so the typed-nil guard is exercised beyond
// reflect.Pointer. Calling it while nil panics, which is what the guard
// prevents.
type funcParamsProvider func(slot uint64) lcommon.ProtocolParameters

func (provider funcParamsProvider) GetCurrentPParams() lcommon.ProtocolParameters {
	return provider(0)
}

func (provider funcParamsProvider) ProtocolParamsForSlot(
	slot uint64,
) lcommon.ProtocolParameters {
	return provider(slot)
}

// TestStartupOpCertCounterUnresolvedEraDoesNotRefuse covers the era contexts a
// startup check can fail to resolve. None of them is a counter violation, so
// none may refuse: the staleness rule stays in force, the no-gap rule is
// reported as unevaluated, and the forge loop applies it per leader slot once
// the node is near the tip.
func TestStartupOpCertCounterUnresolvedEraDoesNotRefuse(t *testing.T) {
	var typedNilProvider *mockPParamsProvider
	for _, testCase := range []struct {
		name       string
		provider   ProtocolParamsProvider
		wantReason string
	}{
		{"missing provider", nil, "no protocol parameters provider"},
		{"typed nil provider", typedNilProvider, "no protocol parameters provider"},
		{
			"typed nil func provider",
			funcParamsProvider(nil),
			"no protocol parameters provider",
		},
		{
			"missing parameters",
			&mockPParamsProvider{},
			"parameters unavailable for slot 100",
		},
		{
			"typed nil parameters",
			&mockPParamsProvider{
				pparams: (*babbage.BabbageProtocolParameters)(nil),
			},
			"nil *babbage.BabbageProtocolParameters pointer",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			credentials := newCredsForLedger(t)
			// Seven against an observed five is a gap Praos refuses. The
			// era here is unknown, so the gap rule is not evaluable and
			// startup must proceed anyway.
			credentials.opCert.IssueNumber = 7
			view := &fakeLedgerView{
				registered: true,
				regVRFHash: lcommon.Blake2b256Hash(credentials.vrfVKey),
				seqFound:   true,
				latestSeq:  5,
			}
			result, err := credentials.ValidateAgainstLedgerAtSlot(
				view, testCase.provider, 100,
			)
			require.NoError(t, err)
			require.True(t, result.Registered)
			require.ErrorIs(t, result.EraUnevaluable, ErrOpCertEraUnevaluable)
			require.ErrorContains(
				t,
				result.EraUnevaluable,
				testCase.wantReason,
			)
		})
	}
}

// TestStartupOpCertCounterUnresolvedEraStillRejectsStaleCounter pins the half
// of the rule an unresolved era does not excuse. A counter below the observed
// on-chain value is a stale or stolen hot key whatever the era, so it must
// still refuse startup.
func TestStartupOpCertCounterUnresolvedEraStillRejectsStaleCounter(t *testing.T) {
	t.Parallel()
	credentials := newCredsForLedger(t)
	credentials.opCert.IssueNumber = 4
	view := &fakeLedgerView{
		registered: true,
		regVRFHash: lcommon.Blake2b256Hash(credentials.vrfVKey),
		seqFound:   true,
		latestSeq:  5,
	}
	result, err := credentials.ValidateAgainstLedgerAtSlot(view, nil, 100)
	require.ErrorContains(t, err, "below last seen")
	require.ErrorIs(t, result.EraUnevaluable, ErrOpCertEraUnevaluable)
}
