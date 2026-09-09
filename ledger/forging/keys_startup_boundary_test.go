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
			registered, matched, err := credentials.ValidateAgainstLedgerAtSlot(
				view, params, testCase.slot,
			)
			require.True(t, registered)
			require.True(t, matched)
			if testCase.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, testCase.wantError)
			}
		})
	}
}

func TestStartupOpCertCounterRequiresEraParameters(t *testing.T) {
	var typedNilProvider *mockPParamsProvider
	for _, testCase := range []struct {
		name      string
		provider  ProtocolParamsProvider
		wantError string
	}{
		{"missing provider", nil, "provider is nil"},
		{"typed nil provider", typedNilProvider, "provider is nil"},
		{
			"missing parameters",
			&mockPParamsProvider{},
			"parameters unavailable",
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
			credentials := newCredsForLedger(t)
			view := &fakeLedgerView{
				registered: true,
				regVRFHash: lcommon.Blake2b256Hash(credentials.vrfVKey),
				seqFound:   true,
			}
			_, _, err := credentials.ValidateAgainstLedgerAtSlot(
				view, testCase.provider, 100,
			)
			require.ErrorContains(t, err, testCase.wantError)
		})
	}
}
