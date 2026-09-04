package koiosparity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCountSignificantExcludesInformational pins that the number reported with
// a parity failure counts the mismatches that caused it.
//
// DetermineStatus deliberately treats the lifecycle and pool-departure
// categories as no-ops, so an epoch can hold many of them and still pass.
// Counting them in the failure message points the reader at rows that are by
// definition never the reason. Preview epoch 198 failed on 3 mismatches and
// reported 12.
func TestCountSignificantExcludesInformational(t *testing.T) {
	mismatches := []CheckMismatch{
		{Category: CategoryAcctOnlyDingo},
		{Category: CategoryAcctOnlyDingo},
		{Category: CategoryAcctOnlyDingo},
		{Category: CategoryAcctZeroReward},
	}
	for range 8 {
		mismatches = append(
			mismatches,
			CheckMismatch{Category: CategoryPoolDeparted},
		)
	}
	require.Len(t, mismatches, 12)
	assert.Equal(t, 3, CountSignificant(mismatches))
	assert.Equal(t, StatusFail, DetermineStatus(mismatches))
}

// TestCountSignificantCountsErrors keeps the error categories significant:
// they drive StatusError, so they are a reason too.
func TestCountSignificantCountsErrors(t *testing.T) {
	mismatches := []CheckMismatch{
		{Category: CategoryDBError},
		{Category: CategoryReferenceLag},
		{Category: CategoryPoolDeparted},
	}
	assert.Equal(t, 2, CountSignificant(mismatches))
	assert.Equal(t, StatusError, DetermineStatus(mismatches))
}

// TestCountSignificantAgreesWithDetermineStatus is the invariant that matters
// more than either number: a status of PASS and a non-zero significant count
// cannot coexist, in either direction. The two must read the same
// classification, or a future category added to one will silently disagree
// with the other.
func TestCountSignificantAgreesWithDetermineStatus(t *testing.T) {
	for _, cat := range []string{
		CategoryDBError,
		CategoryDBMissing,
		CategoryReferenceLag,
		CategoryAcctCoverageIncomplete,
		CategoryAcctZeroReward,
		CategoryAcctNewlyRegistered,
		CategoryAcctDeregistered,
		CategoryPoolDeparted,
		CategoryAcctOnlyDingo,
		CategoryAcctOnlyKoios,
		CategoryAcctDuplicate,
	} {
		t.Run(cat, func(t *testing.T) {
			ms := []CheckMismatch{{Category: cat}}
			passed := DetermineStatus(ms) == StatusPass
			assert.Equal(t, passed, CountSignificant(ms) == 0,
				"status and significant count must classify %q the same way",
				cat)
		})
	}
}
