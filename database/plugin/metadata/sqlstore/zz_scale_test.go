package sqlstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestZZScale(t *testing.T) {
	for _, n := range []int{100_000, 400_000, 1_200_000} {
		store := newMigratedSQLiteStore(t)
		seedStart := time.Now()
		seedRewardLiveStakeScaleFixture(t, store, n, 3, 80, n*4)
		fmt.Printf("SCALE n=%d seed=%s\n", n, time.Since(seedStart))
		start := time.Now()
		require.NoError(t, store.RebuildRewardLiveStakeFromRunningTotals(9_000_000, nil))
		fmt.Printf("SCALE n=%d total=%s\n", n, time.Since(start))
	}
}
