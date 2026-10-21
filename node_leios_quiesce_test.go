package dingo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuiesceClearsLeiosCertificateVerifier(t *testing.T) {
	t.Parallel()

	n, _ := newLiveLifecycleTestNode(t, 1)
	require.NoError(t, n.initLeiosVoteManager(context.Background()))
	require.NotNil(t, n.leiosCertificateVerifier.Load())

	require.NoError(t, n.quiesceForLiveLifecycleOp(context.Background()))
	require.Nil(t, n.leiosCertificateVerifier.Load(), "atomic pointer should be cleared after quiesce")
}
