package dingo

import (
	"context"
	"log/slog"
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

// verifierAtStopHandler records whether the certificate verifier was
// already cleared when the vote manager reported that it had stopped.
type verifierAtStopHandler struct {
	n       *Node
	cleared chan bool
}

func (h *verifierAtStopHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *verifierAtStopHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "leios vote manager stopped" {
		select {
		case h.cleared <- h.n.leiosCertificateVerifier.Load() == nil:
		default:
		}
	}
	return nil
}

func (h *verifierAtStopHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *verifierAtStopHandler) WithGroup(string) slog.Handler      { return h }

func TestQuiesceClearsLeiosCertificateVerifierBeforeStop(t *testing.T) {
	t.Parallel()

	n, _ := newLiveLifecycleTestNode(t, 1)
	h := &verifierAtStopHandler{n: n, cleared: make(chan bool, 1)}
	n.config.logger = slog.New(h)
	require.NoError(t, n.initLeiosVoteManager(context.Background()))

	require.NoError(t, n.quiesceForLiveLifecycleOp(context.Background()))
	select {
	case cleared := <-h.cleared:
		require.True(
			t,
			cleared,
			"verifier must be cleared before the vote manager stops",
		)
	default:
		t.Fatal("vote manager did not report stopping")
	}
}
