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

package lifecycle

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

const cancelOnRestoreBlobProviderName = "cancel-on-restore-blob"

// cancelOnRestoreBlobStore embeds an unopened Badger blob store purely to
// satisfy the full blob.BlobStore method set plugin.Resolve's type
// assertion requires -- restoreBlobStore itself never calls any of those
// promoted methods, only Restore (overridden below) and the Restorer/
// Resettable type assertions.
//
// Its own stop method reproduces BlobStoreBadger.CloseContext's exact
// documented contract (see database/plugin/blob/badger/database.go):
// returning is not the completion signal, because a caller-supplied ctx
// can race the real close and return first. Reproducing that contract
// here, rather than depending on Badger's own on-disk directory lock
// timing, isolates what this test proves -- restoreBlobStore's own
// context handling -- from Badger's OS-level lock behavior, which
// database/plugin/blob/badger's own tests (provider_test.go's
// TestProviderStopDeadlineDuringValueLogGC) already cover.
type cancelOnRestoreBlobStore struct {
	*badger.BlobStoreBadger
	cancelRestore context.CancelFunc
	stopEntered   chan struct{}
	release       chan struct{}
	stopFinished  atomic.Bool
}

func (s *cancelOnRestoreBlobStore) Restore(
	ctx context.Context,
	_ io.Reader,
) error {
	s.cancelRestore()
	return ctx.Err()
}

// stop races ctx against a completion signal gated on s.release, exactly
// as CloseContext races ctx against its own closeDone channel.
func (s *cancelOnRestoreBlobStore) stop(ctx context.Context) error {
	close(s.stopEntered)
	done := make(chan struct{})
	go func() {
		<-s.release
		s.stopFinished.Store(true)
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newCancelOnRestoreTestHost(
	t *testing.T,
	store *cancelOnRestoreBlobStore,
) *plugin.Host {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, plugin.Register[blob.BlobStore](
		host,
		plugin.Descriptor{
			Capability: plugin.CapabilityStorageBlob,
			Name:       cancelOnRestoreBlobProviderName,
		},
		func() struct{} { return struct{}{} },
		func(
			_ context.Context,
			_ struct{},
			_ blob.ProviderDependencies,
		) (blob.BlobStore, plugin.Instance, error) {
			return store, plugin.Lifecycle{
				StartFunc: func(context.Context) error { return nil },
				StopFunc:  store.stop,
			}, nil
		},
	))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return host
}

// TestRestoreBlobStoreStopWaitsForProviderAfterCanceledRestore is a
// regression test for the restore-rollback lock race in dingo#4179's
// class: restoreBlobStore's own StopCapability call must not surface as
// "the provider is stopped" before the provider's Stop has genuinely
// finished, even when Restore failed because the operation's own context
// was just canceled -- the ordinary case, since Restore and the cleanup
// Stop that follows it share the same ctx.
//
// If StopCapability races that canceled ctx instead of waiting for the
// real completion, an automatic rollback (restoreRollback.restore) or a
// caller retrying the same restore against the same host can reopen
// targetDataDir while the prior store's close is still in flight and
// lose the race for its directory lock -- restore_remote_test.go's
// TestRestoreFailureRollsBackPopulatedRemoteStoresExactly/
// cancellation_after_reset is the user-visible "automatic restore
// rollback failed ... Cannot acquire directory lock" symptom this
// produces for a provider whose Stop propagates ctx like the real
// on-disk "badger" plugin does (badger.RegisterProvider wires
// StopFunc: store.CloseContext directly, unlike that test's own
// remoteTestBlobStore, whose Stop always uses context.Background and so
// cannot observe this race).
func TestRestoreBlobStoreStopWaitsForProviderAfterCanceledRestore(t *testing.T) {
	backupPath := filepath.Join(t.TempDir(), "blob.backup")
	require.NoError(t, os.WriteFile(backupPath, nil, 0o600))
	targetDir := t.TempDir()

	embedded, err := badger.New(badger.WithDeferOpen())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelOnRestoreBlobStore{
		BlobStoreBadger: embedded,
		cancelRestore:   cancel,
		stopEntered:     make(chan struct{}),
		release:         make(chan struct{}),
	}
	host := newCancelOnRestoreTestHost(t, store)
	manifest := Manifest{BlobPlugin: cancelOnRestoreBlobProviderName}

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- restoreBlobStore(
			ctx, host, manifest, backupPath, targetDir, nil, false,
		)
	}()

	testutil.RequireReceive(
		t, store.stopEntered, 5*time.Second,
		"restoreBlobStore never reached the post-Restore StopCapability call",
	)

	// restoreBlobStore must not return yet: its StopCapability call is
	// required to wait for the provider's own Stop to actually finish,
	// not race the operation's already-canceled context. 200ms only
	// needs to distinguish "returned without waiting at all" (the bug,
	// which resolves near-instantly since ctx.Done() is already ready)
	// from "genuinely still blocked" -- it is not a narrow race window.
	testutil.RequireNoReceive(
		t, resultCh, 200*time.Millisecond,
		"restoreBlobStore returned before the blob provider finished "+
			"stopping -- a subsequent reopen of the same directory can "+
			"race the still-in-flight close and hit "+
			"\"Cannot acquire directory lock\"",
	)
	require.False(t, store.stopFinished.Load())

	close(store.release)
	err = testutil.RequireReceive(
		t, resultCh, 5*time.Second,
		"restoreBlobStore did not return after the provider finished stopping",
	)
	require.True(
		t, store.stopFinished.Load(),
		"blob provider must be fully stopped by the time restoreBlobStore returns",
	)
	require.ErrorContains(t, err, "context canceled")
}
