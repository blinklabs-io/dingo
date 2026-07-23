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

package bark

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// freeTCPPort finds an OS-assigned free port by binding then immediately
// closing a listener. NewBark substitutes its own default (9091) for
// Port: 0, so this is how a test asks for "any free port" instead.
func freeTCPPort(t *testing.T) uint {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // always *net.TCPAddr for a "tcp" listener
	require.NoError(t, ln.Close())
	return uint(port)
}

// TestDatabaseServiceOverRealHTTP is the wire-level companion to
// database_test.go's in-process handler tests: those call
// databaseServiceHandler's methods directly, proving the job-tracking and
// Service-wiring logic but not that a real Connect client, talking to a
// really-listening bark.Bark server over real HTTP, gets back something
// the generated client can decode. This starts a real server and drives
// it with the generated databaseconnect.DatabaseServiceClient.
func TestDatabaseServiceOverRealHTTP(t *testing.T) {
	// bark's own DB (kept open for GetDatabaseInfo) and the Service's
	// target (opened and closed per call by Service.Snapshot/Truncate)
	// must be separate directories: Badger's exclusive file lock refuses
	// a second concurrent open of the same one, so a single shared
	// directory would fail CreateSnapshot/Truncate with "another process
	// is using this Badger database" the instant they ran.
	block1 := testBlock(1, 0x01)

	barkDataDir := t.TempDir()
	db := newDiskTestDB(t, barkDataDir)
	require.NoError(t, db.BlockCreate(block1, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block1.Slot, Hash: block1.Hash},
		BlockNumber: block1.Number,
	}, nil))

	svcDataDir := t.TempDir()
	svcDB := newDiskTestDB(t, svcDataDir)
	require.NoError(t, svcDB.BlockCreate(block1, nil))
	require.NoError(t, svcDB.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block1.Slot, Hash: block1.Hash},
		BlockNumber: block1.Number,
	}, nil))
	svcDB.Close()

	svc := dblifecycle.NewService(&config.Config{
		DatabasePath:   svcDataDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	}, nil)

	b, err := NewBark(BarkConfig{
		DB:          db,
		Lifecycle:   svc,
		SnapshotDir: t.TempDir(),
		Host:        "127.0.0.1",
		Port:        freeTCPPort(t),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, b.Start(ctx))
	defer func() { _ = b.Stop(context.Background()) }()
	require.NotEmpty(t, b.Addr())

	client := databaseconnect.NewDatabaseServiceClient(
		http.DefaultClient,
		"http://"+b.Addr(),
	)

	infoResp, err := client.GetDatabaseInfo(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetDatabaseInfoRequest{}),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(10), infoResp.Msg.GetTip().GetSlot())
	require.Equal(t, uint64(1), infoResp.Msg.GetTip().GetBlockNumber())
	require.False(t, infoResp.Msg.GetOperationInProgress())
	require.NotZero(t, infoResp.Msg.GetSizeBytes())

	createResp, err := client.CreateSnapshot(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.CreateSnapshotRequest{
			Name: "wire-test",
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, createResp.Msg.GetOperationId())
	require.NotEmpty(t, createResp.Msg.GetSnapshotId())

	var finalProgress *databasev1alpha1.OperationProgress
	require.Eventually(t, func() bool {
		statusResp, err := client.GetSnapshotStatus(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetSnapshotStatusRequest{
				OperationId: createResp.Msg.GetOperationId(),
			}),
		)
		require.NoError(t, err)
		finalProgress = statusResp.Msg.GetProgress()
		return finalProgress.GetStatus() ==
			databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED
	}, databaseOperationTimeout, 20*time.Millisecond,
		"snapshot must complete over the wire",
	)
	require.Equal(t, "completed", finalProgress.GetMessage())
	require.Equal(t, createResp.Msg.GetOperationId(), finalProgress.GetOperationId())

	// Truncate over the same real connection, sequenced after the
	// snapshot completes so the handler's single-operation gate doesn't
	// reject it.
	blockNumber := uint64(1)
	truncResp, err := client.Truncate(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.TruncateRequest{
			Target: &databasev1alpha1.BlockRef{BlockNumber: &blockNumber},
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, truncResp.Msg.GetOperationId())

	require.Eventually(t, func() bool {
		statusResp, err := client.GetTruncateStatus(
			context.Background(),
			connect.NewRequest(&databasev1alpha1.GetTruncateStatusRequest{
				OperationId: truncResp.Msg.GetOperationId(),
			}),
		)
		require.NoError(t, err)
		return statusResp.Msg.GetProgress().GetStatus() ==
			databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED
	}, databaseOperationTimeout, 20*time.Millisecond,
		"truncate must complete over the wire",
	)

	histResp, err := client.GetOperationHistory(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.GetOperationHistoryRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, histResp.Msg.GetRecords(), 2)

	// StreamOperationProgress is the one server-streaming RPC, which needs
	// a real transport to exercise (a unit test can't easily construct a
	// *connect.ServerStream by hand) — the truncate above already reached
	// COMPLETED, so the very first message the stream sends should report
	// that and the stream should then close.
	stream, err := client.StreamOperationProgress(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.StreamOperationProgressRequest{
			OperationId: truncResp.Msg.GetOperationId(),
		}),
	)
	require.NoError(t, err)
	var sawCompleted bool
	for stream.Receive() {
		if stream.Msg().GetProgress().GetStatus() ==
			databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED {
			sawCompleted = true
		}
	}
	require.NoError(t, stream.Err())
	require.True(t, sawCompleted, "stream must report the truncate's completed status before closing")

	listResp, err := client.ListSnapshots(
		context.Background(),
		connect.NewRequest(&databasev1alpha1.ListSnapshotsRequest{}),
	)
	require.NoError(t, err)
	require.Len(t, listResp.Msg.GetSnapshots(), 1)
	require.Equal(
		t,
		createResp.Msg.GetSnapshotId(),
		listResp.Msg.GetSnapshots()[0].GetSnapshotId(),
	)
}
