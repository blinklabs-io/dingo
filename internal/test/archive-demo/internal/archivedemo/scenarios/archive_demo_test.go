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

//go:build archive_demo

package scenarios

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blinklabs-io/dingo/internal/test/archive-demo/internal/archivedemo"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// TestArchiveProxy exercises the full archive-node + history-expiry-node + bark
// proxy story end to end:
//
//  1. wait for chain to advance well past the security window on both nodes
//  2. pick a target block well behind the security window
//  3. assertion 1: BlockFetch on dingo-pruning succeeds and CBOR is non-empty
//  4. assertion 3: target object exists in Minio bucket "dingo-archive"
//  5. stop dingo-pruning, run inspect-blob against the bind-mounted Badger
//     dir, assert block is NOT present locally (assertion 2)
func TestArchiveProxy(t *testing.T) {
	archive, pruning := archivedemo.DefaultEndpoints()

	// Step 1: wait for both nodes to advance past the stability window.
	// Dingo derives the expiry window from LedgerState.StabilityWindow(),
	// which is 3k/f. With testnet.yaml's k=40 and f=0.4 that's 300 slots.
	// We wait for tip >= 400 to give History Expiry some cycles past the window.
	const stabilityWindow = uint64(300) // 3 * k(40) / f(0.4)
	const targetTip = uint64(400)
	_, err := archivedemo.WaitForSlot(archive, archivedemo.DefaultNetworkMagic, targetTip, 10*time.Minute, t.Logf)
	require.NoError(t, err)
	_, err = archivedemo.WaitForSlot(pruning, archivedemo.DefaultNetworkMagic, targetTip, 10*time.Minute, t.Logf)
	require.NoError(t, err)

	// Step 1b: poll until History Expiry has had time to act on candidateSlot.
	// inspect-blob can't run while the container holds the badger lockfile,
	// so we wait until the expiry tick frequency (5s) has had several
	// cycles past candidateSlot+stabilityWindow.
	const candidateSlot = uint64(50)
	require.Eventually(t, func() bool {
		tip, err := archivedemo.GetTip(pruning, archivedemo.DefaultNetworkMagic)
		if err != nil {
			return false
		}
		return tip.Slot >= candidateSlot+stabilityWindow+30
	}, 3*time.Minute, 5*time.Second, "chain did not advance far enough for history expiry to act")

	// Step 2: resolve (slot, hash) of the first block at or after candidateSlot
	// using dingo-archive, which has the full chain.
	point, err := archivedemo.FindBlockAtOrAfterSlot(
		archive, archivedemo.DefaultNetworkMagic, candidateSlot, 60*time.Second, t.Logf,
	)
	require.NoError(t, err)
	t.Logf("target block: slot=%d hash=%x", point.Slot, point.Hash)
	require.GreaterOrEqual(t, point.Slot, candidateSlot)

	// Step 3 (assertion 1): BlockFetch the target on dingo-pruning succeeds.
	cbor, err := archivedemo.FetchBlock(
		pruning, archivedemo.DefaultNetworkMagic, point, 60*time.Second,
	)
	require.NoError(t, err, "BlockFetch on history-expiry node should transparently proxy via bark")
	require.NotEmpty(t, cbor, "block CBOR should be non-empty")

	// Step 4 (assertion 3): object present in Minio bucket.
	s3Client := newMinioClient(t)
	key := blobKeyForBlock(point.Slot, point.Hash)
	_, err = s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("dingo-archive"),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "block %q should be present in Minio bucket", key)

	// Step 5 (assertion 2): stop dingo-pruning, run inspect-blob.
	// We deliberately do not restart it: dingo currently fails to
	// re-initialize a persisted database (foreign-key error recreating
	// genesis). The whole stack is torn down by run-tests.sh anyway.
	stopPruning(t)

	inspectBin := os.Getenv("ARCHIVEDEMO_INSPECT_BIN")
	require.NotEmpty(t, inspectBin, "ARCHIVEDEMO_INSPECT_BIN must be set by run-tests.sh")
	pruningDir := os.Getenv("ARCHIVEDEMO_PRUNING_DATA_DIR")
	require.NotEmpty(t, pruningDir, "ARCHIVEDEMO_PRUNING_DATA_DIR must be set")

	inspectCtx, inspectCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer inspectCancel()
	cmd := exec.CommandContext(inspectCtx, inspectBin,
		"-dir", pruningDir,
		"-slot", fmt.Sprintf("%d", point.Slot),
		"-hash", hex.EncodeToString(point.Hash),
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	runErr := cmd.Run()
	t.Logf("inspect-blob output:\n%s", out.String())
	if errors.Is(inspectCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("inspect-blob timed out: %s", out.String())
	}

	// Exit 0 = present, 1 = absent. We require absent.
	var exitErr *exec.ExitError
	require.Error(t, runErr, "block should be ABSENT from local Badger; inspect-blob exited 0 (present)")
	require.True(t, errors.As(runErr, &exitErr), "expected ExitError, got %T", runErr)
	require.Equal(t, 1, exitErr.ExitCode(),
		"inspect-blob should return 1 (absent), got %d", exitErr.ExitCode())
}

func newMinioClient(t *testing.T) *s3.Client {
	t.Helper()
	endpoint := os.Getenv("ARCHIVEDEMO_MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9100"
	}
	cfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("demo", "demodemo", ""),
		),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

func stopPruning(t *testing.T) {
	t.Helper()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer stopCancel()
	cmd := exec.CommandContext(stopCtx, "docker", "compose",
		"-f", composeFile(t),
		"stop", "dingo-pruning",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "docker compose stop dingo-pruning: %s", out)

	pruningDir := os.Getenv("ARCHIVEDEMO_PRUNING_DATA_DIR")
	require.NotEmpty(t, pruningDir, "ARCHIVEDEMO_PRUNING_DATA_DIR must be set")

	// The dingo container runs as uid 100, so files it created in the
	// bind-mounted data dir aren't readable to the host user that will
	// run inspect-blob or stat Badger's lock file. Spin a one-shot
	// busybox container as root to open the perms back up.
	chmodCtx, chmodCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer chmodCancel()
	chmod := exec.CommandContext(chmodCtx, "docker", "run", "--rm",
		"-v", pruningDir+":/data",
		"alpine:latest",
		"sh", "-c", "chmod -R a+rwX /data",
	)
	chmodOut, err := chmod.CombinedOutput()
	require.NoError(t, err, "chmod bind mount: %s", chmodOut)

	// Wait for badger to release its lock file rather than sleeping a
	// fixed interval.
	lockFile := filepath.Join(pruningDir, "blob", "LOCK")
	testutil.WaitForCondition(t, func() bool {
		_, statErr := os.Stat(lockFile)
		return os.IsNotExist(statErr)
	}, 30*time.Second, "badger lock file was not released after stopping dingo-pruning")
}

func composeFile(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel").Output()
	require.NoError(t, err, "git rev-parse")
	root := strings.TrimSpace(string(out))
	return filepath.Join(root, "internal", "test", "archive-demo", "docker-compose.yml")
}

// blobKeyForBlock returns the S3 object key the AWS blob plugin uses for
// (slot, hash). Matches database/types/keys.go BlockBlobKey + S3 plugin
// fullKey hex-encoding (database/plugin/blob/aws/database.go fullKey).
func blobKeyForBlock(slot uint64, hash []byte) string {
	var slotBytes [8]byte
	binary.BigEndian.PutUint64(slotBytes[:], slot)
	raw := append([]byte("bp"), slotBytes[:]...)
	raw = append(raw, hash...)
	return hex.EncodeToString(raw)
}
