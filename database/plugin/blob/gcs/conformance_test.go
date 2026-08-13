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

//go:build dingo_extra_plugins

package gcs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// hasGCSCredentials mirrors internal/integration/cloud_test.go's check of the
// same name so this suite skips/runs under the same conditions. No GCS
// emulator exists in this repo (unlike S3's MinIO), so this test never runs
// in CI today -- it skips cleanly and is exercised manually against a real
// bucket with Application Default Credentials configured.
func hasGCSCredentials() bool {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return true
	}
	home := os.Getenv("HOME")
	if home != "" {
		adcPath := filepath.Join(
			home,
			".config",
			"gcloud",
			"application_default_credentials.json",
		)
		if _, err := os.Stat(adcPath); err == nil {
			return true
		}
	}
	return false
}

func TestBlobStoreConformance(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	storagetest.RunBlobStoreConformance(t, func(t *testing.T) blob.BlobStore {
		return newTestGCSStore(t)
	})
}

func TestBlobStoreResourceCleanup(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}

	// Deliberately not newTestGCSStore: that defers Stop via t.Cleanup,
	// which only fires once at the end of the whole test, but this check's
	// entire point is that each of the 5 cycles fully stops before the next
	// one starts (see AssertRepeatedLifecycleIsSafe's doc comment) --
	// t.Cleanup timing would silently turn that into "construct 5, then
	// stop all 5," never exercising Stop-then-reopen at all.
	storagetest.AssertRepeatedLifecycleIsSafe(t, 5, func(t *testing.T) {
		store, err := NewWithOptions(WithBucket(bucket))
		require.NoError(t, err)
		require.NoError(t, store.Start())
		txn := store.NewTransaction(true)
		require.NoError(t, store.Set(txn, []byte("k"), []byte("v")))
		require.NoError(t, txn.Commit())
		require.NoError(t, store.Stop())
	})
}

// TestBlobStoreBadCredentialsFailsCleanly needs no real credentials or
// network access: unlike the AWS SDK (which only builds a client in Start
// and defers all validation to first use, see
// aws.TestBlobStoreUnreachableEndpointFailsWithoutHanging), GCS's
// storage.NewGRPCClient loads and parses the credentials file eagerly, so
// pointing GOOGLE_APPLICATION_CREDENTIALS at a nonexistent file makes Start
// itself fail immediately. t.Setenv scopes the override to this test only.
func TestBlobStoreBadCredentialsFailsCleanly(t *testing.T) {
	t.Setenv(
		"GOOGLE_APPLICATION_CREDENTIALS",
		filepath.Join(t.TempDir(), "nonexistent-credentials.json"),
	)
	store, err := NewWithOptions(WithBucket("dingo-test"))
	require.NoError(t, err)
	require.Error(t, store.Start())
}

// newTestGCSStore constructs a store against the same bucket every other
// test in this file uses.
func newTestGCSStore(t *testing.T) *BlobStoreGCS {
	t.Helper()
	bucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}
	store, err := NewWithOptions(WithBucket(bucket))
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() {
		require.NoError(t, store.Stop())
	})
	return store
}

// TestBlobStoreGetBlockURLSignsCommittedBlock exercises the happy path no
// existing test in this package covers: a committed block's GetBlockURL
// returns a usable, parseable, non-expired presigned URL and the block's
// metadata. Note this needs signing capability beyond plain bucket access
// (a service account key, or IAM SignBlob permission for ADC
// impersonation) -- a plain user-account ADC that hasGCSCredentials accepts
// for the rest of this file's tests may not have it, in which case this
// fails with "gcs: failed to sign URL" rather than skipping.
func TestBlobStoreGetBlockURLSignsCommittedBlock(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	store := newTestGCSStore(t)
	slot := uint64(500)
	hash := []byte("block-url-committed")

	writeTxn := store.NewTransaction(true)
	require.NoError(t, store.SetBlock(
		writeTxn, slot, hash, []byte{0x82, 0x01, 0x02}, 1, 0, 7, nil,
	))
	require.NoError(t, writeTxn.Commit())

	readTxn := store.NewTransaction(false)
	defer func() { require.NoError(t, readTxn.Rollback()) }()
	signed, meta, err := store.GetBlockURL(
		t.Context(),
		readTxn,
		ocommon.Point{Slot: slot, Hash: hash},
	)
	require.NoError(t, err)
	require.NotEmpty(t, signed.URL.String())
	require.True(t, signed.Expires.After(time.Now()))
	require.Equal(t, uint64(7), meta.Height)
}

// TestBlobStoreGetBlockURLRejectsStagedUncommittedBlock exercises the
// documented contract in DATABASE.md's Cross-Store Durability Contract: "a
// block staged but not yet committed is reported as not found rather than
// signed into a URL that would 404." The staging check itself runs before
// any network call, but constructing a real GCS client still needs valid
// credentials, so this is gated the same as every other test here.
func TestBlobStoreGetBlockURLRejectsStagedUncommittedBlock(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	store := newTestGCSStore(t)
	slot := uint64(600)
	hash := []byte("block-url-staged")

	txn := store.NewTransaction(true)
	defer func() { require.NoError(t, txn.Rollback()) }()
	require.NoError(t, store.SetBlock(
		txn, slot, hash, []byte{0x82, 0x03, 0x04}, 2, 0, 8, nil,
	))

	_, _, err := store.GetBlockURL(
		t.Context(),
		txn,
		ocommon.Point{Slot: slot, Hash: hash},
	)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
}
