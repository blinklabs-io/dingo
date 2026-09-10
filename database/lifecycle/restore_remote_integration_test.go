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

package lifecycle_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/aws"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/postgres"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

const remoteIntegrationBlobProvider = "s3-restore-integration"

type remoteIntegrationConfig struct {
	Endpoint string `yaml:"endpoint"`
	Bucket   string `yaml:"bucket"`
	Region   string `yaml:"region"`
	Prefix   string `yaml:"prefix"`
}

type remoteIntegrationControl struct {
	failRestores atomic.Int32
	resetCalls   atomic.Int32
}

type remoteIntegrationBlobStore struct {
	*aws.BlobStoreS3
	control *remoteIntegrationControl
}

func (s *remoteIntegrationBlobStore) Reset(ctx context.Context) error {
	s.control.resetCalls.Add(1)
	return s.BlobStoreS3.Reset(ctx)
}

func (s *remoteIntegrationBlobStore) Restore(
	ctx context.Context,
	r io.Reader,
) error {
	for {
		remaining := s.control.failRestores.Load()
		if remaining == 0 {
			return s.BlobStoreS3.Restore(ctx, r)
		}
		if s.control.failRestores.CompareAndSwap(remaining, remaining-1) {
			// Fail only after the replacement is durably loaded so the test
			// exercises compensation of a mutated remote target, not merely
			// an error returned before Restore performs its first write.
			if err := s.BlobStoreS3.Restore(ctx, r); err != nil {
				return err
			}
			return errInjectedRemoteBlobRestore
		}
	}
}

func registerRemoteIntegrationBlobProvider(
	host *plugin.Host,
	control *remoteIntegrationControl,
) error {
	return plugin.Register[blob.BlobStore](
		host,
		plugin.Descriptor{
			Capability: plugin.CapabilityStorageBlob,
			Name:       remoteIntegrationBlobProvider,
		},
		func() remoteIntegrationConfig { return remoteIntegrationConfig{} },
		func(
			_ context.Context,
			cfg remoteIntegrationConfig,
			_ blob.ProviderDependencies,
		) (blob.BlobStore, plugin.Instance, error) {
			store, err := aws.NewWithOptions(
				aws.WithEndpoint(cfg.Endpoint),
				aws.WithBucket(cfg.Bucket),
				aws.WithRegion(cfg.Region),
				aws.WithPrefix(cfg.Prefix),
			)
			if err != nil {
				return nil, nil, err
			}
			wrapped := &remoteIntegrationBlobStore{
				BlobStoreS3: store,
				control:     control,
			}
			return wrapped, plugin.Lifecycle{
				StartFunc: func(context.Context) error { return wrapped.Start() },
				StopFunc:  func(context.Context) error { return wrapped.Stop() },
			}, nil
		},
	)
}

type remoteIntegrationEnvironment struct {
	postgresDSN string
	endpoint    string
	bucket      string
	region      string
}

func requireRemoteIntegrationEnvironment(
	t *testing.T,
) remoteIntegrationEnvironment {
	t.Helper()
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" ||
		os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Skip("S3 credentials not configured")
	}
	bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("DINGO_TEST_S3_BUCKET not configured")
	}

	postgresDSN := os.Getenv("DINGO_POSTGRES_DSN")
	if postgresDSN == "" {
		password := os.Getenv("POSTGRES_PASSWORD")
		if password == "" {
			t.Skip("PostgreSQL credentials not configured")
		}
		host := os.Getenv("POSTGRES_HOST")
		if host == "" {
			host = "127.0.0.1"
		}
		port := os.Getenv("POSTGRES_PORT")
		if port == "" {
			port = "5432"
		}
		user := os.Getenv("POSTGRES_USER")
		if user == "" {
			user = "postgres"
		}
		databaseName := os.Getenv("POSTGRES_DATABASE")
		if databaseName == "" {
			databaseName = "dingo_test"
		}
		postgresDSN = (&url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(user, password),
			Host:   host + ":" + port,
			Path:   databaseName,
			RawQuery: url.Values{
				"sslmode": []string{"disable"},
			}.Encode(),
		}).String()
	}
	_, err := exec.LookPath("pg_dump")
	require.NoError(t, err, "remote restore integration requires pg_dump")
	_, err = exec.LookPath("pg_restore")
	require.NoError(t, err, "remote restore integration requires pg_restore")

	admin, err := sql.Open("pgx", postgresDSN)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	require.NoError(t, admin.Close())
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	return remoteIntegrationEnvironment{
		postgresDSN: postgresDSN,
		endpoint:    os.Getenv("AWS_ENDPOINT"),
		bucket:      bucket,
		region:      region,
	}
}

func createRemoteIntegrationDatabase(
	t *testing.T,
	adminDSN string,
) string {
	t.Helper()
	parsed, err := url.Parse(adminDSN)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		parsed.Scheme,
		"PostgreSQL integration DSN must be a URI",
	)
	admin, err := sql.Open("pgx", adminDSN)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	databaseName := "restore_3268_" + strconv.FormatInt(
		time.Now().UnixNano(),
		10,
	)
	_, err = admin.ExecContext(
		t.Context(), `CREATE DATABASE "`+databaseName+`"`,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.ExecContext(
			context.Background(),
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity `+
				`WHERE datname = $1 AND pid <> pg_backend_pid()`,
			databaseName,
		)
		_, dropErr := admin.ExecContext(
			context.Background(), `DROP DATABASE "`+databaseName+`"`,
		)
		require.NoError(t, dropErr)
		require.NoError(t, admin.Close())
	})
	parsed.Path = "/" + databaseName
	return parsed.String()
}

func remoteIntegrationBlobConfig(
	env remoteIntegrationEnvironment,
	prefix string,
) map[string]any {
	return map[string]any{
		"endpoint": env.endpoint,
		"bucket":   env.bucket,
		"region":   env.region,
		"prefix":   prefix,
	}
}

func cleanupRemoteIntegrationPrefix(
	t *testing.T,
	env remoteIntegrationEnvironment,
	prefix string,
) {
	t.Helper()
	t.Cleanup(func() {
		store, err := aws.NewWithOptions(
			aws.WithEndpoint(env.endpoint),
			aws.WithBucket(env.bucket),
			aws.WithRegion(env.region),
			aws.WithPrefix(prefix),
		)
		require.NoError(t, err)
		require.NoError(t, store.Start())
		require.NoError(t, store.Reset(context.Background()))
		require.NoError(t, store.Stop())
	})
}

type remoteIntegrationDatabase struct {
	db   *database.Database
	host *plugin.Host
}

func openRemoteIntegrationDatabase(
	t *testing.T,
	env remoteIntegrationEnvironment,
	dsn string,
	prefix string,
	network string,
	control *remoteIntegrationControl,
) *remoteIntegrationDatabase {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, postgres.RegisterProvider(host))
	require.NoError(t, registerRemoteIntegrationBlobProvider(host, control))
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		t.Context(),
		host,
		plugin.CapabilityStorageMetadata,
		"postgres",
		map[string]any{"dsn": dsn},
		metadata.ProviderDependencies{StorageMode: types.StorageModeCore},
	)
	require.NoError(t, err)
	blobStore, err := plugin.Resolve[blob.BlobStore](
		t.Context(),
		host,
		plugin.CapabilityStorageBlob,
		remoteIntegrationBlobProvider,
		remoteIntegrationBlobConfig(env, prefix),
		blob.ProviderDependencies{StorageMode: types.StorageModeCore},
	)
	require.NoError(t, err)
	db, err := database.New(
		&database.Config{
			DataDir:        t.TempDir(),
			StorageMode:    types.StorageModeCore,
			Network:        network,
			BlobPlugin:     remoteIntegrationBlobProvider,
			MetadataPlugin: "postgres",
		},
		database.Stores{Blob: blobStore, Metadata: metadataStore},
	)
	if err != nil {
		_ = host.Stop(context.Background())
	}
	require.NoError(t, err)
	return &remoteIntegrationDatabase{db: db, host: host}
}

func (d *remoteIntegrationDatabase) close(t *testing.T) {
	t.Helper()
	require.NoError(t, d.db.Close())
	require.NoError(t, d.host.Stop(context.Background()))
}

type exactRemoteIntegrationState struct {
	tip             ochainsync.Tip
	commitTimestamp int64
	settings        *types.NodeSettings
	gates           map[string]string
	syncMarker      string
	blobs           map[string][]byte
}

func captureRemoteIntegrationState(
	t *testing.T,
	db *database.Database,
) exactRemoteIntegrationState {
	t.Helper()
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	commitTimestamp, err := db.Metadata().GetCommitTimestamp()
	require.NoError(t, err)
	settings, err := db.Metadata().GetNodeSettings()
	require.NoError(t, err)
	gates, err := db.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	syncMarker, err := db.GetSyncState("integration-marker", nil)
	require.NoError(t, err)
	return exactRemoteIntegrationState{
		tip:             tip,
		commitTimestamp: commitTimestamp,
		settings:        settings,
		gates:           gates,
		syncMarker:      syncMarker,
		blobs:           readBlobContents(t, db),
	}
}

func populateRemoteIntegrationDatabase(
	t *testing.T,
	db *database.Database,
	hashByte byte,
	marker string,
) {
	t.Helper()
	txn := db.Transaction(true)
	defer txn.Rollback() //nolint:errcheck
	block := testBlock(1, hashByte)
	require.NoError(t, db.BlockCreate(block, txn))
	require.NoError(t, db.Blob().Set(
		txn.Blob(), []byte("integration-marker"), []byte(marker),
	))
	require.NoError(t, db.SetSyncState("integration-marker", marker, txn))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block.Slot, Hash: block.Hash},
		BlockNumber: block.Number,
	}, txn))
	require.NoError(t, txn.Commit())
}

func copyRemoteIntegrationSnapshot(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "snapshot")
	require.NoError(t, os.MkdirAll(dst, 0o700))
	for _, name := range []string{
		lifecycle.ManifestFileName,
		lifecycle.MetadataBackupFileName,
		lifecycle.BlobBackupFileName,
	} {
		data, err := os.ReadFile(filepath.Join(src, name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, name), data, 0o600))
	}
	return dst
}

func corruptRemoteIntegrationBlobBackup(
	t *testing.T,
	snapshotDir string,
	truncate bool,
) {
	t.Helper()
	path := filepath.Join(snapshotDir, lifecycle.BlobBackupFileName)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(data), 16)
	if truncate {
		data = data[:len(data)-1]
	} else {
		data[len(data)-1] ^= 0xff
	}
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

func TestLiveRemoteRestorePostgresS3PreservesOrSwitchesBothStores(
	t *testing.T,
) {
	env := requireRemoteIntegrationEnvironment(t)
	unique := strconv.FormatInt(time.Now().UnixNano(), 10)
	incomingDSN := createRemoteIntegrationDatabase(t, env.postgresDSN)
	incomingPrefix := fmt.Sprintf("restore-3268/%s/incoming/", unique)
	cleanupRemoteIntegrationPrefix(t, env, incomingPrefix)
	incomingControl := &remoteIntegrationControl{}
	incoming := openRemoteIntegrationDatabase(
		t, env, incomingDSN, incomingPrefix, "preview", incomingControl,
	)
	populateRemoteIntegrationDatabase(t, incoming.db, 0x99, "incoming")
	incomingState := captureRemoteIntegrationState(t, incoming.db)
	snapshotDir := filepath.Join(t.TempDir(), "incoming-snapshot")
	_, err := lifecycle.Snapshot(
		t.Context(),
		incoming.db,
		snapshotDir,
		lifecycle.TriggerManual,
		"test",
		remoteIntegrationBlobProvider,
		"postgres",
	)
	require.NoError(t, err)
	incoming.close(t)

	testCases := []struct {
		name              string
		prepare           func(*testing.T, string)
		validate          func(lifecycle.Manifest) error
		failRestores      int32
		wantError         string
		wantPreflightOnly bool
		wantReset         bool
		wantIncoming      bool
	}{
		{
			name: "wrong network",
			validate: func(m lifecycle.Manifest) error {
				return m.CheckCompatibility(
					remoteIntegrationBlobProvider,
					"postgres",
					types.StorageModeCore,
					"mainnet",
					m.Gates,
				)
			},
			wantError:         "manifest network",
			wantPreflightOnly: true,
		},
		{
			name: "truncated blob",
			prepare: func(t *testing.T, dir string) {
				corruptRemoteIntegrationBlobBackup(t, dir, true)
			},
			wantError:         "unexpected EOF",
			wantPreflightOnly: true,
		},
		{
			name: "blob checksum mismatch",
			prepare: func(t *testing.T, dir string) {
				corruptRemoteIntegrationBlobBackup(t, dir, false)
			},
			wantError:         "corrupted",
			wantPreflightOnly: true,
		},
		{
			name:         "blob restore failure",
			failRestores: 1,
			wantError:    errInjectedRemoteBlobRestore.Error(),
			wantReset:    true,
		},
		{name: "success", wantIncoming: true},
	}

	for i, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			destinationDSN := createRemoteIntegrationDatabase(
				t,
				env.postgresDSN,
			)
			destinationPrefix := fmt.Sprintf(
				"restore-3268/%s/destination-%d/", unique, i,
			)
			cleanupRemoteIntegrationPrefix(t, env, destinationPrefix)
			control := &remoteIntegrationControl{}
			original := openRemoteIntegrationDatabase(
				t, env, destinationDSN, destinationPrefix, "mainnet", control,
			)
			populateRemoteIntegrationDatabase(t, original.db, 0x11, "original")
			originalState := captureRemoteIntegrationState(t, original.db)
			original.close(t)

			candidate := copyRemoteIntegrationSnapshot(t, snapshotDir)
			if tt.prepare != nil {
				tt.prepare(t, candidate)
			}
			control.failRestores.Store(tt.failRestores)
			restoreHost := plugin.NewHost()
			require.NoError(t, postgres.RegisterProvider(restoreHost))
			require.NoError(t, registerRemoteIntegrationBlobProvider(
				restoreHost, control,
			))
			_, restoreErr := lifecycle.RestoreValidated(
				metadata.AllowResetOfPopulatedTarget(t.Context()),
				restoreHost,
				nil,
				candidate,
				filepath.Join(t.TempDir(), "local-target"),
				tt.validate,
				lifecycle.RestoreStorageConfig{
					Blob: remoteIntegrationBlobConfig(env, destinationPrefix),
					Metadata: map[string]any{
						"dsn": destinationDSN,
					},
				},
			)
			require.NoError(t, restoreHost.Stop(context.Background()))
			if tt.wantError != "" {
				require.Error(t, restoreErr)
				require.Contains(t, restoreErr.Error(), tt.wantError)
			} else {
				require.NoError(t, restoreErr)
			}
			if tt.wantPreflightOnly {
				require.Zero(t, control.resetCalls.Load())
			}
			if tt.wantReset {
				require.NotZero(t, control.resetCalls.Load())
			}

			network := "mainnet"
			wantState := originalState
			if tt.wantIncoming {
				network = "preview"
				wantState = incomingState
			}
			verified := openRemoteIntegrationDatabase(
				t, env, destinationDSN, destinationPrefix, network, control,
			)
			require.Equal(t, wantState, captureRemoteIntegrationState(
				t, verified.db,
			))
			verified.close(t)
		})
	}
}
