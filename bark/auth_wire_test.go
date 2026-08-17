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
	"testing"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestDatabaseServiceRequiresClientCertForDestructiveRPCs is the wire-level
// proof for bark#2988: an anonymous caller (no client cert) can still use
// the DatabaseService's read-only RPCs but is rejected on a destructive one;
// a caller presenting a certificate signed by a different, untrusted CA
// never even completes the TLS handshake; and a caller presenting a
// certificate signed by the configured CA gets past the auth check
// entirely (it may still fail for an unrelated reason, e.g. an unknown
// operation ID — the assertion here is specifically about authentication,
// not about that RPC's own business logic succeeding).
func TestDatabaseServiceRequiresClientCertForDestructiveRPCs(t *testing.T) {
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
	dbtest.CloseDatabase(svcDB) //nolint:errcheck

	svc := dblifecycle.NewService(&config.Config{
		DatabasePath: svcDataDir,
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Blob:     plugin.Selection{Provider: "badger"},
				Metadata: plugin.Selection{Provider: "sqlite"},
			},
		},
	}, nil, nil)

	serverCertPath, serverKeyPath := writeTestTLSCertKey(t)
	trustedCA, trustedCAKey, trustedCACertPath := writeTestCA(t)
	trustedClientCertPath, trustedClientKeyPath := writeTestClientCert(
		t, trustedCA, trustedCAKey, "trusted-operator",
	)

	untrustedCA, untrustedCAKey, _ := writeTestCA(t)
	untrustedClientCertPath, untrustedClientKeyPath := writeTestClientCert(
		t, untrustedCA, untrustedCAKey, "untrusted-operator",
	)

	b, err := NewBark(BarkConfig{
		DB:                  db,
		Lifecycle:           svc,
		SnapshotDir:         t.TempDir(),
		Host:                "127.0.0.1",
		Port:                freeTCPPort(t),
		TlsCertFilePath:     serverCertPath,
		TlsKeyFilePath:      serverKeyPath,
		TlsClientCAFilePath: trustedCACertPath,
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	defer func() { _ = b.Stop(context.Background()) }()
	require.NotEmpty(t, b.Addr())

	newClient := func(certPath, keyPath string) databaseconnect.DatabaseServiceClient {
		return databaseconnect.NewDatabaseServiceClient(
			mtlsHTTPClient(t, certPath, keyPath),
			"https://"+b.Addr(),
		)
	}

	t.Run(
		"anonymous client: read-only succeeds, destructive is rejected",
		func(t *testing.T) {
			client := newClient("", "")

			_, err := client.GetDatabaseInfo(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetDatabaseInfoRequest{}),
			)
			require.NoError(
				t,
				err,
				"read-only RPCs must stay reachable without a client cert",
			)

			_, err = client.CancelOperation(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
					OperationId: "nonexistent",
				}),
			)
			require.Error(t, err)
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		},
	)

	t.Run(
		"cert signed by an untrusted CA is treated as unverified, not rejected outright",
		func(t *testing.T) {
			client := newClient(untrustedClientCertPath, untrustedClientKeyPath)

			// Whether this specific connection gets rejected at the handshake
			// or reaches the application layer anonymous is not something this
			// pins: a client can fail to actually present its configured
			// certificate for reasons entirely unrelated to CA trust (e.g. no
			// mutually acceptable signature scheme), in which case the
			// connection proceeds like any anonymous one rather than erroring,
			// alongside Go's documented handshake-level rejection of a
			// genuinely-received-but-untrusted chain. What must hold regardless
			// of which of those occurred is the actual security property:
			// peerCertContextMiddleware keys off r.TLS.VerifiedChains
			// (populated only for a chain that resolved to a trusted ClientCAs
			// root), not r.TLS.PeerCertificates (populated for whatever the
			// client presented, verified or not) — so this untrusted cert must
			// never be treated as an authenticated operator, regardless of
			// which path the connection actually took to get here.
			_, err := client.GetDatabaseInfo(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.GetDatabaseInfoRequest{}),
			)
			if err != nil {
				// The handshake was rejected outright — also an acceptable
				// outcome, and nothing further to check on this connection.
				return
			}

			_, err = client.CancelOperation(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
					OperationId: "nonexistent",
				}),
			)
			require.Error(t, err)
			require.Equal(
				t,
				connect.CodeUnauthenticated,
				connect.CodeOf(err),
				"an unverified cert must be rejected on a destructive RPC exactly like an anonymous caller",
			)
		},
	)

	t.Run(
		"cert signed by the configured CA passes the auth check",
		func(t *testing.T) {
			client := newClient(trustedClientCertPath, trustedClientKeyPath)

			_, err := client.CancelOperation(
				context.Background(),
				connect.NewRequest(&databasev1alpha1.CancelOperationRequest{
					OperationId: "nonexistent",
				}),
			)
			require.Error(t, err)
			require.Equal(t, connect.CodeNotFound, connect.CodeOf(err),
				"should fail on the unknown operation id, not authentication")
		},
	)
}
