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

package plugins

import (
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func localStorageSelections() StorageSelections {
	return StorageSelections{
		Blob: plugin.Selection{
			Provider: "badger",
			Config:   map[string]any{},
		},
		Metadata: plugin.Selection{
			Provider: "sqlite",
			Config:   map[string]any{},
		},
	}
}

func TestOpenDatabaseReturnsRecoveryErrorOnRuntime(t *testing.T) {
	dataDir := t.TempDir()
	deps := StorageDependencies{DataDir: dataDir}
	runtime, err := OpenDatabase(
		context.Background(),
		&database.Config{DataDir: dataDir},
		localStorageSelections(),
		deps,
	)
	require.NoError(t, err)
	firstRuntime := runtime
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, firstRuntime.Close(ctx))
	})

	metaTxn := runtime.Database.Metadata().Transaction()
	require.NoError(
		t,
		runtime.Database.Metadata().SetCommitTimestamp(123456789, metaTxn),
	)
	require.NoError(t, metaTxn.Commit())
	require.NoError(t, runtime.Close(context.Background()))

	runtime, err = OpenDatabase(
		context.Background(),
		&database.Config{DataDir: dataDir},
		localStorageSelections(),
		deps,
	)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	t.Cleanup(func() {
		require.NoError(t, runtime.Close(context.Background()))
	})
	var timestampErr database.CommitTimestampError
	require.ErrorAs(t, runtime.RecoveryError(), &timestampErr)
	require.Equal(t, int64(123456789), timestampErr.MetadataTimestamp)
	require.Zero(t, timestampErr.BlobTimestamp)
}

func TestOpenDatabaseErrorDoesNotReturnLiveRuntime(t *testing.T) {
	selections := localStorageSelections()
	selections.Metadata.Provider = "missing"
	runtime, err := OpenDatabase(
		context.Background(),
		&database.Config{DataDir: t.TempDir()},
		selections,
		StorageDependencies{DataDir: t.TempDir()},
	)
	require.Error(t, err)
	require.Nil(t, runtime)
	require.ErrorContains(
		t,
		err,
		"plugin provider not found: storage.metadata/missing",
	)
}
