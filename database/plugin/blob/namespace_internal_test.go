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

package blob

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteNamespaceReportsPublishedMarkerAfterSyncFailure(t *testing.T) {
	wantErr := errors.New("injected namespace sync failure")
	originalSync := syncNamespaceDir
	syncNamespaceDir = func(string) error { return wantErr }
	t.Cleanup(func() { syncNamespaceDir = originalSync })

	dir := t.TempDir()
	namespace := "0123456789abcdef0123456789abcdef"
	err := WriteNamespace(dir, namespace)
	require.ErrorIs(t, err, wantErr)
	require.True(t, NamespacePublished(err))
	got, readErr := ReadNamespace(dir)
	require.NoError(t, readErr)
	require.Equal(t, namespace, got)
}
