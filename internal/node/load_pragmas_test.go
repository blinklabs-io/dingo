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

package node

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

type bulkLoadOptimizerStub struct {
	setErr      error
	restoreErrs []error
	restoreCall int
}

func (s *bulkLoadOptimizerStub) SetBulkLoadPragmas() error {
	return s.setErr
}

func (s *bulkLoadOptimizerStub) RestoreNormalPragmas() error {
	s.restoreCall++
	if len(s.restoreErrs) == 0 {
		return nil
	}
	err := s.restoreErrs[0]
	s.restoreErrs = s.restoreErrs[1:]
	return err
}

var _ metadata.BulkLoadOptimizer = (*bulkLoadOptimizerStub)(nil)

func TestWithBulkLoadPragmasReturnsCleanupAfterSetupFailure(t *testing.T) {
	t.Parallel()

	setErr := errors.New("partial setup")
	optimizer := &bulkLoadOptimizerStub{setErr: setErr}
	cleanup := withBulkLoadPragmas(
		optimizer,
		slog.New(slog.DiscardHandler),
	)

	require.NoError(t, cleanup())
	require.NoError(t, cleanup())
	require.Equal(t, 1, optimizer.restoreCall)
}

func TestWithBulkLoadPragmasRetriesFailedRestore(t *testing.T) {
	t.Parallel()

	restoreErr := errors.New("restore failed")
	optimizer := &bulkLoadOptimizerStub{
		restoreErrs: []error{restoreErr, nil},
	}
	cleanup := withBulkLoadPragmas(
		optimizer,
		slog.New(slog.DiscardHandler),
	)

	require.ErrorIs(t, cleanup(), restoreErr)
	require.NoError(t, cleanup())
	require.NoError(t, cleanup())
	require.Equal(t, 2, optimizer.restoreCall)
}
