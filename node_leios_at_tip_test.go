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

package dingo

import (
	"testing"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/stretchr/testify/require"
)

// The ledger announces a committed apply with an announcing block at the tip
// through LeiosAnnouncementAtTip; the node must turn that into an endorser
// block validation retry, the only retry signal published after the commit.
func TestLedgerStateConfigLeiosAnnouncementAtTipRequestsRetry(t *testing.T) {
	t.Parallel()

	n := &Node{config: Config{cfg: &internalconfig.Config{}}}
	cfg := n.ledgerStateConfig()
	require.NotNil(t, cfg.LeiosAnnouncementAtTip)

	// Before networking exists there is nothing to retry and no panic.
	require.NotPanics(t, func() { cfg.LeiosAnnouncementAtTip(nil) })

	o := &ouroborosPkg.Ouroboros{}
	n.ouroborosRef.Store(o)
	before := o.LeiosValidationRetryRequestsForTesting()
	cfg.LeiosAnnouncementAtTip(nil)
	require.Equal(
		t,
		before+1,
		o.LeiosValidationRetryRequestsForTesting(),
		"at-tip notification must request an endorser block validation retry",
	)
}
