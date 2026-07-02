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

package event

import (
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ChainsyncResyncEventType is the event type emitted when a
// chainsync re-sync is required (e.g. persistent fork detected
// or rollback exceeds the security parameter).
const ChainsyncResyncEventType = EventType("chainsync.resync")

const (
	ChainsyncResyncReasonLocalTipPlateau              = "local_tip_plateau"
	ChainsyncResyncReasonPostPlateauRealign           = "post_plateau_realign"
	ChainsyncResyncReasonRollbackAhead                = "rollback point ahead of local tip"
	ChainsyncResyncReasonRollbackNotFound             = "rollback point not found"
	ChainsyncResyncReasonRollbackLoop                 = "rollback loop detected"
	ChainsyncResyncReasonPersistentFork               = "persistent chain fork"
	ChainsyncResyncReasonRollbackExceedsK             = "rollback exceeds security parameter K"
	ChainsyncResyncReasonRollbackExceedsMithril       = "rollback exceeds Mithril trust boundary"
	ChainsyncResyncReasonPeerTipBehindMithril         = "peer tip behind Mithril trust boundary"
	ChainsyncResyncReasonForkResolutionExceedsK       = "fork resolution exceeds security parameter K"
	ChainsyncResyncReasonLocalLedgerRollback          = "local ledger rollback"
	ChainsyncResyncReasonLiveTxValidationRecovery     = "live tx validation recovery"
	ChainsyncResyncReasonChainSwitchCursorAhead       = "chain switch cursor ahead of local tip"
	ChainsyncResyncReasonBlockfetchTimeoutRetryFailed = "blockfetch timeout retry failed on all available connections"
)

// ChainsyncResyncEvent carries the connection ID that should
// be re-synced.
type ChainsyncResyncEvent struct {
	ConnectionId ouroboros.ConnectionId
	Reason       string
	Point        ocommon.Point
}
