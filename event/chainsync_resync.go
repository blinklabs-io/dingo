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
	ChainsyncResyncReasonLocalTipPlateau                   = "local_tip_plateau"
	ChainsyncResyncReasonPostPlateauRealign                = "post_plateau_realign"
	ChainsyncResyncReasonRollbackAhead                     = "rollback point ahead of local tip"
	ChainsyncResyncReasonRollbackNotFound                  = "rollback point not found"
	ChainsyncResyncReasonRollbackLoop                      = "rollback loop detected"
	ChainsyncResyncReasonPersistentFork                    = "persistent chain fork"
	ChainsyncResyncReasonRollbackExceedsK                  = "rollback exceeds security parameter K"
	ChainsyncResyncReasonRollbackExceedsMithril            = "rollback exceeds Mithril trust boundary"
	ChainsyncResyncReasonPeerTipBehindMithril              = "peer tip behind Mithril trust boundary"
	ChainsyncResyncReasonRollbackBelowUtxoPruneFloor       = "rollback below consumed UTxO prune floor"
	ChainsyncResyncReasonForkResolutionExceedsK            = "fork resolution exceeds security parameter K"
	ChainsyncResyncReasonLocalLedgerRollback               = "local ledger rollback"
	ChainsyncResyncReasonLiveTxValidationRecovery          = "live tx validation recovery"
	ChainsyncResyncReasonDeterministicTxValidationRecovery = "deterministic tx validation recovery"
	ChainsyncResyncReasonReplayRecoveryNonConverging       = "replay tx validation recovery not converging"
	ChainsyncResyncReasonChainSwitchCursorAhead            = "chain switch cursor ahead of local tip"
	ChainsyncResyncReasonBlockfetchTimeoutRetryFailed      = "blockfetch timeout retry failed on all available connections"
	ChainsyncResyncReasonBlockfetchRangeUnavailable        = "blockfetch could not obtain the queued header range"
	ChainsyncResyncReasonHeaderValidationRecovery          = "deferred header validation recovery"
	// ChainsyncResyncReasonForecastCorroborationRecovery replays a verified
	// staged header after a peer waiting at the forecast horizon supplies the
	// independent Genesis witness it lacked. It is not a peer-fault signal.
	ChainsyncResyncReasonForecastCorroborationRecovery = "forecast corroboration recovery"
	// ChainsyncResyncReasonDeferredHeaderValidationFailure names the peer that
	// supplied a block whose deferred header validation failed with a verdict
	// the peer is responsible for. Unlike HeaderValidationRecovery, which
	// re-syncs every client, it targets only that peer's connection.
	ChainsyncResyncReasonDeferredHeaderValidationFailure = "deferred header validation failure"
	ChainsyncResyncReasonForkQueueOverflowRestartFailed  = "failed to restart blockfetch after fork-resolution header-queue overflow"
	ChainsyncResyncReasonHeaderQueueFullRestartFailed    = "failed to restart blockfetch for a full header queue"
	ChainsyncResyncReasonForkExtensionRestartFailed      = "failed to restart blockfetch after fork extension"
	// ChainsyncResyncReasonFutureHeaderAdmissionRecovery re-intersects the
	// ChainSync mini-protocol after a resolvable header was deliberately dropped
	// outside the permitted clock-skew window. It is not a peer-fault signal and
	// does not impose a peer cooldown.
	ChainsyncResyncReasonFutureHeaderAdmissionRecovery = "future header admission recovery"
	// ChainsyncResyncReasonDisconnectedHeaderAdmission re-intersects the
	// ChainSync mini-protocol after admission found a header past the forecast
	// horizon that does not build on the local chain. The held header cannot be
	// released by applying blocks the node already has, so the connection
	// restarts from fresh intersect points. It is not a peer-fault signal and
	// does not impose a peer cooldown.
	ChainsyncResyncReasonDisconnectedHeaderAdmission = "disconnected header admission"
	// ChainsyncResyncReasonGenesisCorroborationHoldExpired re-intersects a
	// connection whose header was held for Genesis corroboration longer than
	// the hold allows. The header was withheld, so the fresh intersection
	// delivers it again. It is not a peer-fault signal.
	ChainsyncResyncReasonGenesisCorroborationHoldExpired = "genesis corroboration hold expired"
	// ChainsyncResyncReasonForecastHorizonNoWork re-intersects a connected
	// ChainSync peer when no local queued or in-flight blockfetch can advance
	// the forecast horizon. It is not a peer-fault signal and does not impose
	// a peer cooldown.
	ChainsyncResyncReasonForecastHorizonNoWork = "forecast horizon has no local block work"
	// ChainsyncResyncReasonBehindLocalTip re-intersects an ingress-eligible
	// ChainSync connection whose delivered header trails the local tip by more
	// than the security parameter while the peer's advertised tip does not,
	// and only while chain selection has no best peer.
	// Its headers cannot extend the local chain, so the connection restarts
	// from fresh intersect points. It is not a peer-fault signal and does not
	// impose a peer cooldown.
	ChainsyncResyncReasonBehindLocalTip = "behind local tip"
	// ChainsyncResyncReasonPromotedObserverAhead re-intersects a connection
	// promoted from observability-only to ingress-eligible whose delivered
	// header is more than one block above the local tip. Its candidate
	// frontier would start above the ledger and could neither be corroborated
	// nor corroborate the header after the tip, so the connection restarts
	// from the local tip. It is not a peer-fault signal and does not impose a
	// peer cooldown.
	ChainsyncResyncReasonPromotedObserverAhead = "promoted observer ahead of local tip"
	// ChainsyncResyncReasonNetworkIngestReleased re-intersects every tracked
	// ChainSync client from the current chain tip when a network ingest hold
	// is released: while held, each client's cursor ran ahead of the chain
	// with its headers dropped. It carries no ConnectionId, is not a
	// peer-fault signal and does not impose a peer cooldown.
	ChainsyncResyncReasonNetworkIngestReleased = "network ingest released"
)

// ChainsyncResyncEvent carries the connection ID that should
// be re-synced.
type ChainsyncResyncEvent struct {
	ConnectionId ouroboros.ConnectionId
	Reason       string
	Point        ocommon.Point
}
