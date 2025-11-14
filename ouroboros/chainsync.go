package ouroboros

import (
	"fmt"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	chainsyncIntersectPointCount = 100
)

func ChainsyncServerConnOpts(
	findIntersectFunc ochainsync.FindIntersectFunc,
	requestNextFunc ochainsync.RequestNextFunc,
) []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithFindIntersectFunc(findIntersectFunc),
		ochainsync.WithRequestNextFunc(requestNextFunc),
	}
}

func ChainsyncClientConnOpts(
	rollForwardFunc ochainsync.RollForwardFunc,
	rollBackwardFunc ochainsync.RollBackwardFunc,
) []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithRollForwardFunc(rollForwardFunc),
		ochainsync.WithRollBackwardFunc(rollBackwardFunc),
		// Enable pipelining of RequestNext messages to speed up chainsync
		ochainsync.WithPipelineLimit(50),
		// Set the recv queue size to 2x our pipeline limit
		ochainsync.WithRecvQueueSize(100),
	}
}

func ChainsyncClientStart(
	connManager *connmanager.ConnectionManager,
	ledgerState *ledger.LedgerState,
	intersectTip bool,
	intersectPoints []ocommon.Point,
	connId ConnectionId,
) error {
	conn := connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	points, err := ledgerState.RecentChainPoints(
		chainsyncIntersectPointCount,
	)
	if err != nil {
		return err
	}
	// Determine start point if we have no stored chain points
	if len(points) == 0 {
		if intersectTip {
			// Start initial chainsync from current chain tip
			tip, err := conn.ChainSync().Client.GetCurrentTip()
			if err != nil {
				return err
			}
			points = append(
				points,
				tip.Point,
			)
			return conn.ChainSync().Client.Sync(points)
		} else if len(intersectPoints) > 0 {
			// Start initial chainsync at specific point(s)
			points = append(
				points,
				intersectPoints...,
			)
		}
	}
	return conn.ChainSync().Client.Sync(points)
}
