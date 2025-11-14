// Copyright 2024 Blink Labs Software
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

package ouroboros

import (
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func BlockfetchServerConnOpts(
	requestRangeFunc blockfetch.RequestRangeFunc,
) []blockfetch.BlockFetchOptionFunc {
	return []blockfetch.BlockFetchOptionFunc{
		blockfetch.WithRequestRangeFunc(requestRangeFunc),
	}
}

func BlockfetchClientConnOpts(
	blockFunc blockfetch.BlockFunc,
	batchDoneFunc blockfetch.BatchDoneFunc,
) []blockfetch.BlockFetchOptionFunc {
	return []blockfetch.BlockFetchOptionFunc{
		blockfetch.WithBlockFunc(blockFunc),
		blockfetch.WithBatchDoneFunc(batchDoneFunc),
		blockfetch.WithBatchStartTimeout(2 * time.Second),
		blockfetch.WithBlockTimeout(2 * time.Second),
		// Set the recv queue size to 2x our block batch size
		blockfetch.WithRecvQueueSize(1000),
	}
}

func BlockfetchClientRequestRange(
	connManager *connmanager.ConnectionManager,
	connId ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	conn := connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	if err := conn.BlockFetch().Client.GetBlockRange(start, end); err != nil {
		return err
	}
	return nil
}
