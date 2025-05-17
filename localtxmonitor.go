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

package dingo

import (
	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
)

const (
	localtxmonitorMempoolCapacity = 10 * 1024 * 1024 // TODO: replace with configurable value (#400)
)

func (n *Node) localtxmonitorServerConnOpts() []olocaltxmonitor.LocalTxMonitorOptionFunc {
	return []olocaltxmonitor.LocalTxMonitorOptionFunc{
		olocaltxmonitor.WithGetMempoolFunc(n.localtxmonitorServerGetMempool),
	}
}

func (n *Node) localtxmonitorServerGetMempool(
	ctx olocaltxmonitor.CallbackContext,
) (uint64, uint32, []olocaltxmonitor.TxAndEraId, error) {
	tip := n.ledgerState.Tip()
	mempoolTxs := n.mempool.Transactions()
	retTxs := make([]olocaltxmonitor.TxAndEraId, len(mempoolTxs))
	for i := range mempoolTxs {
		retTxs[i] = olocaltxmonitor.TxAndEraId{
			EraId: mempoolTxs[i].Type,
			Tx:    mempoolTxs[i].Cbor,
		}
	}
	return tip.Point.Slot, localtxmonitorMempoolCapacity, retTxs, nil
}
