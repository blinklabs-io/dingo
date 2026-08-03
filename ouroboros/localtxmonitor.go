// Copyright 2025 Blink Labs Software
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
	"math"
	"time"

	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
)

func (o *Ouroboros) localtxmonitorServerConnOpts() []olocaltxmonitor.LocalTxMonitorOptionFunc {
	return []olocaltxmonitor.LocalTxMonitorOptionFunc{
		olocaltxmonitor.WithGetMempoolFunc(
			o.instrumentLocaltxmonitorGetMempool(o.localtxmonitorServerGetMempool),
		),
	}
}

func (o *Ouroboros) instrumentLocaltxmonitorGetMempool(
	fn func(olocaltxmonitor.CallbackContext) (uint64, uint32, []olocaltxmonitor.TxAndEraId, error),
) func(olocaltxmonitor.CallbackContext) (uint64, uint32, []olocaltxmonitor.TxAndEraId, error) {
	return func(ctx olocaltxmonitor.CallbackContext) (uint64, uint32, []olocaltxmonitor.TxAndEraId, error) {
		start := time.Now()
		slot, capacity, txs, err := fn(ctx)
		o.recordProtocolMessage("localtxmonitor", err, time.Since(start))
		return slot, capacity, txs, err
	}
}

func (o *Ouroboros) localtxmonitorServerGetMempool(
	ctx olocaltxmonitor.CallbackContext,
) (uint64, uint32, []olocaltxmonitor.TxAndEraId, error) {
	tip := o.LedgerState.Tip()
	capacity := o.Mempool.CapacityBytes()
	if capacity < 0 || capacity > math.MaxUint32 {
		return 0, 0, nil, fmt.Errorf(
			"mempool capacity %d cannot be represented by LocalTxMonitor",
			capacity,
		)
	}
	mempoolTxs := o.Mempool.Transactions()
	retTxs := make([]olocaltxmonitor.TxAndEraId, len(mempoolTxs))
	for i := range mempoolTxs {
		retTxs[i] = olocaltxmonitor.TxAndEraId{
			EraId: mempoolTxs[i].Type,
			Tx:    mempoolTxs[i].Cbor,
		}
	}
	return tip.Point.Slot, uint32(capacity), retTxs, nil // #nosec G115 -- range checked above
}
