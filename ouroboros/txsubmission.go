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

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
)

func TxsubmissionServerConnOpts(
	initFunc txsubmission.InitFunc,
) []txsubmission.TxSubmissionOptionFunc {
	return []txsubmission.TxSubmissionOptionFunc{
		txsubmission.WithInitFunc(initFunc),
	}
}

func TxsubmissionClientConnOpts(
	requestTxIdsFunc txsubmission.RequestTxIdsFunc,
	requestTxsFunc txsubmission.RequestTxsFunc,
) []txsubmission.TxSubmissionOptionFunc {
	return []txsubmission.TxSubmissionOptionFunc{
		txsubmission.WithRequestTxIdsFunc(requestTxIdsFunc),
		txsubmission.WithRequestTxsFunc(requestTxsFunc),
	}
}

func TxsubmissionClientStart(
	connManager *connmanager.ConnectionManager,
	mempool *mempool.Mempool,
	connId ConnectionId,
) error {
	// Register mempool consumer
	// We don't bother capturing the consumer because we can easily look it up later by connection ID
	_ = mempool.AddConsumer(connId)
	// Start TxSubmission loop
	conn := connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	conn.TxSubmission().Client.Init()
	return nil
}
