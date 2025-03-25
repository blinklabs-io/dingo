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
	"fmt"

	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
)

func (n *Node) localtxsubmissionServerConnOpts() []olocaltxsubmission.LocalTxSubmissionOptionFunc {
	return []olocaltxsubmission.LocalTxSubmissionOptionFunc{
		olocaltxsubmission.WithSubmitTxFunc(n.localtxsubmissionServerSubmitTx),
	}
}

func (n *Node) localtxsubmissionServerSubmitTx(
	ctx olocaltxsubmission.CallbackContext,
	tx olocaltxsubmission.MsgSubmitTxTransaction,
) error {
	// Add transaction to mempool
	err := n.mempool.AddTransaction(
		uint(tx.EraId),
		tx.Raw.Content.([]byte),
	)
	if err != nil {
		n.config.logger.Error(
			fmt.Sprintf(
				"failed to add transaction to mempool: %s",
				err,
			),
			"component", "network",
			"protocol", "local-tx-submission",
			"role", "server",
			"connection_id", ctx.ConnectionId.String(),
		)
		return err
	}
	return nil
}
