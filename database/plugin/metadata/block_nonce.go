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

package metadata

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// BlockNonceBatchStore persists block nonces in bounded batches.
type BlockNonceBatchStore interface {
	// SetBlockNonces persists block nonces within the supplied metadata transaction.
	SetBlockNonces([]models.BlockNonce, types.Txn) error
}

// BlockNonceBatchQueuer queues block nonces in a batch accumulator so that
// FlushBatch writes them as multi-row inserts. Rows are written in queue
// order. A backend that does not implement it takes the immediate
// SetBlockNonce path.
type BlockNonceBatchQueuer interface {
	// QueueBlockNonce adds nonce to acc, which must have been created by the
	// same backend. Nothing reaches the database until FlushBatch.
	QueueBlockNonce(
		nonce models.BlockNonce,
		acc types.MetadataBatchAccumulator,
		txn types.Txn,
	) error
}
