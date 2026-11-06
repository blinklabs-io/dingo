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

package utxorpc

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/types"
)

// mapHistoryExpired reports a read of locally expired history as
// FailedPrecondition. The request is well formed and the chain is intact;
// this node no longer holds the block body, so retrying the same point cannot
// succeed and the caller has to choose a point within retained history.
// Errors that are not history expiry, including ones already carrying a
// Connect code, are returned unchanged.
func mapHistoryExpired(err error) error {
	if err == nil || !errors.Is(err, types.ErrHistoryExpired) {
		return err
	}
	if _, ok := errors.AsType[*connect.Error](err); ok {
		return err
	}
	if expired, ok := errors.AsType[*types.HistoryExpiredError](err); ok {
		return connect.NewError(
			connect.CodeFailedPrecondition,
			fmt.Errorf(
				"history expired: block at slot=%d hash=%x is no longer "+
					"stored on this node; request a point within the "+
					"retained history: %w",
				expired.Slot,
				expired.Hash,
				err,
			),
		)
	}
	return connect.NewError(
		connect.CodeFailedPrecondition,
		fmt.Errorf(
			"history expired: the requested block is no longer stored "+
				"on this node; request a point within the retained "+
				"history: %w",
			err,
		),
	)
}
