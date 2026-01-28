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

package ouroboros

import (
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
)

func (o *Ouroboros) leiosfetchServerConnOpts() []oleiosfetch.LeiosFetchOptionFunc {
	return []oleiosfetch.LeiosFetchOptionFunc{
		oleiosfetch.WithBlockRequestFunc(o.leiosfetchServerBlockRequest),
		oleiosfetch.WithBlockTxsRequestFunc(o.leiosfetchServerBlockTxsRequest),
		oleiosfetch.WithVotesRequestFunc(o.leiosfetchServerVotesRequest),
		oleiosfetch.WithBlockRangeRequestFunc(o.leiosfetchServerBlockRangeRequest),
	}
}

func (o *Ouroboros) leiosfetchClientConnOpts() []oleiosfetch.LeiosFetchOptionFunc {
	return []oleiosfetch.LeiosFetchOptionFunc{
		// NOTE: this is purposely empty
	}
}

func (o *Ouroboros) leiosfetchServerBlockRequest(
	ctx oleiosfetch.CallbackContext,
	point ocommon.Point,
) (protocol.Message, error) {
	// TODO
	return nil, nil
}

func (o *Ouroboros) leiosfetchServerBlockTxsRequest(
	ctx oleiosfetch.CallbackContext,
	point ocommon.Point,
	txBitmap map[uint16]uint64,
) (protocol.Message, error) {
	// TODO
	return nil, nil
}

func (o *Ouroboros) leiosfetchServerVotesRequest(
	ctx oleiosfetch.CallbackContext,
	voteIds []oleiosfetch.MsgVotesRequestVoteId,
) (protocol.Message, error) {
	// TODO
	return nil, nil
}

func (o *Ouroboros) leiosfetchServerBlockRangeRequest(
	ctx oleiosfetch.CallbackContext,
	start ocommon.Point,
	end ocommon.Point,
) error {
	// TODO
	return nil
}
