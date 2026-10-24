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

package blockfrost

import (
	"context"
	"net/http"
)

func stakeAddressPage[Item, Response any](
	b *Blockfrost,
	w http.ResponseWriter,
	r *http.Request,
	fetch func(
		ctx context.Context, stakeAddress string, params PaginationParams,
	) ([]Item, int, error),
	convert func(Item) Response,
	internalMessage string,
) {
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]Item, int, error) {
			return fetch(ctx, r.PathValue("stake_address"), params)
		},
		convert,
		internalMessage,
	)
}

// handleAccountStakeHistory handles
// GET /api/v0/accounts/{stake_address}/history.
func (b *Blockfrost) handleAccountStakeHistory(
	w http.ResponseWriter,
	r *http.Request,
) {
	stakeAddressPage(b, w, r, b.node.AccountStakeHistory,
		func(item AccountStakeHistoryInfo) AccountStakeHistoryResponse {
			return AccountStakeHistoryResponse(item)
		},
		"failed to retrieve account stake history",
	)
}

// handleAccountMIRs handles GET /api/v0/accounts/{stake_address}/mirs.
func (b *Blockfrost) handleAccountMIRs(
	w http.ResponseWriter,
	r *http.Request,
) {
	stakeAddressPage(b, w, r, b.node.AccountMIRs,
		func(item AccountMIRInfo) AccountMIRResponse {
			return AccountMIRResponse(item)
		},
		"failed to retrieve account MIRs",
	)
}

// handleAccountAddressAssets handles
// GET /api/v0/accounts/{stake_address}/addresses/assets.
func (b *Blockfrost) handleAccountAddressAssets(
	w http.ResponseWriter,
	r *http.Request,
) {
	release, ok := b.admitAddressSummary(w, r)
	if !ok {
		return
	}
	defer release()
	stakeAddressPage(b, w, r, b.node.AccountAddressAssets,
		func(item AddressAmountInfo) AddressAmountResponse {
			return AddressAmountResponse(item)
		},
		"failed to retrieve account address assets",
	)
}

// handleAccountAddressTotal handles
// GET /api/v0/accounts/{stake_address}/addresses/total.
func (b *Blockfrost) handleAccountAddressTotal(
	w http.ResponseWriter,
	r *http.Request,
) {
	release, ok := b.admitAddressSummary(w, r)
	if !ok {
		return
	}
	defer release()
	info, err := b.node.AccountAddressTotal(
		r.Context(), r.PathValue("stake_address"),
	)
	if err != nil {
		b.writeHistoryError(
			w, err, "failed to retrieve account address totals",
		)
		return
	}
	writeJSON(w, http.StatusOK, AccountAddressTotalResponse{
		StakeAddress: info.StakeAddress,
		ReceivedSum:  convertAddressAmounts(info.ReceivedSum),
		SentSum:      convertAddressAmounts(info.SentSum),
		TxCount:      info.TxCount,
	})
}
