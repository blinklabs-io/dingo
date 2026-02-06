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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"encoding/json"
	"net/http"
)

const apiVersion = "0.1.0"

// writeJSON writes a JSON response with the given status
// code.
func writeJSON(
	w http.ResponseWriter,
	status int,
	v any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	//nolint:errcheck,errchkjson
	json.NewEncoder(w).Encode(v)
}

// writeError writes a Blockfrost-format error response.
//
//nolint:unparam
func writeError(
	w http.ResponseWriter,
	status int,
	errStr string,
	message string,
) {
	writeJSON(w, status, ErrorResponse{
		StatusCode: status,
		Error:      errStr,
		Message:    message,
	})
}

// handleRoot handles GET / and returns API metadata.
func (b *Blockfrost) handleRoot(
	w http.ResponseWriter,
	_ *http.Request,
) {
	writeJSON(w, http.StatusOK, RootResponse{
		URL:     "https://blockfrost.io/",
		Version: apiVersion,
	})
}

// handleHealth handles GET /health and returns node health
// status.
func (b *Blockfrost) handleHealth(
	w http.ResponseWriter,
	_ *http.Request,
) {
	writeJSON(w, http.StatusOK, HealthResponse{
		IsHealthy: true,
	})
}

// handleLatestBlock handles GET /api/v0/blocks/latest and
// returns the latest block.
func (b *Blockfrost) handleLatestBlock(
	w http.ResponseWriter,
	_ *http.Request,
) {
	info, err := b.node.LatestBlock()
	if err != nil {
		b.logger.Error(
			"failed to get latest block",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve latest block",
		)
		return
	}
	output := "0"
	fees := "0"
	writeJSON(w, http.StatusOK, BlockResponse{
		Hash:          info.Hash,
		Slot:          info.Slot,
		Epoch:         info.Epoch,
		EpochSlot:     info.EpochSlot,
		Height:        info.Height,
		Time:          info.Time,
		Size:          info.Size,
		TxCount:       info.TxCount,
		SlotLeader:    info.SlotLeader,
		PreviousBlock: info.PreviousBlock,
		Confirmations: info.Confirmations,
		Output:        &output,
		Fees:          &fees,
		BlockVRF:      nil,
		NextBlock:     nil,
	})
}

// handleLatestBlockTxs handles
// GET /api/v0/blocks/latest/txs and returns transaction
// hashes from the latest block.
func (b *Blockfrost) handleLatestBlockTxs(
	w http.ResponseWriter,
	_ *http.Request,
) {
	hashes, err := b.node.LatestBlockTxHashes()
	if err != nil {
		b.logger.Error(
			"failed to get latest block txs",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve latest block transactions",
		)
		return
	}
	if hashes == nil {
		hashes = []string{}
	}
	writeJSON(w, http.StatusOK, hashes)
}

// handleLatestEpoch handles GET /api/v0/epochs/latest and
// returns the current epoch info.
func (b *Blockfrost) handleLatestEpoch(
	w http.ResponseWriter,
	_ *http.Request,
) {
	info, err := b.node.CurrentEpoch()
	if err != nil {
		b.logger.Error(
			"failed to get current epoch",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve current epoch",
		)
		return
	}
	activeStake := "0"
	writeJSON(w, http.StatusOK, EpochResponse{
		Epoch:          info.Epoch,
		StartTime:      info.StartTime,
		EndTime:        info.EndTime,
		FirstBlockTime: info.FirstBlockTime,
		LastBlockTime:  info.LastBlockTime,
		BlockCount:     info.BlockCount,
		TxCount:        info.TxCount,
		Output:         "0",
		Fees:           "0",
		ActiveStake:    &activeStake,
	})
}

// handleLatestEpochParams handles
// GET /api/v0/epochs/latest/parameters and returns the
// current protocol parameters.
func (b *Blockfrost) handleLatestEpochParams(
	w http.ResponseWriter,
	_ *http.Request,
) {
	info, err := b.node.CurrentProtocolParams()
	if err != nil {
		b.logger.Error(
			"failed to get protocol params",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve protocol parameters",
		)
		return
	}
	writeJSON(w, http.StatusOK, ProtocolParamsResponse{
		Epoch:                 info.Epoch,
		MinFeeA:               info.MinFeeA,
		MinFeeB:               info.MinFeeB,
		MaxBlockSize:          info.MaxBlockSize,
		MaxTxSize:             info.MaxTxSize,
		MaxBlockHeaderSize:    info.MaxBlockHeaderSize,
		KeyDeposit:            info.KeyDeposit,
		PoolDeposit:           info.PoolDeposit,
		EMax:                  info.EMax,
		NOpt:                  info.NOpt,
		A0:                    info.A0,
		Rho:                   info.Rho,
		Tau:                   info.Tau,
		ProtocolMajorVer:      info.ProtocolMajorVer,
		ProtocolMinorVer:      info.ProtocolMinorVer,
		MinPoolCost:           info.MinPoolCost,
		CoinsPerUtxoSize:      &info.CoinsPerUtxoSize,
		CoinsPerUtxoWord:      info.CoinsPerUtxoSize,
		PriceMem:              &info.PriceMem,
		PriceStep:             &info.PriceStep,
		MaxTxExMem:            &info.MaxTxExMem,
		MaxTxExSteps:          &info.MaxTxExSteps,
		MaxBlockExMem:         &info.MaxBlockExMem,
		MaxBlockExSteps:       &info.MaxBlockExSteps,
		MaxValSize:            &info.MaxValSize,
		CollateralPercent:     &info.CollateralPercent,
		MaxCollateralInputs:   &info.MaxCollateralInputs,
		MinUtxo:               "0",
		Nonce:                 "",
		DecentralisationParam: 0,
		ExtraEntropy:          nil,
	})
}

// handleNetwork handles GET /api/v0/network and returns
// network supply and stake information.
func (b *Blockfrost) handleNetwork(
	w http.ResponseWriter,
	_ *http.Request,
) {
	// TODO: Wire to real supply/stake data when available
	writeJSON(w, http.StatusOK, NetworkResponse{
		Supply: NetworkSupply{
			Max:         "45000000000000000",
			Total:       "0",
			Circulating: "0",
			Locked:      "0",
			Treasury:    "0",
			Reserves:    "0",
		},
		Stake: NetworkStake{
			Live:   "0",
			Active: "0",
		},
	})
}
