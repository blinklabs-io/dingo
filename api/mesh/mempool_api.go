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

package mesh

import (
	"errors"
	"fmt"
	"net/http"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
)

// handleMempool handles POST /mempool. It refuses a request that finds every
// listing slot held, after the body has validated so a malformed request never
// takes a slot. The response must name every pending transaction and has no
// cursor, so a pool larger than maxMempoolListing is refused as retriable
// rather than answered with a prefix that hides the rest.
func (s *Server) handleMempool(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req NetworkRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	select {
	case s.mempoolSlots <- struct{}{}:
		defer func() { <-s.mempoolSlots }()
	default:
		writeError(w, ErrUnavailable)
		return
	}

	hashes, total := s.config.Mempool.TransactionHashes(
		s.config.maxMempoolListing,
	)
	if total > len(hashes) {
		writeError(w, wrapErr(ErrUnavailable, fmt.Errorf(
			"mempool holds %d transactions, more than the %d one "+
				"response can list",
			total, s.config.maxMempoolListing,
		)))
		return
	}
	txIDs := make(
		[]*TransactionIdentifier, len(hashes),
	)
	for i := range hashes {
		txIDs[i] = &TransactionIdentifier{
			Hash: hashes[i],
		}
	}

	writeJSON(w, http.StatusOK, &MempoolResponse{
		TransactionIdentifiers: txIDs,
	})
}

// handleMempoolTransaction handles
// POST /mempool/transaction.
func (s *Server) handleMempoolTransaction(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req MempoolTransactionRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	if req.TransactionIdentifier == nil ||
		req.TransactionIdentifier.Hash == "" {
		writeError(w, wrapErr(
			ErrInvalidRequest,
			errors.New(
				"transaction_identifier is required",
			),
		))
		return
	}

	mempoolTx, exists := s.config.Mempool.GetTransaction(
		req.TransactionIdentifier.Hash,
	)
	if !exists {
		writeError(w, ErrTransactionNotFound)
		return
	}

	tx, err := gledger.NewTransactionFromCbor(
		mempoolTx.Type, mempoolTx.Cbor,
	)
	if err != nil {
		s.logger.Error("failed to decode mempool transaction", "error", err)
		writeError(w, ErrInternal)
		return
	}

	ops, _ := convertParsedTransaction(tx, true)

	writeJSON(
		w,
		http.StatusOK,
		&MempoolTransactionResponse{
			Transaction: &Transaction{
				TransactionIdentifier: &TransactionIdentifier{
					Hash: tx.Hash().String(),
				},
				Operations: ops,
			},
		},
	)
}
