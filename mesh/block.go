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

package mesh

import (
	"bytes"
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
)

// handleBlock handles POST /block.
func (s *Server) handleBlock(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req BlockRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	if req.BlockIdentifier == nil {
		writeError(w, ErrInvalidRequest)
		return
	}

	block, meshErr := s.lookupBlock(
		req.BlockIdentifier,
	)
	if meshErr != nil {
		writeError(w, meshErr)
		return
	}

	txs, err := s.config.Database.
		GetTransactionsByBlockHash(
			block.Hash, nil,
		)
	if err != nil {
		s.logger.Error(
			"failed to get block transactions",
			"error", err,
		)
		writeError(w, wrapErr(ErrInternal, err))
		return
	}

	writeJSON(w, http.StatusOK, &BlockResponse{
		Block: convertBlock(
			block, txs,
			s.addrNetworkID,
			s.slotToTimestamp,
		),
	})
}

// handleBlockTransaction handles
// POST /block/transaction.
func (s *Server) handleBlockTransaction(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req BlockTransactionRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	if req.TransactionIdentifier == nil ||
		req.TransactionIdentifier.Hash == "" {
		writeError(w, ErrInvalidRequest)
		return
	}
	if req.BlockIdentifier == nil ||
		req.BlockIdentifier.Hash == "" {
		writeError(w, wrapErr(
			ErrInvalidRequest,
			errors.New(
				"block_identifier with hash "+
					"is required",
			),
		))
		return
	}

	hashBytes, err := hex.DecodeString(
		req.TransactionIdentifier.Hash,
	)
	if err != nil {
		writeError(w, wrapErr(
			ErrInvalidRequest, err,
		))
		return
	}

	tx, err := s.config.Database.GetTransactionByHash(
		hashBytes, nil,
	)
	if err != nil {
		s.logger.Error(
			"failed to look up transaction",
			"error", err,
		)
		writeError(w, wrapErr(ErrInternal, err))
		return
	}
	if tx == nil {
		writeError(w, ErrTransactionNotFound)
		return
	}

	// Verify the transaction belongs to the requested
	// block to prevent cross-block ambiguity.
	blockHashBytes, err := hex.DecodeString(
		req.BlockIdentifier.Hash,
	)
	if err != nil {
		writeError(w, wrapErr(
			ErrInvalidRequest, err,
		))
		return
	}
	if !bytes.Equal(tx.BlockHash, blockHashBytes) {
		writeError(w, ErrTransactionNotFound)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		&BlockTransactionResponse{
			Transaction: convertTransaction(
				*tx, s.addrNetworkID,
			),
		},
	)
}

// lookupBlock resolves a PartialBlockIdentifier to a
// database Block by hash or index.
func (s *Server) lookupBlock(
	id *PartialBlockIdentifier,
) (models.Block, *Error) {
	var (
		block models.Block
		err   error
	)

	switch {
	case id.Hash != nil && *id.Hash != "":
		hashBytes, decErr := hex.DecodeString(
			*id.Hash,
		)
		if decErr != nil {
			return block, wrapErr(
				ErrInvalidRequest, decErr,
			)
		}
		block, err = database.BlockByHash(
			s.config.Database, hashBytes,
		)
	case id.Index != nil:
		if *id.Index < 0 {
			return block, ErrInvalidRequest
		}
		// #nosec G115 -- validated non-negative
		block, err = s.config.Database.BlockByIndex(
			uint64(*id.Index), nil,
		)
	default:
		return block, ErrInvalidRequest
	}

	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return block, ErrBlockNotFound
		}
		s.logger.Error(
			"failed to look up block",
			"error", err,
		)
		return block, wrapErr(ErrInternal, err)
	}
	return block, nil
}
