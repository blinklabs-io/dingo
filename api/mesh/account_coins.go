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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
)

// handleAccountCoins handles POST /account/coins.
//
// Mesh defines the response as the account's complete coin set: a client
// treats an omitted coin as absent, so the response has no pagination and no
// size refusal. The UTxOs are read in bounded pages from one database
// snapshot and each page is encoded and flushed before the next is read, so
// server memory is bounded by one page however large the account is. Once the
// first coin is written the status line is gone; a later failure aborts the
// connection, leaving a body that is not a complete JSON document.
func (s *Server) handleAccountCoins(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req AccountCoinsRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	addr, meshErr := s.parseAccountAddress(
		req.AccountIdentifier,
	)
	if meshErr != nil {
		writeError(w, meshErr)
		return
	}
	pattern, err := models.ExactUtxoAddressPattern(addr)
	if err != nil {
		writeError(w, wrapErr(ErrInvalidRequest, err))
		return
	}
	release, ok := s.acquireBulk()
	if !ok {
		writeError(w, ErrUnavailable)
		return
	}
	defer release()

	stream := &coinStream{w: w, rc: http.NewResponseController(w)}
	tip, err := s.config.LedgerState.ScanUtxosByAddress(
		r.Context(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
			Limit:           s.config.accountCoinsPageSize,
		},
		s.config.accountCoinsPageCandidates,
		s.config.accountCoinsPageBytes,
		stream.writePage,
	)
	if err == nil {
		err = stream.finish(s.tipBlockID(tip))
	}
	if err == nil {
		return
	}
	canceled := r.Context().Err() != nil
	switch {
	case canceled:
		s.logger.Debug("account coins read canceled", "error", err)
	default:
		s.logger.Error(
			"failed to stream UTxOs for account",
			"address",
			req.AccountIdentifier.Address,
			"started", stream.started,
			"error", err,
		)
	}
	if stream.started {
		// The 200 status and part of the coin list are already on the wire.
		// Aborting resets the connection without the closing bytes, so no
		// client can read the partial list as a complete response.
		panic(http.ErrAbortHandler)
	}
	if !canceled {
		writeError(w, ErrInternal)
	}
}

// coinStream encodes an AccountCoinsResponse incrementally. The coins array
// opens with the first page and the document closes only in finish, after
// the scan has succeeded.
type coinStream struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	started bool
	wrote   bool
	buf     bytes.Buffer
}

func (c *coinStream) start() {
	if c.started {
		return
	}
	c.started = true
	c.w.Header().Set("Content-Type", "application/json")
	c.w.WriteHeader(http.StatusOK)
	c.buf.WriteString(`{"coins":[`)
}

// writePage encodes one page of UTxOs and flushes it to the client before
// the scan reads the next page. It retains nothing from the page.
func (c *coinStream) writePage(page []models.UtxoWithOrdering) error {
	c.start()
	utxos := make([]models.Utxo, 0, len(page))
	for i := range page {
		utxo := page[i].Utxo
		// The page carries output CBOR but not the asset table; the output
		// holds the same native assets.
		if len(utxo.Assets) == 0 && len(utxo.Cbor) > 0 {
			assets, err := outputAssets(&utxo)
			if err != nil {
				return fmt.Errorf(
					"decode assets of %x#%d: %w",
					utxo.TxId,
					utxo.OutputIdx,
					err,
				)
			}
			utxo.Assets = assets
		}
		utxos = append(utxos, utxo)
	}
	for _, coin := range utxosToCoins(utxos) {
		raw, err := json.Marshal(coin)
		if err != nil {
			return err
		}
		if c.wrote {
			c.buf.WriteByte(',')
		}
		c.wrote = true
		c.buf.Write(raw)
	}
	return c.flush()
}

// finish closes the coins array and writes the block identifier of the
// snapshot the coins were read from. JSON object members are unordered, so
// the identifier may follow the coins.
func (c *coinStream) finish(blockID *BlockIdentifier) error {
	c.start()
	raw, err := json.Marshal(blockID)
	if err != nil {
		return err
	}
	c.buf.WriteString(`],"block_identifier":`)
	c.buf.Write(raw)
	c.buf.WriteString("}\n")
	return c.flush()
}

func (c *coinStream) flush() error {
	// A writer without deadline support, such as a test recorder, still
	// streams; only the stall bound is lost.
	_ = c.rc.SetWriteDeadline(time.Now().Add(accountCoinsPageWriteTimeout))
	if _, err := c.w.Write(c.buf.Bytes()); err != nil {
		return err
	}
	c.buf.Reset()
	if err := c.rc.Flush(); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// outputAssets decodes an output's native assets in a stable policy and name
// order.
func outputAssets(utxo *models.Utxo) ([]models.Asset, error) {
	output, err := utxo.Decode()
	if err != nil {
		return nil, fmt.Errorf("decode output: %w", err)
	}
	assets, err := models.ConvertMultiAssetToModels(output.Assets())
	if err != nil {
		return nil, err
	}
	slices.SortFunc(assets, func(a, b models.Asset) int {
		if c := bytes.Compare(a.PolicyId, b.PolicyId); c != 0 {
			return c
		}
		return bytes.Compare(a.Name, b.Name)
	})
	return assets, nil
}
