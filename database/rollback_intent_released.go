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

package database

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// A v0.94.2 database keeps a pending rollback intent as a sync_state row that
// carries the rolled-back block bodies themselves, because that release
// deleted them from the block store when it truncated the chain.

const (
	releasedRollbackIntentVersion      = 1
	releasedRollbackIntentPayloadBytes = 64 << 20
	// releasedRollbackIntentEncodedBytes bounds the row before it is decoded:
	// the base64 form of a full payload, per-block field names and numbers,
	// and the record envelope. A row v0.94.2 wrote always fits.
	releasedRollbackIntentEncodedBytes = (releasedRollbackIntentPayloadBytes+2)/3*4 +
		MaxRollbackIntentBlocks*512 + 4096
)

type releasedRollbackIntent struct {
	Slot    *uint64
	Hash    *string
	Blocks  []releasedRollbackBlock
	Version int
}

type releasedRollbackBlock struct {
	Hash     []byte `json:"hash"`
	PrevHash []byte `json:"prev_hash"`
	Cbor     []byte `json:"cbor"`
	ID       uint64 `json:"id"`
	Slot     uint64 `json:"slot"`
	Number   uint64 `json:"number"`
	Type     uint   `json:"type"`
}

// decodeReleasedRollbackIntent decodes a v0.94.2 row under the limits that
// release wrote within. The block array is read element by element so a row
// with too many blocks, or too many block bytes, is refused before the rest
// of it is allocated, and an unknown field is refused instead of
// materialized.
func decodeReleasedRollbackIntent(raw string) (releasedRollbackIntent, error) {
	var intent releasedRollbackIntent
	fail := func(err error) (releasedRollbackIntent, error) {
		return releasedRollbackIntent{}, fmt.Errorf(
			"%w: decode: %s", ErrInvalidRollbackIntent, err.Error(),
		)
	}
	if len(raw) > releasedRollbackIntentEncodedBytes {
		return fail(fmt.Errorf("encoded size %d", len(raw)))
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	if tok, err := dec.Token(); err != nil {
		return fail(err)
	} else if tok != json.Delim('{') {
		return fail(errors.New("record is not a JSON object"))
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return fail(err)
		}
		key, _ := keyTok.(string)
		switch key {
		case "format_version":
			err = dec.Decode(&intent.Version)
		case "slot":
			err = dec.Decode(&intent.Slot)
		case "hash":
			err = dec.Decode(&intent.Hash)
		case "blocks":
			intent.Blocks, err = decodeReleasedRollbackBlocks(dec)
		default:
			err = fmt.Errorf("unknown field %q", key)
		}
		if err != nil {
			return fail(err)
		}
	}
	if _, err := dec.Token(); err != nil {
		return fail(err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fail(errors.New("unexpected data after record"))
	}
	if intent.Version != releasedRollbackIntentVersion {
		return fail(fmt.Errorf("format version %d", intent.Version))
	}
	return intent, nil
}

func decodeReleasedRollbackBlocks(
	dec *json.Decoder,
) ([]releasedRollbackBlock, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if tok == nil {
		return nil, nil
	}
	if tok != json.Delim('[') {
		return nil, errors.New("blocks is not an array")
	}
	var blocks []releasedRollbackBlock
	total := 0
	for dec.More() {
		if len(blocks) >= MaxRollbackIntentBlocks {
			return nil, fmt.Errorf(
				"more than %d blocks", MaxRollbackIntentBlocks,
			)
		}
		var block releasedRollbackBlock
		if err := dec.Decode(&block); err != nil {
			return nil, err
		}
		total += len(block.Hash) + len(block.PrevHash) + len(block.Cbor)
		if total > releasedRollbackIntentPayloadBytes {
			return nil, fmt.Errorf(
				"block bytes exceed %d", releasedRollbackIntentPayloadBytes,
			)
		}
		blocks = append(blocks, block)
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return blocks, nil
}

// moveReleasedRollbackIntent turns a v0.94.2 rollback intent row into a
// block-store intent: each body the row carries is written back as a
// rolled-back block, one transaction per body, then the record that names
// them, synced. The row stays.
//
// A stop at any step is repeated by the next open: the bodies are rewritten
// with the same bytes, and once the block store holds a record the row is
// left alone. A row that cannot be read gets no record, which is what
// recovery in that release did with it.
func (d *Database) moveReleasedRollbackIntent() error {
	raw, err := d.metadata.GetSyncStateBounded(
		RollbackIntentKey, releasedRollbackIntentEncodedBytes, nil,
	)
	tooLarge := errors.Is(err, metadata.ErrSyncStateTooLarge)
	if err != nil && !tooLarge {
		return fmt.Errorf("read %s row: %w", RollbackIntentKey, err)
	}
	if raw == "" && !tooLarge {
		return nil
	}
	held, err := d.GetChainState(RollbackIntentKey, nil)
	if err != nil {
		return err
	}
	if held != "" {
		return nil
	}
	return d.writeReleasedRollbackIntent(raw, tooLarge)
}

func (d *Database) writeReleasedRollbackIntent(
	raw string,
	tooLarge bool,
) error {
	discard := func(err error) error {
		d.logger.Error(
			"discarding unreadable rollback intent",
			"component", "database",
			"error", err,
		)
		return nil
	}
	if tooLarge {
		return discard(metadata.ErrSyncStateTooLarge)
	}
	released, err := decodeReleasedRollbackIntent(raw)
	if err != nil {
		return discard(err)
	}
	point, err := rollbackIntentPoint(released.Slot, released.Hash)
	if err != nil {
		return discard(err)
	}
	intent := RollbackIntent{
		Point:  point,
		Blocks: make([]RollbackIntentBlock, 0, len(released.Blocks)),
	}
	for _, block := range released.Blocks {
		intent.Blocks = append(intent.Blocks, RollbackIntentBlock{
			Hash: block.Hash, PrevHash: block.PrevHash,
			ID: block.ID, Slot: block.Slot, Number: block.Number,
			Type: block.Type,
		})
	}
	value, err := encodeRollbackIntent(intent)
	if err != nil {
		return discard(err)
	}
	for _, block := range released.Blocks {
		if len(block.Hash) == 0 {
			continue
		}
		if err := d.RetainRolledBackBlock(
			block.Slot, block.Hash, block.Cbor,
		); err != nil {
			return err
		}
	}
	return d.SetChainStateSynced(RollbackIntentKey, value)
}
