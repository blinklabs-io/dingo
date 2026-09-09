// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");

package ledger

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLedgerProcessBlockAllowsSyntheticByronBlocksWithPlaceholderCbor keeps
// structured test blocks out of the decoded-wire size-validation boundary.
// A placeholder Cbor value is not sufficient to apply Byron genesis limits;
// concrete gouroboros Byron blocks are covered by the envelope tests.
func TestLedgerProcessBlockAllowsSyntheticByronBlocksWithPlaceholderCbor(
	t *testing.T,
) {
	db := newTestDB(t)
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	block := &envelopeTestBlock{
		header: &envelopeTestHeader{
			cbor:   []byte{0x80},
			slot:   1,
			number: 1,
			era:    byron.EraByron,
		},
		cbor: []byte{0x82, 0x80, 0x80},
	}

	err := db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 1, Hash: block.Hash().Bytes()},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{origin: true},
			nil,
			eras.ByronEraDesc,
			&shelley.ShelleyProtocolParameters{},
			nil,
			0,
		)
		return err
	})
	require.NoError(t, err)
}
