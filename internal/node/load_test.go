package node

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	fxcbor "github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractHeaderCbor(t *testing.T) {
	t.Parallel()

	header := gcbor.RawMessage{0x01}
	body := gcbor.RawMessage{0x02}
	blockCbor, err := fxcbor.Marshal([]gcbor.RawMessage{header, body})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	got, err := extractHeaderCbor(blockCbor)
	if err != nil {
		t.Fatalf("extractHeaderCbor returned error: %v", err)
	}
	if !bytes.Equal(got, header) {
		t.Fatalf("unexpected header bytes: got %x want %x", got, header)
	}
}

func TestCborArrayHeaderLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		want    int
		wantErr bool
	}{
		{
			name: "small definite",
			data: []byte{gcbor.CborTypeArray + 2, 0x01, 0x02},
			want: 1,
		},
		{
			name: "uint8 length",
			data: []byte{gcbor.CborTypeArray + 24, 0x20},
			want: 2,
		},
		{
			name: "uint16 length",
			data: []byte{gcbor.CborTypeArray + 25, 0x01, 0x00},
			want: 3,
		},
		{
			name: "uint32 length",
			data: []byte{gcbor.CborTypeArray + 26, 0x00, 0x01, 0x00, 0x00},
			want: 5,
		},
		{
			name: "uint64 length",
			data: []byte{gcbor.CborTypeArray + 27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00},
			want: 9,
		},
		{
			name: "indefinite",
			data: []byte{gcbor.CborTypeArray + 31, 0x01, 0xff},
			want: 1,
		},
		{
			name:    "empty input",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "non-array major type",
			data:    []byte{gcbor.CborTypeMap + 2},
			wantErr: true,
		},
		{
			name:    "truncated uint8 header",
			data:    []byte{gcbor.CborTypeArray + 24},
			wantErr: true,
		},
		{
			name:    "truncated uint16 header",
			data:    []byte{gcbor.CborTypeArray + 25, 0x01},
			wantErr: true,
		},
		{
			name:    "truncated uint32 header",
			data:    []byte{gcbor.CborTypeArray + 26, 0x00, 0x01},
			wantErr: true,
		},
		{
			name:    "truncated uint64 header",
			data:    []byte{gcbor.CborTypeArray + 27, 0x00, 0x00, 0x00},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := cborArrayHeaderLen(test.data)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("cborArrayHeaderLen returned error: %v", err)
			}
			if got != test.want {
				t.Fatalf("unexpected header len: got %d want %d", got, test.want)
			}
		})
	}
}

func TestCopyBlocksRaw_PreservesByronEbbLinkageAtOrigin(t *testing.T) {
	t.Parallel()

	immutableDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
	imm, err := immutable.New(immutableDir)
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err)
	defer iter.Close()

	ebbBlock, err := iter.Next()
	require.NoError(t, err)
	require.NotNil(t, ebbBlock)
	require.True(t, ebbBlock.IsEbb)

	nextBlock, err := iter.Next()
	require.NoError(t, err)
	require.NotNil(t, nextBlock)

	ebbHeader, err := decodeImmutableBlockHeader(ebbBlock)
	require.NoError(t, err)
	nextHeader, err := decodeImmutableBlockHeader(nextBlock)
	require.NoError(t, err)
	require.Equal(
		t,
		ebbHeader.Hash().Bytes(),
		nextHeader.PrevHash().Bytes(),
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	blocksCopied, _, err := copyBlocksRawWithCallback(
		context.Background(),
		logger,
		immutableDir,
		db,
		cm.PrimaryChain(),
		nil,
	)
	require.NoError(t, err)
	require.Greater(t, blocksCopied, 1)

	ebbPoint := ocommon.NewPoint(
		ebbHeader.SlotNumber(),
		ebbHeader.Hash().Bytes(),
	)
	importedEbb, err := cm.BlockByPoint(ebbPoint, nil)
	require.NoError(t, err)
	require.NotNil(t, importedEbb)
	assert.Equal(t, ebbPoint.Hash, importedEbb.Hash)

	nextPoint := ocommon.NewPoint(
		nextHeader.SlotNumber(),
		nextHeader.Hash().Bytes(),
	)
	importedNext, err := cm.BlockByPoint(nextPoint, nil)
	require.NoError(t, err)
	require.NotNil(t, importedNext)
	assert.Equal(t, ebbPoint.Hash, importedNext.PrevHash)
}

func TestCopyBlocksRawWithCallback_StoresUtxoOffsets(t *testing.T) {
	// No t.Parallel(): newTestDB shares process-wide plugin state
	// (see database.go:164), so concurrent test runs race on
	// SetPluginOption and the in-memory schema migration.
	immutableDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
	imm, err := immutable.New(immutableDir)
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	var (
		expectedPoint      ocommon.Point
		expectedTxHash     []byte
		expectedOutputIdx  uint32
		expectedOutputCbor []byte
	)
	const maxIterations = 10_000
	for i := range maxIterations {
		next, err := iter.Next()
		require.NoError(t, err)
		if next == nil {
			require.Failf(
				t,
				"no non-EBB block with produced outputs found",
				"iterator ended after %d iterations",
				i+1,
			)
		}
		if next.IsEbb {
			continue
		}
		block, err := gledger.NewBlockFromCbor(
			next.Type,
			next.Cbor,
			lcommon.VerifyConfig{SkipBodyHashValidation: true},
		)
		require.NoError(t, err)
		for _, tx := range block.Transactions() {
			produced := tx.Produced()
			if len(produced) == 0 {
				continue
			}
			expectedPoint = ocommon.NewPoint(next.Slot, next.Hash)
			expectedTxHash = bytes.Clone(tx.Hash().Bytes())
			expectedOutputIdx = produced[0].Id.Index()
			expectedOutputCbor = bytes.Clone(produced[0].Output.Cbor())
			break
		}
		if len(expectedTxHash) > 0 {
			break
		}
	}
	require.NotEmptyf(
		t,
		expectedTxHash,
		"no non-EBB block with produced outputs found after %d iterations",
		maxIterations,
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	blocksCopied, _, err := copyBlocksRawWithCallback(
		context.Background(),
		logger,
		immutableDir,
		db,
		cm.PrimaryChain(),
		func(rb chain.RawBlock, txn *database.Txn) error {
			_, err := storeRawBlockUtxoOffsets(txn, rb)
			return err
		},
	)
	require.NoError(t, err)
	require.Greater(t, blocksCopied, 1)

	blobTxn := db.BlobTxn(false)
	defer blobTxn.Rollback() //nolint:errcheck
	offsetData, err := db.Blob().GetUtxo(
		blobTxn.Blob(),
		expectedTxHash,
		expectedOutputIdx,
	)
	require.NoError(t, err)
	require.True(t, database.IsUtxoOffsetStorage(offsetData))

	offset, err := database.DecodeUtxoOffset(offsetData)
	require.NoError(t, err)
	assert.Equal(t, expectedPoint.Slot, offset.BlockSlot)
	assert.Equal(t, expectedPoint.Hash, offset.BlockHash[:])

	storedBlock, err := database.BlockByPoint(db, expectedPoint)
	require.NoError(t, err)
	end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
	require.LessOrEqual(t, end, uint64(len(storedBlock.Cbor)))
	assert.Equal(
		t,
		expectedOutputCbor,
		storedBlock.Cbor[offset.ByteOffset:end],
	)
}

func TestStoreRawBlockUtxoOffsetsPropagatesExtractError(t *testing.T) {
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck

	_, err := storeRawBlockUtxoOffsets(txn, chain.RawBlock{
		Slot: 42,
		Hash: bytes.Repeat([]byte{0x42}, 32),
		Cbor: []byte{0x82, 0x01},
		Type: 1,
	})
	require.ErrorContains(
		t,
		err,
		"block at slot 42: extract transaction offsets",
	)
}

func TestCopyBlocksRawWithCallback_BackfillsWhenChainTipPastImmutableTip(
	t *testing.T,
) {
	// No t.Parallel(): newTestDB shares process-wide plugin state
	// (see database.go:164), so concurrent test runs race on
	// SetPluginOption and the in-memory schema migration.
	immutableDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, immutableTipSlot, err := copyBlocksRawWithCallback(
		context.Background(),
		logger,
		immutableDir,
		db,
		cm.PrimaryChain(),
		nil,
	)
	require.NoError(t, err)

	currentTip := cm.PrimaryChain().Tip()
	// AddRawBlocks treats Cbor as opaque — a stub byte string is
	// sufficient to advance the chain tip past immutableTipSlot
	// for the resume check exercised below. If a future change
	// makes AddRawBlocks decode the body, swap this for a real
	// raw block from the immutable testdata directory.
	err = cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
		{
			Slot:        immutableTipSlot + 5,
			Hash:        bytes.Repeat([]byte{0x42}, 32),
			BlockNumber: currentTip.BlockNumber + 1,
			Type:        0,
			PrevHash:    currentTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	})
	require.NoError(t, err)

	var offsetsStored int
	blocksCopied, resumedImmutableTipSlot, err := copyBlocksRawWithCallback(
		context.Background(),
		logger,
		immutableDir,
		db,
		cm.PrimaryChain(),
		func(rb chain.RawBlock, txn *database.Txn) error {
			stored, err := storeRawBlockUtxoOffsets(txn, rb)
			offsetsStored += stored
			return err
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, blocksCopied)
	assert.Equal(t, immutableTipSlot, resumedImmutableTipSlot)
	assert.Greater(t, offsetsStored, 0)
}

func decodeImmutableBlockHeader(
	block *immutable.Block,
) (gledger.BlockHeader, error) {
	headerCbor, err := extractHeaderCbor(block.Cbor)
	if err != nil {
		return nil, fmt.Errorf(
			"decodeImmutableBlockHeader: extractHeaderCbor failed: %w",
			err,
		)
	}
	header, err := gledger.NewBlockHeaderFromCbor(block.Type, headerCbor)
	if err != nil {
		return nil, fmt.Errorf(
			"decodeImmutableBlockHeader: NewBlockHeaderFromCbor failed for type %v: %w",
			block.Type,
			err,
		)
	}
	return header, nil
}
