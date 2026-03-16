package utxorpc

import (
	"github.com/blinklabs-io/dingo/database/models"
)

// blockRef is an internal helper representation that can be adapted to
// different protobuf BlockRef / ChainPoint types.
type blockRef struct {
	Slot   uint64
	Hash   []byte
	Height uint64
}

// blockRefFromModel builds a blockRef from a database block model.
// Height is derived from the block number.
func blockRefFromModel(b models.Block) blockRef {
	return blockRef{
		Slot:   b.Slot,
		Hash:   b.Hash,
		Height: b.Number,
	}
}
