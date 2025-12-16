package models

// UtxoId uniquely identifies a UTxO by transaction hash and output index.
type UtxoId struct {
	Hash []byte
	Idx  uint32
}
