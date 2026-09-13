package forging

import "github.com/blinklabs-io/gouroboros/cbor"

type segmentedBodySize struct {
	payloadSize   uint64
	txCount       int
	metadataCount int
}

func (size segmentedBodySize) withTransaction(
	body, witnesses, metadata cbor.RawMessage,
) segmentedBodySize {
	size.payloadSize += uint64(len(body)) + uint64(len(witnesses))
	if metadata != nil {
		size.payloadSize += uint64(cbor.ArrayHeaderSize(size.txCount)) +
			uint64(len(metadata))
		size.metadataCount++
	}
	size.txCount++
	return size
}

func (size segmentedBodySize) size(era eraKind) uint64 {
	total := size.payloadSize + 2*uint64(cbor.ArrayHeaderSize(size.txCount)) +
		uint64(cbor.ArrayHeaderSize(size.metadataCount))
	if era.hasInvalidTxs() {
		total++
		if era.usesIndefInvalidList() {
			total++
		}
	}
	return total
}
