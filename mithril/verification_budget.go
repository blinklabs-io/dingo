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

package mithril

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Certificate-chain limits. Everything a certificate carries is
// aggregator-supplied and unauthenticated until the walk reaches the
// genesis signature, so every dimension of work the walk does before that
// point needs a bound that is independent of what the aggregator claims.
// Bounds are set from observed aggregator data (mainnet: about one
// certificate per epoch, 660 epochs, 150 KB largest certificate, 134
// signers, 53 signatures, 1949 lottery indices at k=1944, m=16948;
// preview: about 1443 certificates of 8 KB) with several times headroom.
const (
	// maxCertificateChainLength bounds how many certificates one
	// verification fetches. A chain holds roughly one certificate per
	// epoch, so this is several times the longest live network's history.
	maxCertificateChainLength = 10000

	// maxCertificateBytes bounds one certificate's JSON body. It stays
	// below maxResponseBytes so that overflow is distinguishable from the
	// transport's own truncation.
	maxCertificateBytes = 8 << 20

	// maxCertificateChainBytes bounds the bytes of every certificate the
	// walk reads. Decoded signer collections have a separate cumulative cap
	// because short JSON entries can expand substantially in memory.
	maxCertificateChainBytes = 512 << 20

	// maxCertificateChainSigners bounds signer entries retained across the
	// decoded chain. A separate cumulative limit is needed because small JSON
	// entries expand into slice elements and allocated party-ID strings.
	maxCertificateChainSigners = 1 << 22

	// stmMaxSigners bounds signers in certificate metadata and signatures
	// in an aggregate signature. Mainnet registers a few thousand pools at
	// most.
	stmMaxSigners = 1 << 13

	// stmMaxLotteryIndices bounds the lottery indices of one aggregate
	// signature. Indices are below parameter m (16948 on mainnet), so this
	// leaves several times headroom.
	stmMaxLotteryIndices = 1 << 16

	// stmMaxBatchPathValues bounds the Merkle sibling hashes of one batch
	// proof: at most one path of 30 levels per signer.
	stmMaxBatchPathValues = stmMaxSigners * 32

	// maxProtocolMessageParts bounds the entries of one certificate's
	// protocol message. Upstream defines about ten part keys and a
	// certificate carries a few of them.
	maxProtocolMessageParts = 64

	// maxCertificateChainWork bounds the weighted verification cost of a
	// whole chain. Mainnet costs about 5400 units per certificate, 3.6
	// million for the chain; the budget is a few times that, and about a
	// hundred worst-case certificates.
	maxCertificateChainWork = 1 << 26

	// Cost weights: a signer costs a point decompression, a subgroup
	// check and a pairing share; an index costs one hash and one float
	// evaluation; a path value costs one hash.
	stmWorkPerSigner   = 64
	stmWorkPerIndex    = 1
	stmWorkPerPathNode = 1
)

// errCertificateChainBudget is wrapped by every limit rejection in the
// certificate-chain walk.
var errCertificateChainBudget = errors.New(
	"certificate chain verification budget exceeded",
)

// certificateChainBudget accumulates the cost of one chain walk. It is not
// safe for concurrent use.
type certificateChainBudget struct {
	maxCertificates int
	maxBytes        int64
	maxSigners      int
	maxWork         uint64

	certificates int
	bytes        int64
	signers      int
	work         uint64
}

func newCertificateChainBudget() *certificateChainBudget {
	return &certificateChainBudget{
		maxCertificates: maxCertificateChainLength,
		maxBytes:        maxCertificateChainBytes,
		maxSigners:      maxCertificateChainSigners,
		maxWork:         maxCertificateChainWork,
	}
}

// chargeCertificate counts one more certificate fetch.
func (b *certificateChainBudget) chargeCertificate() error {
	if b.certificates >= b.maxCertificates {
		return fmt.Errorf(
			"%w: chain exceeded maximum depth of %d",
			errCertificateChainBudget,
			b.maxCertificates,
		)
	}
	b.certificates++
	return nil
}

// certificateByteLimit is the most the next certificate may occupy.
func (b *certificateChainBudget) certificateByteLimit() int64 {
	return min(maxCertificateBytes, max(b.maxBytes-b.bytes, 0))
}

// chargeBytes records the size of a certificate already read.
func (b *certificateChainBudget) chargeBytes(n int64) {
	b.bytes += n
}

// chargeSigners bounds signer entries before the chain retains a certificate.
func (b *certificateChainBudget) chargeSigners(n int) error {
	if n < 0 || n > b.maxSigners-b.signers {
		return fmt.Errorf(
			"%w: certificate chain signer entries exceed limit %d",
			errCertificateChainBudget,
			b.maxSigners,
		)
	}
	b.signers += n
	return nil
}

// chargeWork reserves cost before the work it describes is done.
func (b *certificateChainBudget) chargeWork(cost uint64) error {
	if cost > b.maxWork-b.work {
		return fmt.Errorf(
			"%w: verification work exceeds %d units",
			errCertificateChainBudget,
			b.maxWork,
		)
	}
	b.work += cost
	return nil
}

// checkSTMAggregateSignatureCounts rejects a parsed signature whose
// collections exceed the fixed bounds.
func checkSTMAggregateSignatureCounts(sig *stmAggregateSignature) error {
	if len(sig.Signatures) > stmMaxSigners {
		return fmt.Errorf(
			"%w: %d signatures exceed limit %d",
			errCertificateChainBudget, len(sig.Signatures), stmMaxSigners,
		)
	}
	indices := 0
	for _, s := range sig.Signatures {
		indices += len(s.Sig.Indexes)
		if indices > stmMaxLotteryIndices {
			return fmt.Errorf(
				"%w: lottery indices exceed limit %d",
				errCertificateChainBudget, stmMaxLotteryIndices,
			)
		}
	}
	if len(sig.BatchProof.Values) > stmMaxBatchPathValues ||
		len(sig.BatchProof.Indices) > stmMaxSigners {
		return fmt.Errorf(
			"%w: batch proof has %d values and %d indices",
			errCertificateChainBudget,
			len(sig.BatchProof.Values), len(sig.BatchProof.Indices),
		)
	}
	return nil
}

// stmAggregateSignatureWork is the weighted cost of verifying sig.
func stmAggregateSignatureWork(sig *stmAggregateSignature) uint64 {
	work := uint64(len(sig.Signatures)) * stmWorkPerSigner
	for _, s := range sig.Signatures {
		work += uint64(len(s.Sig.Indexes)) * stmWorkPerIndex
	}
	return work + uint64(len(sig.BatchProof.Values))*stmWorkPerPathNode
}

// walkBoundedJSONArray visits each array element without first materializing
// the complete collection. It is used at untrusted JSON collection boundaries
// where a short serialized element can expand into a much larger Go value.
func walkBoundedJSONArray(
	data []byte,
	limit int,
	label string,
	visit func(json.RawMessage) error,
) (int, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return 0, err
	}
	if token == nil {
		return 0, nil
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '[' {
		return 0, fmt.Errorf("%s is not a JSON array", label)
	}
	count := 0
	for decoder.More() {
		if count >= limit {
			return 0, fmt.Errorf(
				"%w: %s exceed limit %d",
				errCertificateChainBudget,
				label,
				limit,
			)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return 0, err
		}
		if visit != nil {
			if err := visit(raw); err != nil {
				return 0, err
			}
		}
		count++
	}
	if _, err := decoder.Token(); err != nil {
		return 0, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return 0, fmt.Errorf("%s has trailing JSON data", label)
		}
		return 0, err
	}
	return count, nil
}
