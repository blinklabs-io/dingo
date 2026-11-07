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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	"github.com/stretchr/testify/require"
)

// testSTMSigner is a test-only signer: it holds a BLS secret key derived from
// a seed and produces registrations and single signatures. The production
// code only verifies and aggregates, so signing lives here.
type testSTMSigner struct {
	partyID string
	secret  *big.Int
	stake   uint64
}

func newTestSTMSigner(seed int, stake uint64) *testSTMSigner {
	sum := sha256.Sum256(fmt.Appendf(nil, "dingo-test-signer-%d", seed))
	var sk fr.Element
	sk.SetBytes(sum[:])
	return &testSTMSigner{
		partyID: fmt.Sprintf("pool%d", seed),
		secret:  sk.BigInt(new(big.Int)),
		stake:   stake,
	}
}

func (s *testSTMSigner) sign(msg []byte) bls12381.G1Affine {
	h, err := bls12381.HashToG1(msg, stmBLSDomainSeparationTag)
	if err != nil {
		panic(err)
	}
	var sigma bls12381.G1Affine
	sigma.ScalarMultiplication(&h, s.secret)
	return sigma
}

func (s *testSTMSigner) verificationKey() []byte {
	var vk bls12381.G2Affine
	vk.ScalarMultiplicationBase(s.secret)
	b := vk.Bytes()
	return b[:]
}

func (s *testSTMSigner) proofOfPossession() []byte {
	k1 := s.sign(stmProofOfPossessionMessage)
	var k2 bls12381.G1Affine
	k2.ScalarMultiplicationBase(s.secret)
	a, b := k1.Bytes(), k2.Bytes()
	return append(a[:], b[:]...)
}

func (s *testSTMSigner) registration() stmRegistration {
	return stmRegistration{
		PartyID:           s.partyID,
		VerificationKey:   s.verificationKey(),
		ProofOfPossession: s.proofOfPossession(),
		Stake:             s.stake,
	}
}

// singleSignature signs msg and claims every lottery index it wins. ok is
// false when the signer wins none.
func (s *testSTMSigner) singleSignature(
	msg []byte,
	avk *stmAggregateVerificationKey,
	params ProtocolParameters,
	signerIndex uint64,
) (stmSingleSignature, bool) {
	msgp := stmConcatenateWithMessage(avk, msg)
	sigma := s.sign(msgp)
	sigmaBytes := sigma.Bytes()
	out := stmSingleSignature{
		Sigma:       sigmaBytes[:],
		SignerIndex: signerIndex,
	}
	for i := range params.M {
		ev := stmDenseMapping(out.Sigma, msgp, i)
		if stmIsLotteryWon(params.PhiF, ev, s.stake, avk.TotalStake) {
			out.Indexes = append(out.Indexes, i)
		}
	}
	return out, len(out.Indexes) > 0
}

type testSTMRegistration struct {
	signers []*testSTMSigner
	entries []stmClosedRegistrationEntry
	total   uint64
	tree    *stmMerkleTree
	avk     *stmAggregateVerificationKey
}

func closeTestSTMRegistration(
	t *testing.T,
	signers []*testSTMSigner,
) testSTMRegistration {
	t.Helper()
	regs := make([]stmRegistration, len(signers))
	for i, signer := range signers {
		regs[i] = signer.registration()
	}
	ordered, entries, total := closeSTMRegistration(regs)
	orderedSigners := make([]*testSTMSigner, len(ordered))
	for i, reg := range ordered {
		for _, signer := range signers {
			if signer.partyID == reg.PartyID {
				orderedSigners[i] = signer
				break
			}
		}
		require.NotNil(t, orderedSigners[i])
	}
	tree := newSTMMerkleTree(entries)
	avk := &stmAggregateVerificationKey{
		MTCommitment: stmMerkleTreeBatchCommitment{
			Root: tree.root(), NrLeaves: len(entries),
		},
		TotalStake: total,
	}
	return testSTMRegistration{
		signers: orderedSigners,
		entries: entries,
		total:   total,
		tree:    tree,
		avk:     avk,
	}
}

func TestVerifySTMProofOfPossession(t *testing.T) {
	t.Parallel()
	a, b := newTestSTMSigner(1, 10), newTestSTMSigner(2, 10)

	require.NoError(
		t,
		verifySTMProofOfPossession(a.verificationKey(), a.proofOfPossession()),
	)
	// A key registered with another signer's proof is rejected: possession
	// of one secret does not vouch for another's key.
	require.Error(
		t,
		verifySTMProofOfPossession(a.verificationKey(), b.proofOfPossession()),
	)
	// A matching signature with a swapped G1 key fails the second check.
	forged := append(
		a.proofOfPossession()[:48:48], b.proofOfPossession()[48:]...,
	)
	require.ErrorContains(
		t, verifySTMProofOfPossession(a.verificationKey(), forged),
		"does not match",
	)
	require.Error(t, verifySTMProofOfPossession(a.verificationKey(), []byte{1}))
}

func TestSTMMerkleBatchPathVerifiesForEverySubset(t *testing.T) {
	t.Parallel()
	for n := 1; n <= 9; n++ {
		entries := make([]stmClosedRegistrationEntry, n)
		for i := range entries {
			entries[i] = stmClosedRegistrationEntry{
				VerificationKey: newTestSTMSigner(i, 1).verificationKey(),
				Stake:           uint64(i + 1),
			}
		}
		tree := newSTMMerkleTree(entries)
		avk := &stmAggregateVerificationKey{
			MTCommitment: stmMerkleTreeBatchCommitment{
				Root: tree.root(), NrLeaves: n,
			},
		}
		for mask := 1; mask < 1<<n; mask++ {
			var indices []int
			var leaves []stmClosedRegistrationEntry
			for i := range n {
				if mask&(1<<i) != 0 {
					indices = append(indices, i)
					leaves = append(leaves, entries[i])
				}
			}
			proof, err := tree.batchPath(indices)
			require.NoError(t, err)
			require.NoError(
				t,
				stmVerifyLeavesMembershipFromBatchPath(avk, leaves, &proof),
				"n=%d subset=%v", n, indices,
			)
		}
	}
}

func TestSTMMerkleBatchPathRejectsBadIndices(t *testing.T) {
	t.Parallel()
	tree := newSTMMerkleTree([]stmClosedRegistrationEntry{
		{VerificationKey: newTestSTMSigner(1, 1).verificationKey(), Stake: 1},
		{VerificationKey: newTestSTMSigner(2, 1).verificationKey(), Stake: 1},
	})
	for _, idx := range [][]int{nil, {2}, {-1}, {1, 0}, {1, 1}} {
		_, err := tree.batchPath(idx)
		require.Error(t, err, "indices %v", idx)
	}
}

// aggregateForTest drives the production selection, proof and encoding for
// the given signers and returns the encoded AVK and multi-signature.
func aggregateForTest(
	t *testing.T,
	signers []*testSTMSigner,
	params ProtocolParameters,
	msg []byte,
) (string, string, error) {
	t.Helper()
	reg := closeTestSTMRegistration(t, signers)
	var singles []stmSingleSignature
	for idx, signer := range reg.signers {
		if sig, ok := signer.singleSignature(
			msg, reg.avk, params, uint64(idx),
		); ok {
			singles = append(singles, sig)
		}
	}
	selected, err := selectSTMSignatures(singles, params.K)
	if err != nil {
		return "", "", err
	}
	var withParty []stmSingleSignatureWithRegisteredParty
	var indices []int
	for _, sig := range selected {
		withParty = append(withParty, stmSingleSignatureWithRegisteredParty{
			Sig:      sig,
			RegParty: reg.entries[sig.SignerIndex],
		})
		indices = append(indices, int(sig.SignerIndex))
	}
	proof, err := reg.tree.batchPath(indices)
	require.NoError(t, err)
	return encodeSTMAggregateVerificationKey(
			reg.tree.root(), len(reg.entries), reg.total,
		),
		hex.EncodeToString(encodeSTMAggregateSignature(withParty, proof)),
		nil
}

func TestAggregatedSTMSignatureVerifies(t *testing.T) {
	t.Parallel()
	signers := []*testSTMSigner{
		newTestSTMSigner(1, 100),
		newTestSTMSigner(2, 200),
		newTestSTMSigner(3, 300),
		newTestSTMSigner(4, 400),
	}
	// phi_f below one makes the lottery real: each signer wins only some of
	// the m indices, in proportion to stake.
	params := ProtocolParameters{K: 6, M: 60, PhiF: 0.5}
	msg := []byte("signed message")

	avk, sig, err := aggregateForTest(t, signers, params, msg)
	require.NoError(t, err)
	require.NoError(
		t,
		verifySTMSignature(msg, avk, sig, params, newCertificateChainBudget()),
	)

	t.Run("other message", func(t *testing.T) {
		t.Parallel()
		require.Error(
			t,
			verifySTMSignature(
				[]byte("other"), avk, sig, params, newCertificateChainBudget(),
			),
		)
	})
	t.Run("higher quorum", func(t *testing.T) {
		t.Parallel()
		higher := params
		higher.K = 1000
		require.Error(
			t,
			verifySTMSignature(
				msg,
				avk,
				sig,
				higher,
				newCertificateChainBudget(),
			),
		)
	})
}

func TestAggregatedSTMSignatureRejectsRedistributedSignatures(t *testing.T) {
	t.Parallel()
	signers := []*testSTMSigner{
		newTestSTMSigner(1, 10),
		newTestSTMSigner(2, 10),
	}
	reg := closeTestSTMRegistration(t, signers)
	msg := []byte("signed message")
	msgp := stmConcatenateWithMessage(reg.avk, msg)

	first := reg.signers[0].sign(msgp)
	second := reg.signers[1].sign(msgp)
	var delta bls12381.G1Affine
	delta.ScalarMultiplicationBase(big.NewInt(13))
	var negDelta bls12381.G1Affine
	negDelta.Neg(&delta)
	first.Add(&first, &delta)
	second.Add(&second, &negDelta)
	firstBytes, secondBytes := first.Bytes(), second.Bytes()
	sigs := []stmSingleSignatureWithRegisteredParty{
		{
			Sig: stmSingleSignature{
				Sigma: firstBytes[:], Indexes: []uint64{0}, SignerIndex: 0,
			},
			RegParty: reg.entries[0],
		},
		{
			Sig: stmSingleSignature{
				Sigma: secondBytes[:], Indexes: []uint64{1}, SignerIndex: 1,
			},
			RegParty: reg.entries[1],
		},
	}
	proof, err := reg.tree.batchPath([]int{0, 1})
	require.NoError(t, err)
	encodedAVK := encodeSTMAggregateVerificationKey(
		reg.tree.root(), len(reg.entries), reg.total,
	)
	encodedSig := hex.EncodeToString(encodeSTMAggregateSignature(sigs, proof))

	require.ErrorContains(
		t,
		verifySTMSignature(
			msg,
			encodedAVK,
			encodedSig,
			ProtocolParameters{K: 2, M: 2, PhiF: 1},
			newCertificateChainBudget(),
		),
		"aggregate signature verification failed",
	)
}

func TestAggregatedSTMSignatureRejectsSignerWithoutLotteryIndex(t *testing.T) {
	t.Parallel()
	signers := []*testSTMSigner{
		newTestSTMSigner(1, 10),
		newTestSTMSigner(2, 10),
	}
	reg := closeTestSTMRegistration(t, signers)
	msg := []byte("signed message")
	msgp := stmConcatenateWithMessage(reg.avk, msg)
	sigs := make([]stmSingleSignatureWithRegisteredParty, len(reg.signers))
	for i, signer := range reg.signers {
		sigma := signer.sign(msgp)
		sigmaBytes := sigma.Bytes()
		sigs[i] = stmSingleSignatureWithRegisteredParty{
			Sig: stmSingleSignature{
				Sigma: sigmaBytes[:], SignerIndex: uint64(i),
			},
			RegParty: reg.entries[i],
		}
		if i == 0 {
			sigs[i].Sig.Indexes = []uint64{0}
		}
	}
	proof, err := reg.tree.batchPath([]int{0, 1})
	require.NoError(t, err)

	err = verifySTMSignature(
		msg,
		encodeSTMAggregateVerificationKey(
			reg.tree.root(), len(reg.entries), reg.total,
		),
		hex.EncodeToString(encodeSTMAggregateSignature(sigs, proof)),
		ProtocolParameters{K: 1, M: 1, PhiF: 1},
		newCertificateChainBudget(),
	)
	require.ErrorContains(t, err, "signature claims no lottery indices")
}

func TestAggregateSTMNeedsKIndices(t *testing.T) {
	t.Parallel()
	_, _, err := aggregateForTest(
		t,
		[]*testSTMSigner{newTestSTMSigner(1, 1)},
		ProtocolParameters{K: 50, M: 5, PhiF: 1},
		[]byte("m"),
	)
	require.ErrorContains(t, err, "need 50")
}

func TestVerifySTMSingleSignature(t *testing.T) {
	t.Parallel()
	signer := newTestSTMSigner(1, 10)
	other := newTestSTMSigner(2, 10)
	_, entries, total := closeSTMRegistration(
		[]stmRegistration{signer.registration()},
	)
	tree := newSTMMerkleTree(entries)
	avk := &stmAggregateVerificationKey{
		MTCommitment: stmMerkleTreeBatchCommitment{
			Root:     tree.root(),
			NrLeaves: 1,
		},
		TotalStake: total,
	}
	params := ProtocolParameters{K: 1, M: 8, PhiF: 1}
	msg := []byte("m")
	sig, ok := signer.singleSignature(msg, avk, params, 0)
	require.True(t, ok)

	require.NoError(
		t, verifySTMSingleSignature(msg, avk, params, entries[0], &sig),
	)
	require.Error(
		t, verifySTMSingleSignature([]byte("x"), avk, params, entries[0], &sig),
		"signature must bind the message",
	)
	otherEntry := stmClosedRegistrationEntry{
		VerificationKey: other.verificationKey(), Stake: 10,
	}
	require.Error(
		t, verifySTMSingleSignature(msg, avk, params, otherEntry, &sig),
		"signature must bind the registered key",
	)
	outOfRange := sig
	outOfRange.Indexes = []uint64{params.M}
	require.ErrorContains(
		t, verifySTMSingleSignature(msg, avk, params, entries[0], &outOfRange),
		"exceeds",
	)
	duplicate := sig
	duplicate.Indexes = []uint64{0, 0}
	require.ErrorContains(
		t, verifySTMSingleSignature(msg, avk, params, entries[0], &duplicate),
		"twice",
	)
	empty := sig
	empty.Indexes = nil
	require.Error(
		t, verifySTMSingleSignature(msg, avk, params, entries[0], &empty),
	)
	// A tiny stake at phi_f below one loses the lottery for a claimed index.
	lost := ProtocolParameters{K: 1, M: 8, PhiF: 0.01}
	avkBigTotal := *avk
	avkBigTotal.TotalStake = 1 << 40
	require.ErrorContains(
		t,
		verifySTMSingleSignature(msg, &avkBigTotal, lost, entries[0], &sig),
		"lottery lost",
	)
}

func TestSelectSTMSignaturesResolvesContestedIndices(t *testing.T) {
	t.Parallel()
	small := stmSingleSignature{
		Sigma: []byte{1}, Indexes: []uint64{0, 1, 2}, SignerIndex: 5,
	}
	large := stmSingleSignature{
		Sigma: []byte{2}, Indexes: []uint64{2, 3}, SignerIndex: 1,
	}

	// Index 2 is claimed by both; the smaller sigma keeps it, so the other
	// signer is left with nothing and drops out.
	got, err := selectSTMSignatures([]stmSingleSignature{large, small}, 3)
	require.NoError(t, err)
	require.Equal(t, []stmSingleSignature{small}, got)

	// With a fourth index needed the loser contributes only its own index
	// and the result is ordered by signer index.
	got, err = selectSTMSignatures([]stmSingleSignature{small, large}, 4)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, uint64(1), got[0].SignerIndex)
	require.Equal(t, []uint64{3}, got[0].Indexes)
	require.Equal(t, uint64(5), got[1].SignerIndex)
	require.Equal(t, []uint64{0, 1, 2}, got[1].Indexes)
}
