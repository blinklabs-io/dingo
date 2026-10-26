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

package ledger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// leiosCertApplyFixture is a standard-profile (non-Musashi) Dijkstra ledger:
// per-transaction validation runs, certificate verification runs, and Plutus
// disagreements are not tolerated.
type leiosCertApplyFixture struct {
	*pathFixture
	ebHash      lcommon.Blake2b256
	ebTxs       []cbor.RawMessage
	ebTx        lcommon.Transaction
	stakeKey    []byte
	certErr     error
	certCalls   int
	certAnnHash []byte
	ebAvailable bool
}

const (
	leiosCertApplyAnnouncingSlot uint64 = pathBlockSlot + 1
	leiosCertApplyCertifyingSlot uint64 = pathBlockSlot + 2
	leiosCertApplyDeposit        uint64 = 20
	leiosCertApplyInitialReward  uint64 = 5
)

func newLeiosCertApplyFixture(t *testing.T) *leiosCertApplyFixture {
	return newLeiosCertApplyFixtureWith(t, pathTx{top: pathLevel{
		fields: pathDeposits(
			map[cbor.ByteString]uint64{
				pathKeyAccount(0x42): leiosCertApplyDeposit,
			},
		),
		funds: leiosCertApplyDeposit,
	}})
}

func newLeiosCertApplyFixtureWith(
	t *testing.T,
	txSpec pathTx,
) *leiosCertApplyFixture {
	return newLeiosCertApplyFixtureWithTransactions(
		t,
		[]pathAccount{{key: 0x42, reward: leiosCertApplyInitialReward}},
		0x42,
		[]pathTx{txSpec},
	)
}

func newLeiosCertApplyFixtureWithTransactions(
	t *testing.T,
	accounts []pathAccount,
	stakeKey byte,
	txSpecs []pathTx,
) *leiosCertApplyFixture {
	t.Helper()
	path := newPathFixture(
		t,
		accounts,
		nil,
		txSpecs...,
	)
	f := &leiosCertApplyFixture{
		pathFixture: path,
		stakeKey:    pathStakeKey(stakeKey),
		ebAvailable: true,
	}
	f.ebTxs = make([]cbor.RawMessage, 0, len(path.txs))
	for _, tx := range path.txs {
		// Endorser transactions travel as block_transactions: the fixture's
		// [body, witnesses, auxiliary_data] gains the trailing is_valid flag.
		var parts []cbor.RawMessage
		_, err := cbor.Decode(tx.Cbor(), &parts)
		require.NoError(t, err)
		raw, err := cbor.Encode(append(parts, cbor.RawMessage{0xf5}))
		require.NoError(t, err)
		f.ebTxs = append(f.ebTxs, raw)
	}
	var err error
	f.ebTx, err = safedecode.DijkstraBlockTransaction(f.ebTxs[0])
	require.NoError(t, err)
	f.ebHash = lcommon.NewBlake2b256(leiosTestHash(0xEB))
	f.ls.config.EndorserBlockProvider = func(
		hash []byte,
		slot uint64,
	) ([]cbor.RawMessage, bool) {
		if !f.ebAvailable || !bytes.Equal(hash, f.ebHash.Bytes()) ||
			slot != leiosCertApplyAnnouncingSlot {
			return nil, false
		}
		return f.ebTxs, true
	}
	f.ls.config.ValidateLeiosCertificate = func(
		_ uint64,
		announcingBlockHash []byte,
		_ []byte,
		_ []byte,
	) error {
		f.certCalls++
		f.certAnnHash = bytes.Clone(announcingBlockHash)
		return f.certErr
	}
	return f
}

// block builds a Dijkstra ranking block whose Leios header extension carries
// the certified flag and, when announce is set, the fixture's endorser-block
// announcement. A certifying block also carries a body certificate.
func (f *leiosCertApplyFixture) block(
	number, slot uint64,
	prevHash []byte,
	certified bool,
	announce bool,
) (*dijkstra.DijkstraBlock, *database.BlockIngestionResult) {
	t := f.t
	t.Helper()
	announcement := cbor.RawMessage{0xf6}
	if announce {
		announcement = leiosTestRaw(t, []any{f.ebHash.Bytes(), uint64(4096)})
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: dijkstraTestBlockHeader(
			number,
			slot,
			lcommon.NewBlake2b256(prevHash),
			lcommon.Blake2b256{},
			0,
		),
	}
	block.BlockHeader.LeiosHeaderExtension = []cbor.RawMessage{
		leiosTestRaw(t, certified),
		announcement,
	}
	if certified {
		block.BlockBody.LeiosCertificate = &dijkstra.DijkstraLeiosCertificate{
			Signers:             []byte{0x80},
			AggregatedSignature: make([]byte, 48),
		}
	}
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	offsets, err := database.NewBlockIndexer(slot, block.Hash().Bytes()).
		ComputeOffsets(blockCbor, block)
	require.NoError(t, err)
	return block, offsets
}

// process applies block through ledgerProcessBlock in one database
// transaction, which commits only when it returns no error.
func (f *leiosCertApplyFixture) process(
	block *dijkstra.DijkstraBlock,
	offsets *database.BlockIngestionResult,
	parent envelopeParent,
) error {
	point := ocommon.Point{Slot: block.SlotNumber(), Hash: block.Hash().Bytes()}
	return f.db.Transaction(context.Background(), true).Do(
		func(txn *database.Txn) error {
			_, err := f.ls.ledgerProcessBlock(
				context.Background(),
				txn,
				point,
				block,
				true,
				false,
				false,
				block.PrevHash().Bytes(),
				parent,
				offsets,
				eras.DijkstraEraDesc,
				f.pparams,
				nil,
				0,
				0,
				false,
			)
			return err
		},
	)
}

// storeBlock persists a processed block so a certifying child can resolve its
// announcement through the block store.
func (f *leiosCertApplyFixture) storeBlock(block *dijkstra.DijkstraBlock) {
	f.t.Helper()
	require.NoError(f.t, f.db.BlockCreate(models.Block{
		Slot:     block.SlotNumber(),
		Hash:     block.Hash().Bytes(),
		PrevHash: block.PrevHash().Bytes(),
		Number:   block.BlockNumber(),
		Type:     gledger.BlockTypeDijkstra,
		Cbor:     block.Cbor(),
	}, nil))
}

// announce processes and stores the announcing ranking block, the parent of
// any certifying block in these tests.
func (f *leiosCertApplyFixture) announce() *dijkstra.DijkstraBlock {
	f.t.Helper()
	block, offsets := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	require.NoError(f.t, f.process(block, offsets, envelopeParent{origin: true}))
	f.storeBlock(block)
	return block
}

func (f *leiosCertApplyFixture) certifyingChild(
	parent *dijkstra.DijkstraBlock,
) (*dijkstra.DijkstraBlock, *database.BlockIngestionResult, envelopeParent) {
	block, offsets := f.block(
		2,
		leiosCertApplyCertifyingSlot,
		parent.Hash().Bytes(),
		true,
		false,
	)
	return block, offsets, envelopeParent{
		slot:        parent.SlotNumber(),
		blockNumber: parent.BlockNumber(),
	}
}

// requireEndorserEffects reports whether the endorser transaction's UTxO,
// transaction row and stake registration (with its deposit) are on the
// ledger, failing on any partial state.
func (f *leiosCertApplyFixture) requireEndorserEffects(want bool) {
	t := f.t
	t.Helper()
	txHash := f.ebTx.Hash().Bytes()
	utxo, err := f.db.Metadata().GetUtxo(txHash, 0, nil)
	require.NoError(t, err)
	txs, err := f.db.GetTransactionsByHashes(
		context.Background(),
		[][]byte{txHash},
		nil,
	)
	require.NoError(t, err)
	account, accountErr := f.db.GetAccountByCredential(
		context.Background(),
		0,
		f.stakeKey,
		false,
		nil,
	)
	if !want {
		require.Nil(t, utxo, "endorser transaction output reached the UTxO set")
		require.Empty(t, txs, "endorser transaction was recorded")
		require.NoError(t, accountErr)
		require.Equal(t, leiosCertApplyInitialReward, uint64(account.Reward))
		return
	}
	require.NotNil(t, utxo, "certified endorser output is missing")
	require.Len(t, txs, 1, "certified endorser transaction is not recorded")
	require.NoError(t, accountErr, "certified direct-deposit account is missing")
	require.NotNil(t, account)
	require.Equal(
		t,
		leiosCertApplyInitialReward+leiosCertApplyDeposit,
		uint64(account.Reward),
	)
}

func (f *leiosCertApplyFixture) removeEndorserWitnesses() {
	f.t.Helper()
	var envelope []cbor.RawMessage
	_, err := cbor.Decode(f.ebTx.Cbor(), &envelope)
	require.NoError(f.t, err)
	require.GreaterOrEqual(f.t, len(envelope), 2)
	emptyWitnesses, err := cbor.Encode(map[uint]any{})
	require.NoError(f.t, err)
	envelope[1] = emptyWitnesses
	raw, err := cbor.Encode(envelope)
	require.NoError(f.t, err)
	tx, err := safedecode.DijkstraBlockTransaction(raw)
	require.NoError(f.t, err)
	f.ebTx = tx
	f.ebTxs = []cbor.RawMessage{raw}
}

func (f *leiosCertApplyFixture) corruptEndorserWitness() {
	f.t.Helper()
	var envelope []cbor.RawMessage
	_, err := cbor.Decode(f.ebTx.Cbor(), &envelope)
	require.NoError(f.t, err)
	require.GreaterOrEqual(f.t, len(envelope), 2)
	var witnesses map[uint]cbor.RawMessage
	_, err = cbor.Decode(envelope[1], &witnesses)
	require.NoError(f.t, err)
	var vkeys []cbor.RawMessage
	_, err = cbor.Decode(witnesses[0], &vkeys)
	require.NoError(f.t, err)
	require.NotEmpty(f.t, vkeys)
	var vkeyWitness []cbor.RawMessage
	_, err = cbor.Decode(vkeys[0], &vkeyWitness)
	require.NoError(f.t, err)
	require.Len(f.t, vkeyWitness, 2)
	var signature []byte
	_, err = cbor.Decode(vkeyWitness[1], &signature)
	require.NoError(f.t, err)
	require.NotEmpty(f.t, signature)
	signature[0] ^= 0xff
	vkeyWitness[1], err = cbor.Encode(signature)
	require.NoError(f.t, err)
	vkeys[0], err = cbor.Encode(vkeyWitness)
	require.NoError(f.t, err)
	witnesses[0], err = cbor.Encode(vkeys)
	require.NoError(f.t, err)
	envelope[1], err = cbor.Encode(witnesses)
	require.NoError(f.t, err)
	raw, err := cbor.Encode(envelope)
	require.NoError(f.t, err)
	tx, err := safedecode.DijkstraBlockTransaction(raw)
	require.NoError(f.t, err)
	f.ebTx = tx
	f.ebTxs = []cbor.RawMessage{raw}
}

func requireRejectedEndorserClosure(
	t *testing.T,
	f *leiosCertApplyFixture,
) {
	t.Helper()
	parent := f.announce()
	certifier, offsets, envParent := f.certifyingChild(parent)
	require.NoError(t, f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	))
	err := f.process(certifier, offsets, envParent)
	var validationErr *headerValidationError
	require.ErrorAs(t, err, &validationErr)
	f.requireEndorserEffects(false)
}

// A ranking block that announces an endorser block without a certificate is
// an ordinary Praos block: none of the announced transactions reach the
// ledger, however they would fare under validation, as in the reference
// Forker.applyBlock and CIP-0164's "only when properly certified".
func TestLeiosUncertifiedAnnouncementAppliesNoEndorserTransactions(
	t *testing.T,
) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)

	block, offsets := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	require.NoError(
		t,
		f.process(block, offsets, envelopeParent{origin: true}),
		"an uncertified announcing block must apply as an ordinary block",
	)
	f.requireEndorserEffects(false)
	require.Zero(t, f.certCalls)
}

// A certifying ranking block applies the endorser block its parent announced,
// after the certificate verifies against that parent and the closure's
// transactions pass Dijkstra validation.
func TestLeiosCertifiedRankingBlockAppliesParentEndorserBlock(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	parent := f.announce()
	f.requireEndorserEffects(false)

	certifier, offsets, envParent := f.certifyingChild(parent)
	require.NoError(t, f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	))
	require.NoError(t, f.process(certifier, offsets, envParent))
	f.requireEndorserEffects(true)
	require.Positive(t, f.certCalls)
	require.Equal(t, parent.Hash().Bytes(), f.certAnnHash)
}

func TestLeiosCertifiedClosureAppliesStateBeforeValidatingNextTransaction(
	t *testing.T,
) {
	t.Parallel()
	f := newLeiosCertApplyFixtureWithTransactions(
		t,
		nil,
		pathSignerKey,
		[]pathTx{
			{top: pathLevel{fields: map[uint]any{
				4: []any{pathRegistration(pathSignerKey)},
			}}},
			{top: pathLevel{
				fields: pathDeposits(map[cbor.ByteString]uint64{
					pathKeyAccount(pathSignerKey): leiosCertApplyDeposit,
				}),
				funds: leiosCertApplyDeposit,
			}},
		},
	)
	parent := f.announce()
	certifier, offsets, envParent := f.certifyingChild(parent)
	require.NoError(t, f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	))
	require.NoError(t, f.process(certifier, offsets, envParent))
	account, err := f.db.GetAccountByCredential(
		t.Context(),
		0,
		pathStakeKey(pathSignerKey),
		false,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, leiosCertApplyDeposit, uint64(account.Reward))
}

func TestLeiosCertifiedClosureRejectsMissingWitness(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	f.removeEndorserWitnesses()
	requireRejectedEndorserClosure(t, f)
}

func TestLeiosCertifiedClosureRejectsInvalidWitness(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	f.corruptEndorserWitness()
	requireRejectedEndorserClosure(t, f)
}

func TestLeiosCertifiedClosureMalformedCBORRejectsWithoutEffects(
	t *testing.T,
) {
	t.Parallel()
	badBody, err := cbor.Encode([]any{
		uint64(1),
		map[uint]any{},
		nil,
	})
	require.NoError(t, err)
	malformed := []struct {
		name string
		raw  cbor.RawMessage
	}{
		{name: "truncated envelope", raw: cbor.RawMessage{0x82, 0x01}},
		{name: "invalid body shape", raw: badBody},
	}
	for _, malformedTx := range malformed {
		for _, unticked := range []bool{false, true} {
			pathName := "normal block"
			if unticked {
				pathName = "unticked epoch boundary"
			}
			t.Run(malformedTx.name+"/"+pathName, func(t *testing.T) {
				t.Parallel()
				f := newLeiosCertApplyFixture(t)
				f.ebTxs = []cbor.RawMessage{bytes.Clone(malformedTx.raw)}
				parent := f.announce()
				certifier, offsets, envParent := f.certifyingChild(parent)
				var applyErr error
				if unticked {
					parentPoint := ocommon.Point{
						Slot: parent.SlotNumber(), Hash: parent.Hash().Bytes(),
					}
					applyErr = f.db.Transaction(t.Context(), true).Do(
						func(txn *database.Txn) error {
							return f.ls.applyUntickedBoundaryClosure(
								t.Context(), txn, certifier, parentPoint,
							)
						},
					)
				} else {
					applyErr = f.process(certifier, offsets, envParent)
				}
				var rejected *headerValidationError
				require.ErrorAs(t, applyErr, &rejected)
				require.NotErrorIs(
					t,
					applyErr,
					errCertifiedEndorserBlockUnavailable,
					"malformed peer content must reject instead of retrying",
				)
				f.requireEndorserEffects(false)
				if unticked {
					require.Nil(t, f.ls.untickedClosure)
				}
			})
		}
	}
}

func TestLeiosAnnouncementValidationMarksInvalidWitnessDeterministic(
	t *testing.T,
) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	f.corruptEndorserWitness()
	announcer, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	err := f.ls.ValidateLeiosEndorserBlockTransactions(
		t.Context(),
		announcer.Header(),
		LeiosValidationParentHash(announcer.Header()),
		[][]byte{f.ebTx.Cbor()},
	)
	require.ErrorIs(t, err, ErrInvalidLeiosEndorserBlockTransactions)
}

func TestLeiosAnnouncementValidationKeepsProtocolParamFaultRetryable(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name    string
		pparams lcommon.ProtocolParameters
	}{
		{name: "wrong type", pparams: &conway.ConwayProtocolParameters{}},
		{name: "typed nil", pparams: (*dijkstra.DijkstraProtocolParameters)(nil)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newLeiosCertApplyFixture(t)
			f.ls.currentPParams = tc.pparams
			f.ls.publishSnapshotsLocked()
			announcer, _ := f.block(
				1,
				leiosCertApplyAnnouncingSlot,
				f.originHash,
				false,
				true,
			)
			err := f.ls.ValidateLeiosEndorserBlockTransactions(
				t.Context(),
				announcer.Header(),
				LeiosValidationParentHash(announcer.Header()),
				[][]byte{f.ebTx.Cbor()},
			)
			require.ErrorIs(t, err, eras.ErrIncompatibleProtocolParams)
			require.NotErrorIs(t, err, ErrInvalidLeiosEndorserBlockTransactions)
		})
	}
}

func TestLeiosAnnouncementValidationPropagatesCancellation(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	era := eras.DijkstraEraDesc
	era.ValidateTxFunc = func(
		_ lcommon.Transaction,
		_ uint64,
		state lcommon.LedgerState,
		_ lcommon.ProtocolParameters,
	) error {
		cancel()
		provider, ok := state.(interface{ EvaluationContext() context.Context })
		require.True(t, ok)
		require.ErrorIs(t, provider.EvaluationContext().Err(), context.Canceled)
		return errors.New("deterministic verdict after cancellation")
	}
	f.ls.currentEra = era
	f.ls.activeEras = []eras.EraDesc{era}
	f.ls.publishSnapshotsLocked()
	announcer, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	err := f.ls.ValidateLeiosEndorserBlockTransactions(
		ctx,
		announcer.Header(),
		LeiosValidationParentHash(announcer.Header()),
		[][]byte{f.ebTx.Cbor()},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrInvalidLeiosEndorserBlockTransactions)
}

func TestLeiosCertifiedClosureRejectsValueCreation(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixtureWith(t, pathTx{top: pathLevel{
		fields: pathDeposits(map[cbor.ByteString]uint64{
			pathKeyAccount(0x42): leiosCertApplyDeposit,
		}),
	}})
	requireRejectedEndorserClosure(t, f)
}

func TestLeiosCertifiedClosureRejectsInvalidDirectDeposit(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixtureWith(t, pathTx{top: pathLevel{
		fields: pathDeposits(map[cbor.ByteString]uint64{
			pathKeyAccount(0x43): leiosCertApplyDeposit,
		}),
		funds: leiosCertApplyDeposit,
	}})
	requireRejectedEndorserClosure(t, f)
	_, err := f.db.GetAccountByCredential(
		context.Background(),
		0,
		pathStakeKey(0x43),
		false,
		nil,
	)
	require.ErrorIs(t, err, models.ErrAccountNotFound)
}

func TestLeiosCertifiedClosureRejectsInvalidCachedVerdict(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	f.ls.config.EndorserBlockValidation = func(
		[]byte,
		uint64,
	) EndorserBlockValidationStatus {
		return EndorserBlockValidationInvalid
	}
	parent := f.announce()
	certifier, offsets, envParent := f.certifyingChild(parent)
	err := f.process(certifier, offsets, envParent)
	var validationErr *headerValidationError
	require.ErrorAs(t, err, &validationErr)
	f.requireEndorserEffects(false)
}

func TestLeiosCertifiedClosureWaitsForPendingCachedVerdict(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	f.ls.config.EndorserBlockValidation = func(
		[]byte,
		uint64,
	) EndorserBlockValidationStatus {
		return EndorserBlockValidationPending
	}
	parent := f.announce()
	certifier, offsets, envParent := f.certifyingChild(parent)
	err := f.process(certifier, offsets, envParent)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	var validationErr *headerValidationError
	require.False(t, errors.As(err, &validationErr))
	f.requireEndorserEffects(false)
}

// A certifying ranking block whose certificate does not verify is rejected,
// before any endorser-block work, and its database transaction commits
// nothing.
func TestLeiosInvalidCertificateRejectsRankingBlock(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	parent := f.announce()
	f.certErr = fmt.Errorf(
		"%w: aggregate signature does not verify",
		ErrLeiosInvalidCertificate,
	)

	certifier, offsets, envParent := f.certifyingChild(parent)
	err := f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	)
	require.ErrorIs(t, err, f.certErr)
	var rejected *headerValidationError
	require.ErrorAs(
		t,
		err,
		&rejected,
		"an invalid certificate must reject the block",
	)
	err = f.process(certifier, offsets, envParent)
	require.ErrorIs(t, err, f.certErr)
	require.ErrorAs(
		t,
		err,
		&rejected,
		"an invalid certificate must reject the block",
	)
	require.NotErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	f.requireEndorserEffects(false)
}

// A certificate this node could not check, because the validator failed
// locally rather than returning a verdict, says nothing about the block. It
// must stay a retryable failure, not a rejection that rewinds past a block that
// may be valid, and the block applies once the validator recovers.
func TestLeiosCertificateLocalFailureIsRetried(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	parent := f.announce()
	f.certErr = errors.New("leios vote manager is unavailable")

	certifier, offsets, envParent := f.certifyingChild(parent)
	var rejected *headerValidationError
	err := f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	)
	require.ErrorIs(t, err, f.certErr)
	require.False(
		t,
		errors.As(err, &rejected),
		"a local validator failure must not reject the block: %v",
		err,
	)
	err = f.process(certifier, offsets, envParent)
	require.ErrorIs(t, err, f.certErr)
	require.False(
		t,
		errors.As(err, &rejected),
		"a local validator failure must not reject the block: %v",
		err,
	)
	f.requireEndorserEffects(false)

	f.certErr = nil
	require.NoError(t, f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	))
	require.NoError(t, f.process(certifier, offsets, envParent))
	f.requireEndorserEffects(true)
}

// A certifying ranking block whose closure has not arrived is held back as
// temporarily unavailable rather than committed without the closure's
// effects, as the reference ChainSel.isUnacquiredCertRB does. Once the closure
// arrives the same block applies with its effects.
func TestLeiosCertifiedRankingBlockWaitsForMissingClosure(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	parent := f.announce()
	f.ebAvailable = false

	certifier, offsets, envParent := f.certifyingChild(parent)
	err := f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	err = f.process(certifier, offsets, envParent)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	f.requireEndorserEffects(false)

	f.ebAvailable = true
	require.NoError(t, f.ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	))
	require.NoError(t, f.process(certifier, offsets, envParent))
	f.requireEndorserEffects(true)
}

// A parent whose stored bytes do not decode as a block says nothing about
// what it announced. Reading it as "announced nothing" would reject the
// certifying block as LeiosCertificateWithoutAnnouncement, so it must stay a
// retryable failure to resolve the parent.
func TestLeiosUndecodableParentIsNotAMissingAnnouncement(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	parent, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	require.NoError(t, f.db.BlockCreate(models.Block{
		Slot:     parent.SlotNumber(),
		Hash:     parent.Hash().Bytes(),
		PrevHash: parent.PrevHash().Bytes(),
		Number:   parent.BlockNumber(),
		Type:     gledger.BlockTypeDijkstra,
		Cbor:     []byte{0xff},
	}, nil))

	certifier, _, _ := f.certifyingChild(parent)
	err := f.ls.validateDijkstraLeiosCertificate(t.Context(), certifier, nil)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	var rejected *headerValidationError
	require.False(
		t,
		errors.As(err, &rejected),
		"an undecodable parent must not reject the certifying block: %v",
		err,
	)
	require.Zero(t, f.certCalls)
}
