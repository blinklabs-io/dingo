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
	"crypto/ed25519"
	"errors"
	"io"
	"log/slog"
	"maps"
	"math/big"
	"reflect"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/plutusv4script"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	pathUtxoValue      uint64 = 100_000_000
	pathRefScriptValue uint64 = 5_000_000
	// Each script runs with this many memory and step units, priced at one
	// lovelace per unit; the fee adds a margin for the reference script.
	pathScriptExUnits   int64  = 5_000_000
	pathScriptFee       uint64 = 10_001_000
	pathCollateralValue uint64 = 60_000_000
	pathOriginSlot      uint64 = 1
	pathBlockSlot       uint64 = 10
)

// pathLevel is one transaction body of a batch. fields carries the raw body
// keys under test, encoded exactly as given so the wire form is the oracle
// rather than a struct marshal. funds is the value carved out of the level's
// output to pay for the direct deposits the fields declare; leaving it zero
// builds an underfunded body. withdrawn is the reward balance the body
// withdraws, which the same output absorbs.
//
// script, when set, is a PlutusV4 script that guards the body. It is supplied
// by reference, as V4 requires, and runs as a guarding script with the
// redeemer the fixture adds. guards lists key credentials that sort before it
// in the body's guard set.
type pathLevel struct {
	fields    map[uint]any
	funds     uint64
	withdrawn uint64
	script    lcommon.PlutusV4Script
	guards    []lcommon.Credential
}

// pathTx is one top-level transaction and its subtransactions.
type pathTx struct {
	top      pathLevel
	children []pathLevel
}

// pathAccount is a registered reward account. The key byte repeats into a
// 28-byte credential; pathSignerKey selects the credential of the key that
// signs every fixture transaction, so a body can withdraw from or guard on it.
type pathAccount struct {
	key    byte
	reward uint64
	// script registers a script credential rather than a key credential.
	script bool
}

const pathSignerKey byte = 0

func pathSigner() ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x91}, ed25519.SeedSize))
}

func pathSignerHash() []byte {
	hash := lcommon.Blake2b224Hash(pathSigner().Public().(ed25519.PublicKey))
	return hash[:]
}

// pathFixture holds a Dijkstra ledger with funded inputs and registered
// accounts, plus one block carrying the given transactions. Every production
// path under test runs on its own fixture so no path observes another's
// state.
type pathFixture struct {
	t          *testing.T
	db         *database.Database
	ls         *LedgerState
	txs        []*dijkstra.DijkstraTransaction
	block      *dijkstra.DijkstraBlock
	blockCbor  []byte
	offsets    *database.BlockIngestionResult
	originHash []byte
	pparams    *dijkstra.DijkstraProtocolParameters
}

func pathStakeKey(key byte) []byte {
	if key == pathSignerKey {
		return pathSignerHash()
	}
	return bytes.Repeat([]byte{key}, 28)
}

// pathKeyAccount is the testnet key reward account for a stake key byte.
func pathKeyAccount(key byte) cbor.ByteString {
	return cbor.NewByteString(append([]byte{0xe0}, pathStakeKey(key)...))
}

// pathAccountFor is a reward account with an explicit header byte, for
// wrong-network and script accounts.
func pathAccountFor(header byte, hashByte byte) cbor.ByteString {
	return cbor.NewByteString(append([]byte{header}, pathStakeKey(hashByte)...))
}

func pathOutput(address []byte, amount uint64) map[uint]any {
	return map[uint]any{0: address, 1: amount}
}

func pathSignedWitnesses(
	key ed25519.PrivateKey,
	bodyHash lcommon.Blake2b256,
) map[uint]any {
	return map[uint]any{0: cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      key.Public().(ed25519.PublicKey),
			Signature: ed25519.Sign(key, bodyHash[:]),
		}},
		false,
	)}
}

// newPathFixture builds the ledger, the block carrying blockTxs, and any
// pending transactions that are valid only after that block applies. Pending
// transactions are not in the block.
func newPathFixture(
	t *testing.T,
	accounts []pathAccount,
	blockTxs []pathTx,
	pending ...pathTx,
) *pathFixture {
	t.Helper()
	db := newTestDB(t)
	key := pathSigner()
	paymentHash := lcommon.Blake2b224Hash(key.Public().(ed25519.PublicKey))
	address := append([]byte{0x60}, paymentHash[:]...)
	shelleyAddress, err := lcommon.NewAddressFromBytes(address)
	require.NoError(t, err)

	f := &pathFixture{t: t, db: db}
	f.pparams = newPathProtocolParameters(t)
	seed := func(txID []byte, amount uint64, script lcommon.PlutusV4Script) {
		require.NoError(
			t,
			db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
				if err := db.CreateUtxo(context.Background(), txn, &models.Utxo{
					TxId:       txID,
					OutputIdx:  0,
					PaymentKey: paymentHash.Bytes(),
					AddedSlot:  pathOriginSlot,
					Amount:     dbtypes.Uint64(amount),
				}); err != nil {
					return err
				}
				output := &babbage.BabbageTransactionOutput{
					OutputAddress: shelleyAddress,
					OutputAmount: mary.MaryTransactionOutputValue{
						Amount: amount,
					},
				}
				if script != nil {
					output.TxOutScriptRef = &lcommon.ScriptRef{
						Type:   lcommon.ScriptRefTypePlutusV4,
						Script: script,
					}
				}
				encoded, err := cbor.Encode(output)
				if err != nil {
					return err
				}
				return db.Blob().SetUtxo(txn.Blob(), txID, 0, encoded)
			}),
		)
	}
	for _, account := range accounts {
		tag := uint8(0)
		if account.script {
			tag = 1
		}
		require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey:    pathStakeKey(account.key),
			CredentialTag: tag,
			AddedSlot:     pathOriginSlot,
			Reward:        dbtypes.Uint64(account.reward),
			Active:        true,
		}))
	}

	input := func(seedByte byte, amount uint64) []any {
		txID := bytes.Repeat([]byte{seedByte}, lcommon.Blake2b256Size)
		seed(txID, amount, nil)
		return []any{txID, uint64(0)}
	}
	// encodeLevel returns the body of one level and the witness fields its
	// script needs. Seed bytes base+1.. address the level's own UTxOs: input,
	// reference script and collateral.
	encodeLevel := func(
		level pathLevel,
		seedByte byte,
		fee uint64,
		top bool,
	) (map[uint]any, map[uint]any) {
		body := map[uint]any{
			0: []any{input(seedByte, pathUtxoValue)},
			1: []any{pathOutput(
				address,
				pathUtxoValue-fee-level.funds+level.withdrawn,
			)},
		}
		witnesses := map[uint]any{}
		if top {
			body[2] = fee
		}
		if level.script != nil {
			refID := bytes.Repeat(
				[]byte{seedByte + 0x08},
				lcommon.Blake2b256Size,
			)
			seed(refID, pathRefScriptValue, level.script)
			body[18] = []any{[]any{refID, uint64(0)}}
			guards := append(
				slices.Clone(level.guards),
				plutusv4script.ScriptCredential(level.script),
			)
			body[14] = cbor.NewSetType(guards, true)
			redeemers := map[lcommon.RedeemerKey]lcommon.RedeemerValue{{
				Tag:   lcommon.RedeemerTagGuarding,
				Index: uint32(len(level.guards)), //nolint:gosec
			}: {
				Data: lcommon.Datum{Data: data.NewInteger(big.NewInt(0))},
				ExUnits: lcommon.ExUnits{
					Steps:  pathScriptExUnits,
					Memory: pathScriptExUnits,
				},
			}}
			redeemersCbor, err := cbor.Encode(redeemers)
			require.NoError(t, err)
			langViews, err := lcommon.EncodeLangViews(
				map[uint]struct{}{3: {}},
				f.pparams.CostModels,
			)
			require.NoError(t, err)
			hash := lcommon.Blake2b256Hash(append(redeemersCbor, langViews...))
			body[11] = hash[:]
			witnesses[5] = cbor.RawMessage(redeemersCbor)
		}
		if top && fee > 0 {
			// Phase 2 needs collateral worth a percentage of the fee.
			body[13] = []any{input(seedByte+0x0f, pathCollateralValue)}
			body[17] = pathCollateralValue
		}
		maps.Copy(body, level.fields)
		return body, witnesses
	}
	for index, spec := range append(slices.Clone(blockTxs), pending...) {
		base := byte(0x10 + index*0x10) //nolint:gosec
		fee := uint64(0)
		scripts := 0
		for _, level := range append([]pathLevel{spec.top}, spec.children...) {
			if level.script != nil {
				scripts++
			}
		}
		if scripts > 0 {
			fee = pathScriptFee * uint64(scripts) //nolint:gosec
		}
		children := make([]cbor.RawMessage, 0, len(spec.children))
		for childIndex, child := range spec.children {
			childBody, childWitnesses := encodeLevel(
				child,
				base+byte(childIndex)*0x01+1, //nolint:gosec
				0,
				false,
			)
			bodyCbor, err := cbor.Encode(childBody)
			require.NoError(t, err)
			witnesses := pathSignedWitnesses(
				key,
				lcommon.Blake2b256Hash(bodyCbor),
			)
			maps.Copy(witnesses, childWitnesses)
			childCbor, err := cbor.Encode([]any{
				cbor.RawMessage(bodyCbor),
				witnesses,
				nil,
			})
			require.NoError(t, err)
			children = append(children, childCbor)
		}
		topBody, topWitnesses := encodeLevel(spec.top, base, fee, true)
		if len(children) > 0 {
			topBody[23] = cbor.NewSetType(children, true)
		}
		bodyCbor, err := cbor.Encode(topBody)
		require.NoError(t, err)
		witnesses := pathSignedWitnesses(key, lcommon.Blake2b256Hash(bodyCbor))
		maps.Copy(witnesses, topWitnesses)
		txCbor, err := cbor.Encode([]any{
			cbor.RawMessage(bodyCbor),
			witnesses,
			nil,
		})
		require.NoError(t, err)
		decoded, err := gledger.NewTransactionFromCbor(
			gledger.TxTypeDijkstra,
			txCbor,
		)
		require.NoError(t, err)
		tx, ok := decoded.(*dijkstra.DijkstraTransaction)
		require.True(t, ok)
		f.txs = append(f.txs, tx)
	}

	f.originHash = bytes.Repeat([]byte{0xf1}, lcommon.Blake2b256Size)
	originTip := ochainsync.Tip{
		Point: ocommon.Point{Slot: pathOriginSlot, Hash: f.originHash},
	}
	require.NoError(t, db.SetTip(originTip, nil))
	config := newTestShelleyGenesisCfg(t)
	config.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{eras.DijkstraEraDesc},
		currentEra: eras.DijkstraEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1,
			LengthInSlots: 1_000,
			EraId:         eras.DijkstraEraDesc.Id,
		},
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1,
			LengthInSlots: 1_000,
			EraId:         eras.DijkstraEraDesc.Id,
		}},
		currentPParams: f.pparams,
		currentTip:     originTip,
		currentTipBlockNonce: bytes.Repeat(
			[]byte{0xf2},
			lcommon.Blake2b256Size,
		),
		validationEnabled: true,
		config: LedgerStateConfig{
			CardanoNodeConfig: config,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	// Rollback reloads the era, epochs and protocol parameters from the
	// database, so persist the ones the in-memory ledger above holds.
	fillUnsetRats(reflect.ValueOf(f.pparams))
	pparamsCbor, err := cbor.Encode(f.pparams)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, 0, 0, eras.DijkstraEraDesc.Id, nil,
	))
	require.NoError(t, db.SetEpoch(
		0, 0, nil, nil, nil, nil, eras.DijkstraEraDesc.Id, 1, 1_000, nil,
	))
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()
	f.ls = ls

	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        pathBlockSlot,
					PrevHash:    lcommon.NewBlake2b256(f.originHash),
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
				},
			},
		},
	}
	for _, tx := range f.txs[:len(blockTxs)] {
		block.BlockBody.Transactions = append(block.BlockBody.Transactions, *tx)
	}
	blockBodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(blockBodyCbor))
	f.blockCbor, err = block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(f.blockCbor)
	f.block = block
	f.offsets, err = database.NewBlockIndexer(pathBlockSlot, block.Hash().Bytes()).
		ComputeOffsets(f.blockCbor, block)
	require.NoError(t, err)
	return f
}

func (f *pathFixture) point() ocommon.Point {
	return ocommon.Point{Slot: pathBlockSlot, Hash: f.block.Hash().Bytes()}
}

// storeOriginAndBlock persists the origin block that rollback returns to and
// the block under test, the state a node is in when it replays or rolls back.
func (f *pathFixture) storeOriginAndBlock(withBlock bool) {
	f.t.Helper()
	require.NoError(f.t, f.db.BlockCreate(models.Block{
		Slot: pathOriginSlot,
		Hash: f.originHash,
		Type: gledger.BlockTypeDijkstra,
	}, nil))
	require.NoError(f.t, f.db.SetBlockNonce(
		f.originHash,
		pathOriginSlot,
		bytes.Repeat([]byte{0xf2}, lcommon.Blake2b256Size),
		true,
		nil,
	))
	if !withBlock {
		return
	}
	require.NoError(f.t, f.db.BlockCreate(models.Block{
		Slot:     pathBlockSlot,
		Hash:     f.point().Hash,
		PrevHash: f.originHash,
		Number:   f.block.BlockNumber(),
		Type:     gledger.BlockTypeDijkstra,
		Cbor:     f.blockCbor,
	}, nil))
}

// admit validates the transaction the way the node does before accepting it
// from a peer or a client.
func (f *pathFixture) admit(index int) error {
	return f.ls.ValidateTx(f.txs[index])
}

// mempoolAdd submits the transaction's wire bytes to a mempool whose
// validator is the ledger.
func (f *pathFixture) mempoolAdd(index int) error {
	f.t.Helper()
	return f.mempoolAddRaw(f.txs[index].Cbor())
}

func (f *pathFixture) mempoolAddRaw(txCbor []byte) error {
	f.t.Helper()
	pool, err := mempool.NewMempool(mempool.MempoolConfig{
		Validator:       f.ls,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry:    prometheus.NewRegistry(),
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(f.t, err)
	return pool.AddTransaction(uint(gledger.TxTypeDijkstra), txCbor)
}

// withBodyField returns the transaction at index, and the block carrying it,
// with one body key replaced by raw CBOR. child selects a subtransaction body,
// or the top-level body when negative. The result is not a valid transaction:
// only its decoding is of interest.
func (f *pathFixture) withBodyField(
	index, child int,
	key uint,
	field cbor.RawMessage,
) (txCbor, blockCbor []byte) {
	f.t.Helper()
	patch := func(bodyCbor cbor.RawMessage) cbor.RawMessage {
		var body map[uint]cbor.RawMessage
		_, err := cbor.Decode(bodyCbor, &body)
		require.NoError(f.t, err)
		body[key] = field
		patched, err := cbor.Encode(body)
		require.NoError(f.t, err)
		return patched
	}
	var parts []cbor.RawMessage
	_, err := cbor.Decode(f.txs[index].Cbor(), &parts)
	require.NoError(f.t, err)
	if child < 0 {
		parts[0] = patch(parts[0])
	} else {
		var body map[uint]cbor.RawMessage
		_, err = cbor.Decode(parts[0], &body)
		require.NoError(f.t, err)
		var children cbor.SetType[cbor.RawMessage]
		_, err = cbor.Decode(body[23], &children)
		require.NoError(f.t, err)
		items := slices.Clone(children.Items())
		var childParts []cbor.RawMessage
		_, err = cbor.Decode(items[child], &childParts)
		require.NoError(f.t, err)
		childParts[0] = patch(childParts[0])
		items[child], err = cbor.Encode(childParts)
		require.NoError(f.t, err)
		body[23], err = cbor.Encode(cbor.NewSetType(items, true))
		require.NoError(f.t, err)
		parts[0], err = cbor.Encode(body)
		require.NoError(f.t, err)
	}
	txCbor, err = cbor.Encode(parts)
	require.NoError(f.t, err)

	var blockParts []cbor.RawMessage
	_, err = cbor.Decode(f.blockCbor, &blockParts)
	require.NoError(f.t, err)
	var blockBody []cbor.RawMessage
	_, err = cbor.Decode(blockParts[1], &blockBody)
	require.NoError(f.t, err)
	var txs []cbor.RawMessage
	_, err = cbor.Decode(blockBody[0], &txs)
	require.NoError(f.t, err)
	// A block carries each transaction as [body, witnesses, auxiliary data,
	// is_valid], extending the standalone three-field transaction.
	txs[index], err = cbor.Encode(
		[]any{parts[0], parts[1], parts[2], true},
	)
	require.NoError(f.t, err)
	blockBody[0], err = cbor.Encode(txs)
	require.NoError(f.t, err)
	blockParts[1], err = cbor.Encode(blockBody)
	require.NoError(f.t, err)
	blockCbor, err = cbor.Encode(blockParts)
	require.NoError(f.t, err)
	return txCbor, blockCbor
}

// applyBlock runs the block through live block processing.
func (f *pathFixture) applyBlock() error {
	return f.db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		_, err := f.ls.ledgerProcessBlock(
			context.Background(),
			txn,
			f.point(),
			f.block,
			true,
			false,
			false,
			f.originHash,
			envelopeParent{origin: true},
			f.offsets,
			eras.DijkstraEraDesc,
			f.pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
}

// replayBlock feeds the stored block through the historical replay reader.
func (f *pathFixture) replayBlock() error {
	f.t.Helper()
	f.storeOriginAndBlock(true)
	results := make(chan readChainResult, 1)
	done := make(chan struct{})
	results <- readChainResult{blocks: []gledger.Block{f.block}, done: done}
	close(results)
	err := f.ls.ledgerProcessBlocksFromSource(f.t.Context(), results)
	select {
	case <-done:
	default:
		f.t.Fatal("reader result was not released after replay")
	}
	return err
}

func (f *pathFixture) reward(key byte) uint64 {
	f.t.Helper()
	return f.rewardOf(0, key)
}

// rewardOf is the balance of the key (tag 0) or script (tag 1) account.
func (f *pathFixture) rewardOf(tag uint8, key byte) uint64 {
	f.t.Helper()
	account, err := f.db.GetAccountByCredential(
		context.Background(),
		tag,
		pathStakeKey(key),
		false,
		nil,
	)
	require.NoError(f.t, err)
	return uint64(account.Reward)
}

// inputUnspent reports whether every spending and collateral input at every
// transaction level is still unspent.
func (f *pathFixture) inputUnspent(index int) bool {
	f.t.Helper()
	tx := f.txs[index]
	inputs := append(slices.Clone(tx.Inputs()), tx.Collateral()...)
	for _, child := range tx.Body.TxSubTransactions.Items() {
		childInputs := child.Body.TxInputs.Items()
		for i := range childInputs {
			inputs = append(inputs, &childInputs[i])
		}
	}
	for _, input := range inputs {
		utxo, err := f.db.Metadata().GetUtxo(
			input.Id().Bytes(), input.Index(), nil,
		)
		require.NoError(f.t, err)
		if utxo == nil {
			return false
		}
	}
	return true
}

// pathAccountID identifies a reward account by credential tag and hash.
type pathAccountID struct {
	tag  uint
	hash lcommon.Blake2b224
}

func (a pathAccount) id() pathAccountID {
	id := pathAccountID{hash: lcommon.NewBlake2b224(pathStakeKey(a.key))}
	if a.script {
		id.tag = lcommon.CredentialTypeScriptHash
	}
	return id
}

// registeredAccounts lists accounts registered at every transaction level.
func (f *pathFixture) registeredAccounts(index int) []pathAccountID {
	tx := f.txs[index]
	certs := slices.Clone(tx.Certificates())
	for _, child := range tx.Body.TxSubTransactions.Items() {
		certs = append(certs, child.Body.Certificates()...)
	}
	var ids []pathAccountID
	for _, cert := range certs {
		var cred lcommon.Credential
		switch c := cert.(type) {
		case *lcommon.StakeRegistrationCertificate:
			cred = c.StakeCredential
		case *lcommon.RegistrationCertificate:
			cred = c.StakeCredential
		case *lcommon.StakeRegistrationDelegationCertificate:
			cred = c.StakeCredential
		case *lcommon.VoteRegistrationDelegationCertificate:
			cred = c.StakeCredential
		case *lcommon.StakeVoteRegistrationDelegationCertificate:
			cred = c.StakeCredential
		default:
			continue
		}
		ids = append(ids, pathAccountID{tag: cred.CredType, hash: cred.Credential})
	}
	return ids
}

// accountPresent reports whether the reward account exists.
func (f *pathFixture) accountPresent(id pathAccountID) bool {
	f.t.Helper()
	_, err := f.db.GetAccountByCredential(
		context.Background(),
		uint8(id.tag), id.hash.Bytes(), false, nil,
	)
	if errors.Is(err, models.ErrAccountNotFound) {
		return false
	}
	require.NoError(f.t, err)
	return true
}

// fillUnsetRats gives every unset rational in v the value zero. A protocol
// parameter set with a nil rational cannot be persisted, and rollback reloads
// the persisted set.
func fillUnsetRats(v reflect.Value) {
	ratType := reflect.TypeFor[cbor.Rat]()
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			if v.Type().Elem() != ratType {
				return
			}
			v.Set(reflect.ValueOf(&cbor.Rat{}))
		}
		fillUnsetRats(v.Elem())
	case reflect.Struct:
		if v.Type() == ratType {
			if rat := v.Addr().Interface().(*cbor.Rat); rat.Rat == nil {
				rat.Rat = big.NewRat(0, 1)
			}
			return
		}
		for field := range v.NumField() {
			if v.Type().Field(field).IsExported() {
				fillUnsetRats(v.Field(field))
			}
		}
	}
}

// newPathProtocolParameters are Dijkstra parameters that admit PlutusV4
// scripts supplied by reference and priced at one lovelace per unit.
func newPathProtocolParameters(
	t *testing.T,
) *dijkstra.DijkstraProtocolParameters {
	t.Helper()
	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	pparams.MinFeeRefScriptCostPerByte = &cbor.Rat{Rat: big.NewRat(1, 1)}
	pparams.MaxTxExUnits = lcommon.ExUnits{
		Steps:  50_000_000,
		Memory: 50_000_000,
	}
	pparams.MaxBlockExUnits = pparams.MaxTxExUnits
	pparams.ExecutionCosts = lcommon.ExUnitPrice{
		MemPrice:  &cbor.Rat{Rat: big.NewRat(1, 1)},
		StepPrice: &cbor.Rat{Rat: big.NewRat(1, 1)},
	}
	pparams.CostModels = map[uint][]int64{
		3: blockV3MachineCostModel(t, lang.LanguageVersionV4),
	}
	pparams.MaxRefScriptSizePerTx = 100_000
	pparams.MaxRefScriptSizePerBlock = 1_000_000
	pparams.RefScriptCostStride = 25_600
	pparams.RefScriptCostMultiplier = &cbor.Rat{Rat: big.NewRat(6, 5)}
	return pparams
}
