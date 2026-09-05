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

package sqlite

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The account that wedged a from-genesis Preview replay at epoch 111, and the
// pointer address it held most of its funds at. The pointer tail 81b3bb010100
// decodes to (2940289, 1, 0) -- the position of the stake registration
// certificate that registered the credential.
const (
	wedgePointerAddress = "addr_test1gzgj6rad2h398mvgv59zcnrrq0x2adcftl6647ukcp7masupkwaszqgqjupejx"
	wedgePointerSlot    = 2_940_289
	wedgePointerTxIndex = 1
	wedgeCertIndex      = 0
)

// pointerVarNat encodes one component of a pointer address's tail: a
// big-endian base-128 natural whose non-final bytes carry the high bit.
func pointerVarNat(value uint64) []byte {
	ret := []byte{byte(value & 0x7f)}
	for value >>= 7; value > 0; value >>= 7 {
		ret = append([]byte{byte(value&0x7f) | 0x80}, ret...)
	}
	return ret
}

// newPointerAddress builds a testnet type-4 (pointer, key payment) address
// naming the certificate at (slot, txIndex, certIndex).
func newPointerAddress(
	t *testing.T,
	paymentKey []byte,
	slot, txIndex, certIndex uint64,
) lcommon.Address {
	t.Helper()
	raw := []byte{0x40}
	raw = append(raw, paymentKey...)
	raw = append(raw, pointerVarNat(slot)...)
	raw = append(raw, pointerVarNat(txIndex)...)
	raw = append(raw, pointerVarNat(certIndex)...)
	addr, err := lcommon.NewAddressFromBytes(raw)
	require.NoError(t, err)
	require.IsType(t, lcommon.AddressPayloadPointer{}, addr.StakingPayload(),
		"fixture must be a pointer address")
	return addr
}

type pointerStakeFixture struct {
	store    *sqlstore.Store
	pool     []byte
	stakeKey lcommon.CredentialHash
	nextTx   byte
}

func newPointerStakeFixture(t *testing.T) *pointerStakeFixture {
	t.Helper()
	store, _ := newSharedSQLStore(t)
	return &pointerStakeFixture{
		store: store,
		pool:  bytes.Repeat([]byte{0xF1}, 28),
		stakeKey: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{0x31}, lcommon.AddressHashSize),
		),
		nextTx: 1,
	}
}

// setEra records the epoch containing every slot the fixture uses. The stake
// query resolves the era from this row; the length is deliberately wide enough
// to cover the whole fixture.
func (f *pointerStakeFixture) setEra(t *testing.T, eraID uint) {
	t.Helper()
	require.NoError(t, f.store.SetEpoch(
		0, 0, nil, nil, nil, nil, eraID, 1, 1_000_000, nil,
	))
}

// apply writes one transaction as block index blockIndex of the block at slot,
// with the given certificates and produced outputs.
func (f *pointerStakeFixture) apply(
	t *testing.T,
	slot uint64,
	blockIndex uint32,
	certificates []lcommon.Certificate,
	outputs ...lcommon.TransactionOutput,
) {
	t.Helper()
	hash := lcommon.Blake2b256{}
	hash[0] = f.nextTx
	f.nextTx++
	produced := make([]lcommon.Utxo, 0, len(outputs))
	for i, output := range outputs {
		produced = append(produced, lcommon.Utxo{
			Id: mockTransactionInput{
				hash:  hash,
				index: uint32(i), //nolint:gosec // small test index
			},
			Output: output,
		})
	}
	deposits := make(map[int]uint64, len(certificates))
	for i := range certificates {
		deposits[i] = 0
	}
	require.NoError(t, f.store.SetTransaction(
		&mockTransaction{
			hash:         hash,
			isValid:      true,
			certificates: certificates,
			produced:     produced,
		},
		ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{0xc1}, 32)},
		blockIndex,
		deposits,
		false,
		nil,
	))
}

// spend consumes an input at slot, marking the output it names deleted.
func (f *pointerStakeFixture) spend(
	t *testing.T,
	slot uint64,
	input lcommon.TransactionInput,
) {
	t.Helper()
	hash := lcommon.Blake2b256{}
	hash[0] = f.nextTx
	f.nextTx++
	require.NoError(t, f.store.SetTransaction(
		&mockTransaction{
			hash:     hash,
			isValid:  true,
			consumed: []lcommon.TransactionInput{input},
		},
		ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{0xc2}, 32)},
		0,
		nil,
		false,
		nil,
	))
}

func (f *pointerStakeFixture) register() lcommon.Certificate {
	return &lcommon.StakeRegistrationCertificate{
		CertType: uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: lcommon.Credential{
			CredType: 0, Credential: f.stakeKey,
		},
	}
}

func (f *pointerStakeFixture) deregister() lcommon.Certificate {
	return &lcommon.StakeDeregistrationCertificate{
		CertType: uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: lcommon.Credential{
			CredType: 0, Credential: f.stakeKey,
		},
	}
}

func (f *pointerStakeFixture) delegate() lcommon.Certificate {
	credential := lcommon.Credential{CredType: 0, Credential: f.stakeKey}
	return &lcommon.StakeDelegationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDelegation),
		StakeCredential: &credential,
		PoolKeyHash:     lcommon.PoolKeyHash(f.pool),
	}
}

func (f *pointerStakeFixture) output(
	amount int64,
	address lcommon.Address,
) lcommon.TransactionOutput {
	return &mockTransactionOutput{
		amount:  big.NewInt(amount),
		address: address,
	}
}

func (f *pointerStakeFixture) baseAddress(t *testing.T) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x21}, lcommon.AddressHashSize),
		f.stakeKey.Bytes(),
	)
	require.NoError(t, err)
	return addr
}

func (f *pointerStakeFixture) stakeAt(t *testing.T, slot uint64) uint64 {
	t.Helper()
	stakes, _, err := f.store.GetStakeByPoolsAtSlot(
		[][]byte{f.pool}, slot, 0, 0, nil,
	)
	require.NoError(t, err)
	return stakes[string(f.pool)]
}

// TestPointerAddressStakeReachesItsCredential is the dingo #3854 regression,
// driven end to end through Store.SetTransaction and the historical stake
// query rather than against the resolver in isolation.
//
// A pointer address carries the position of a stake registration certificate
// instead of a credential, so the produced utxo row has no staking_key and the
// output's value never reached the stake distribution -- understating the
// producing pool's stake until the node rejected a canonical block.
func TestPointerAddressStakeReachesItsCredential(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	// The registration this pointer names, at (100, 0, 0), plus the
	// delegation that puts the credential under the pool.
	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	// A base-address output for the same credential, so the assertion
	// isolates the pointer's contribution rather than the whole query.
	f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
	f.apply(t, 200, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	require.Equal(t, uint64(1_300), f.stakeAt(t, 300),
		"stake held at a pointer address must reach the credential the "+
			"pointer designates")
}

// TestPointerAddressStakeRejectsMismatchedPositions pins each of the three
// pointer components. A resolution that ignored any one of them would credit a
// pool with stake the ledger does not.
func TestPointerAddressStakeRejectsMismatchedPositions(t *testing.T) {
	t.Parallel()
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	for _, tc := range []struct {
		name                     string
		slot, txIndex, certIndex uint64
		want                     uint64
	}{
		{name: "the named position resolves", slot: 100, want: 1_300},
		{name: "a different slot does not", slot: 101, want: 700},
		{
			name: "a different transaction index does not",
			slot: 100, txIndex: 1, want: 700,
		},
		{
			name: "a different certificate index does not",
			slot: 100, certIndex: 1, want: 700,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPointerStakeFixture(t)
			f.setEra(t, babbage.EraIdBabbage)
			// Registration at (100, 0, 0); the delegation follows it in the
			// same transaction, at certificate index 1.
			f.apply(
				t, 100, 0,
				[]lcommon.Certificate{f.register(), f.delegate()},
			)
			f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
			f.apply(t, 200, 0, nil, f.output(600, newPointerAddress(
				t, paymentKey, tc.slot, tc.txIndex, tc.certIndex,
			)))
			require.Equal(t, tc.want, f.stakeAt(t, 300))
		})
	}
}

// TestPointerAddressStakeIndexesEveryCertificate covers the highest-value
// invariant in the resolution: a pointer's third component counts every
// certificate in the transaction, not only the registrations. The reference
// mints the Ptr with CertIx (length gamma), gamma being all certificates
// processed so far.
//
// The registration here is the second certificate of its transaction, so it
// sits at index 1 and index 0 holds an unrelated delegation.
func TestPointerAddressStakeIndexesEveryCertificate(t *testing.T) {
	t.Parallel()
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	otherKey := bytes.Repeat([]byte{0x41}, lcommon.AddressHashSize)
	for _, tc := range []struct {
		name      string
		certIndex uint64
		want      uint64
	}{
		{
			name:      "the registration is at the index counting all certificates",
			certIndex: 1,
			want:      1_300,
		},
		{
			name:      "the index of the preceding delegation resolves to nothing",
			certIndex: 0,
			want:      700,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPointerStakeFixture(t)
			f.setEra(t, babbage.EraIdBabbage)
			otherCredential := lcommon.Credential{
				CredType:   0,
				Credential: lcommon.NewBlake2b224(otherKey),
			}
			// Certificate 0 is another account's delegation; the registration
			// under test is certificate 1.
			f.apply(t, 100, 0, []lcommon.Certificate{
				&lcommon.StakeDelegationCertificate{
					CertType: uint(
						lcommon.CertificateTypeStakeDelegation,
					),
					StakeCredential: &otherCredential,
					PoolKeyHash:     lcommon.PoolKeyHash(f.pool),
				},
				f.register(),
			})
			// The delegation of the credential under test follows, so its
			// position is after the registration's.
			f.apply(t, 120, 0, []lcommon.Certificate{f.delegate()})
			f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
			f.apply(t, 200, 0, nil, f.output(600, newPointerAddress(
				t, paymentKey, 100, 0, tc.certIndex,
			)))
			require.Equal(t, tc.want, f.stakeAt(t, 300))
		})
	}
}

// TestPointerAddressStakeIsEraGated covers the Conway divergence. Shelley
// through Babbage carry sisPtrStake alongside the credential map, so pointer
// stake counts. ConwayInstantStake has no pointer map at all and the
// Babbage->Conway translation drops saPtrs, so pointer stake stops counting for
// every such output -- including one produced long before the fork, which is
// the case every network is in today.
func TestPointerAddressStakeIsEraGated(t *testing.T) {
	t.Parallel()
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	for _, tc := range []struct {
		name  string
		eraID uint
		want  uint64
	}{
		{name: "babbage counts pointer stake", eraID: babbage.EraIdBabbage, want: 1_300},
		{name: "conway does not", eraID: conway.EraIdConway, want: 700},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPointerStakeFixture(t)
			f.setEra(t, tc.eraID)
			f.apply(
				t, 100, 0,
				[]lcommon.Certificate{f.register(), f.delegate()},
			)
			f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
			f.apply(t, 200, 0, nil, f.output(600, newPointerAddress(
				t, paymentKey, 100, 0, 0,
			)))
			require.Equal(t, tc.want, f.stakeAt(t, 300))
		})
	}
}

// TestPointerAddressStakeCountsAForwardPointer covers a pointer that names a
// position no certificate occupies yet. Nothing in any era validates an
// address's pointer payload, so an output may be produced first; the reference
// resolves the Ptr at snapshot time, so the stake starts counting once the
// registration lands.
func TestPointerAddressStakeCountsAForwardPointer(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	// The output comes first, naming a position that does not exist yet.
	f.apply(t, 100, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 200, 0, 0)))
	require.Equal(t, uint64(0), f.stakeAt(t, 150),
		"a pointer naming no certificate confers no stake")

	// The registration then lands at exactly that position.
	f.apply(t, 200, 0, []lcommon.Certificate{f.register(), f.delegate()})
	require.Equal(t, uint64(600), f.stakeAt(t, 300),
		"the pointer must resolve once its certificate is on chain")
}

// TestPointerAddressStakeStopsWhenTheOutputIsSpent pins the liveness bound on
// the pointer branch. Stake is held by the output, not by the pointer, so an
// output spent before the evaluated slot confers none -- and one spent after it
// still does.
func TestPointerAddressStakeStopsWhenTheOutputIsSpent(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	pointerTx := f.nextTx
	f.apply(t, 150, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	spentHash := lcommon.Blake2b256{}
	spentHash[0] = pointerTx
	f.spend(t, 200, mockTransactionInput{hash: spentHash, index: 0})

	require.Equal(t, uint64(600), f.stakeAt(t, 175),
		"the output was still live at this slot")
	require.Equal(t, uint64(0), f.stakeAt(t, 250),
		"a spent pointer output confers no stake")
}

// TestPointerAddressStakeStopsAtDeregistration covers removePtr: de-registering
// the credential deletes the Ptr, so the address is permanently dangling. A
// later re-registration mints a Ptr at a new position, which the old address
// does not name, so it must not revive the old one.
func TestPointerAddressStakeStopsAtDeregistration(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	f.apply(t, 150, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))
	require.Equal(t, uint64(600), f.stakeAt(t, 175),
		"the pointer resolves while its registration stands")

	f.apply(t, 200, 0, []lcommon.Certificate{f.deregister()})
	require.Equal(t, uint64(0), f.stakeAt(t, 225),
		"a de-registered credential holds no stake at all")

	// Re-registered at a new position, so the credential is delegated again --
	// but the address names the old, removed Ptr.
	f.apply(t, 250, 0, []lcommon.Certificate{f.register(), f.delegate()})
	f.apply(t, 260, 0, nil, f.output(700, f.baseAddress(t)))
	require.Equal(t, uint64(700), f.stakeAt(t, 300),
		"a re-registration mints a new Ptr; the old address stays dangling")
}

// TestPointerAddressStakeResolvesTheWedgeAddress runs the real bech32 address
// from the Preview wedge, so the fixture is the on-chain encoding rather than
// this test's own pointer writer.
func TestPointerAddressStakeResolvesTheWedgeAddress(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	addr, err := lcommon.NewAddress(wedgePointerAddress)
	require.NoError(t, err)
	pointer, ok := addr.StakingPayload().(lcommon.AddressPayloadPointer)
	require.True(t, ok)
	require.Equal(t, uint64(wedgePointerSlot), pointer.Slot)
	require.Equal(t, uint64(wedgePointerTxIndex), pointer.TxIndex)
	require.Equal(t, uint64(wedgeCertIndex), pointer.CertIndex)

	// The registration at (2940289, 1, 0): transaction index 1 of that block.
	f.apply(
		t, wedgePointerSlot, wedgePointerTxIndex,
		[]lcommon.Certificate{f.register(), f.delegate()},
	)
	f.apply(t, wedgePointerSlot+1, 0, nil, f.output(35_553_515_656, addr))

	require.Equal(t, uint64(35_553_515_656), f.stakeAt(t, wedgePointerSlot+10))
}
