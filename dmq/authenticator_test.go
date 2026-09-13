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

package dmq

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const testSlotsPerKesPeriod = 129600

// fakeStakeAuthority is an in-memory StakeAuthority for tests.
type fakeStakeAuthority struct {
	stakes map[lcommon.PoolKeyHash]uint64
}

func (f *fakeStakeAuthority) PoolActiveStake(
	poolKeyHash lcommon.PoolKeyHash,
) (uint64, error) {
	return f.stakes[poolKeyHash], nil
}

// erroringStakeAuthority always fails the lookup, for testing propagation of
// a stake-lookup error.
type erroringStakeAuthority struct{ err error }

func (e *erroringStakeAuthority) PoolActiveStake(
	lcommon.PoolKeyHash,
) (uint64, error) {
	return 0, e.err
}

// signedMessageParams controls how validMessage builds and signs a test
// message, so individual fields can be tampered with by the caller after
// construction, or the opcert/message KES periods can be set apart to
// exercise the evolution-offset math.
type signedMessageParams struct {
	coldSeed         [32]byte
	kesSeed          [32]byte
	issueNumber      uint64
	certKESPeriod    uint64
	messageKESPeriod uint64
	expiresAt        uint32
	body             []byte
}

// validMessage builds a fully and correctly signed DmqMessage: a real
// ed25519 cold-key signature over the OCertSignable representation of the
// operational certificate, and a real KES signature (at evolution
// messageKESPeriod-certKESPeriod) over the CBOR-wrapped payload, matching
// Authenticator.Verify's expectations byte-for-byte. It returns the message
// and the pool key hash (Blake2b-224 of the cold verification key) callers
// use to configure a fakeStakeAuthority.
func validMessage(
	t *testing.T,
	p signedMessageParams,
) (ocommon.DmqMessage, lcommon.PoolKeyHash) {
	t.Helper()

	coldPriv := ed25519.NewKeyFromSeed(p.coldSeed[:])
	coldPub, ok := coldPriv.Public().(ed25519.PublicKey)
	require.True(t, ok)

	kesSk, kesPk, err := kes.KeyGen(kes.CardanoKesDepth, p.kesSeed[:])
	require.NoError(t, err)

	payload := ocommon.DmqMessagePayload{
		MessageBody: p.body,
		KESPeriod:   p.messageKESPeriod,
		ExpiresAt:   p.expiresAt,
	}
	messageID, err := ocommon.ComputeDmqMessageID(payload)
	require.NoError(t, err)

	payloadCbor, err := cbor.Encode(payload)
	require.NoError(t, err)
	wrappedCbor, err := cbor.Encode(payloadCbor)
	require.NoError(t, err)

	require.GreaterOrEqual(t, p.messageKESPeriod, p.certKESPeriod)
	evolution := p.messageKESPeriod - p.certKESPeriod
	kesSig, err := kes.Sign(kesSk, evolution, wrappedCbor)
	require.NoError(t, err)

	opCertSignable := lcommon.OpCertSignableBytes(
		kesPk,
		p.issueNumber,
		p.certKESPeriod,
	)
	coldSig := ed25519.Sign(coldPriv, opCertSignable)

	msg := ocommon.DmqMessage{
		MessageID:    messageID,
		Payload:      payload,
		KESSignature: kesSig,
		OperationalCertificate: ocommon.OperationalCertificate{
			KESVerificationKey: kesPk,
			IssueNumber:        p.issueNumber,
			KESPeriod:          p.certKESPeriod,
			ColdSignature:      coldSig,
		},
		ColdVerificationKey: coldPub,
	}
	return msg, lcommon.Blake2b224Hash(coldPub)
}

func defaultParams(t *testing.T, seedByte byte) signedMessageParams {
	t.Helper()
	var coldSeed, kesSeed [32]byte
	for i := range coldSeed {
		coldSeed[i] = seedByte
		kesSeed[i] = seedByte ^ 0xAA
	}
	return signedMessageParams{
		coldSeed:         coldSeed,
		kesSeed:          kesSeed,
		issueNumber:      1,
		certKESPeriod:    0,
		messageKESPeriod: 0,
		expiresAt:        farFutureExpiry(t),
		body:             []byte("dmq phase 2 authentication"),
	}
}

func farFutureExpiry(t *testing.T) uint32 {
	t.Helper()
	// #nosec G115 -- test fixture timestamp, far from the uint32 rollover
	return uint32(time.Now().Add(time.Hour).Unix())
}

func newTestAuthenticator(
	t *testing.T,
	authority StakeAuthority,
) *Authenticator {
	t.Helper()
	auth, err := NewAuthenticator(AuthenticatorConfig{
		StakeAuthority:    authority,
		SlotsPerKESPeriod: testSlotsPerKesPeriod,
	})
	require.NoError(t, err)
	return auth
}

func TestNewAuthenticator_RequiresStakeAuthority(t *testing.T) {
	t.Parallel()
	_, err := NewAuthenticator(AuthenticatorConfig{
		SlotsPerKESPeriod: testSlotsPerKesPeriod,
	})
	require.ErrorIs(t, err, ErrAuthenticatorMisconfigured)
}

func TestNewAuthenticator_RequiresSlotsPerKESPeriod(t *testing.T) {
	t.Parallel()
	_, err := NewAuthenticator(AuthenticatorConfig{
		StakeAuthority: &fakeStakeAuthority{},
	})
	require.ErrorIs(t, err, ErrAuthenticatorMisconfigured)
}

func TestAuthenticator_Verify_AcceptsFullyValidMessage(t *testing.T) {
	t.Parallel()
	msg, poolKeyHash := validMessage(t, defaultParams(t, 0x01))
	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	err := auth.Verify(&msg)
	require.NoError(t, err)
}

func TestAuthenticator_Verify_RejectsNilMessage(t *testing.T) {
	t.Parallel()
	auth := newTestAuthenticator(t, &fakeStakeAuthority{})
	require.Error(t, auth.Verify(nil))
}

func TestAuthenticator_Verify_RejectsExpiredMessage(t *testing.T) {
	t.Parallel()
	params := defaultParams(t, 0x02)
	params.expiresAt = 1 // long past
	msg, poolKeyHash := validMessage(t, params)
	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	err := auth.Verify(&msg)
	require.ErrorIs(t, err, ErrMessageExpired)
}

func TestAuthenticator_Verify_RejectsMessageIDMismatch(t *testing.T) {
	t.Parallel()
	msg, poolKeyHash := validMessage(t, defaultParams(t, 0x03))
	msg.MessageID[0] ^= 0xFF
	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	err := auth.Verify(&msg)
	require.ErrorIs(t, err, ErrMessageIDMismatch)
}

func TestAuthenticator_Verify_RejectsTamperedOpCertSignature(t *testing.T) {
	t.Parallel()
	msg, poolKeyHash := validMessage(t, defaultParams(t, 0x04))
	msg.OperationalCertificate.ColdSignature[0] ^= 0xFF
	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	err := auth.Verify(&msg)
	require.ErrorIs(t, err, ErrOpCertInvalid)
}

func TestAuthenticator_Verify_RejectsTamperedKESSignature(t *testing.T) {
	t.Parallel()
	msg, poolKeyHash := validMessage(t, defaultParams(t, 0x05))
	msg.KESSignature[0] ^= 0xFF
	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	err := auth.Verify(&msg)
	require.ErrorIs(t, err, ErrKESSignatureInvalid)
}

func TestAuthenticator_Verify_RejectsPoolNotInStakeDistribution(t *testing.T) {
	t.Parallel()
	msg, _ := validMessage(t, defaultParams(t, 0x06))
	// No stake registered for any pool.
	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{},
	}
	auth := newTestAuthenticator(t, authority)

	err := auth.Verify(&msg)
	require.ErrorIs(t, err, ErrPoolNotInStakeDistribution)
}

func TestAuthenticator_Verify_PropagatesStakeLookupError(t *testing.T) {
	t.Parallel()
	msg, _ := validMessage(t, defaultParams(t, 0x07))
	wantErr := errors.New("boom: database unavailable")
	auth := newTestAuthenticator(t, &erroringStakeAuthority{err: wantErr})

	err := auth.Verify(&msg)
	require.ErrorIs(t, err, wantErr)
}

func TestAuthenticator_Verify_RejectsKESPeriodPrecedingCert(t *testing.T) {
	t.Parallel()
	params := defaultParams(t, 0x08)
	params.certKESPeriod = 5
	params.messageKESPeriod = 5 // sign correctly at t=0 first...
	msg, poolKeyHash := validMessage(t, params)
	// ...then claim (post-signing) that the message was signed at an
	// earlier period than the certificate's own issuance period, updating
	// the message ID to match so the ID check does not intercept this
	// first. This must be rejected before any KES cryptography runs, since
	// a message-level period preceding cert issuance can never be
	// legitimate regardless of what the signature checks out as.
	msg.Payload.KESPeriod = 2
	newID, err := ocommon.ComputeDmqMessageID(msg.Payload)
	require.NoError(t, err)
	msg.SetMessageID(newID)

	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	err = auth.Verify(&msg)
	require.ErrorIs(t, err, ErrKESPeriodPrecedesCert)
}

func TestAuthenticator_Verify_RejectsOpCertIssueNumberRegression(t *testing.T) {
	t.Parallel()
	seedByte := byte(0x09)

	firstParams := defaultParams(t, seedByte)
	firstParams.issueNumber = 5
	firstMsg, poolKeyHash := validMessage(t, firstParams)

	secondParams := defaultParams(t, seedByte)
	secondParams.issueNumber = 3
	secondParams.body = []byte("a second, independently valid message")
	secondMsg, secondPoolKeyHash := validMessage(t, secondParams)
	require.Equal(t, poolKeyHash, secondPoolKeyHash)

	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	require.NoError(t, auth.Verify(&firstMsg))
	err := auth.Verify(&secondMsg)
	require.ErrorIs(t, err, ErrOpCertIssueNumberRegressed)
}

func TestAuthenticator_Verify_AcceptsOpCertIssueNumberAdvancing(t *testing.T) {
	t.Parallel()
	seedByte := byte(0x0A)

	firstParams := defaultParams(t, seedByte)
	firstParams.issueNumber = 1
	firstMsg, poolKeyHash := validMessage(t, firstParams)

	secondParams := defaultParams(t, seedByte)
	secondParams.issueNumber = 2
	secondParams.body = []byte("a second, later-numbered message")
	secondMsg, _ := validMessage(t, secondParams)

	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	require.NoError(t, auth.Verify(&firstMsg))
	require.NoError(t, auth.Verify(&secondMsg))
}

func TestAuthenticator_Verify_RejectsShortColdVerificationKey(t *testing.T) {
	t.Parallel()
	msg, poolKeyHash := validMessage(t, defaultParams(t, 0x0B))
	msg.ColdVerificationKey = msg.ColdVerificationKey[:16]
	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	err := auth.Verify(&msg)
	require.Error(t, err)
}

func TestAuthenticator_ForgetPool_ClearsIssueNumberBaseline(t *testing.T) {
	t.Parallel()
	seedByte := byte(0x0C)

	firstParams := defaultParams(t, seedByte)
	firstParams.issueNumber = 5
	firstMsg, poolKeyHash := validMessage(t, firstParams)

	secondParams := defaultParams(t, seedByte)
	secondParams.issueNumber = 3
	secondParams.body = []byte("resubmitted after ForgetPool")
	secondMsg, _ := validMessage(t, secondParams)

	authority := &fakeStakeAuthority{
		stakes: map[lcommon.PoolKeyHash]uint64{poolKeyHash: 1_000_000},
	}
	auth := newTestAuthenticator(t, authority)

	require.NoError(t, auth.Verify(&firstMsg))
	auth.ForgetPool(poolKeyHash)
	// With no remembered baseline, a lower issue number is once again a
	// first sighting rather than a regression.
	require.NoError(t, auth.Verify(&secondMsg))
}
