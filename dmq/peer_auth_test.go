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
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// dmqSigner holds a pool's cold key, KES key and operational certificate, and
// signs DMQ messages that pass CIP-0137 authentication at KES period 0.
type dmqSigner struct {
	coldVKey []byte
	kesSKey  *kes.SecretKey
	opCert   ocommon.OperationalCertificate
}

func newDMQSigner(t *testing.T, seed byte) *dmqSigner {
	t.Helper()
	cold := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	kesSKey, kesVKey, err := kes.KeyGen(
		kes.CardanoKesDepth,
		bytes.Repeat([]byte{seed + 1}, 32),
	)
	require.NoError(t, err)
	const issueNumber, certPeriod = 1, 0
	// The cold key signs cardano-node's raw OCertSignable bytes.
	signable := binary.BigEndian.AppendUint64(
		append([]byte{}, kesVKey...), issueNumber,
	)
	signable = binary.BigEndian.AppendUint64(signable, certPeriod)
	return &dmqSigner{
		coldVKey: cold.Public().(ed25519.PublicKey),
		kesSKey:  kesSKey,
		opCert: ocommon.OperationalCertificate{
			KESVerificationKey: kesVKey,
			IssueNumber:        issueNumber,
			KESPeriod:          certPeriod,
			ColdSignature:      ed25519.Sign(cold, signable),
		},
	}
}

func (s *dmqSigner) pool() ocommon.PoolKeyHash {
	return ocommon.PoolKeyHash(lcommon.Blake2b224Hash(s.coldVKey))
}

func (s *dmqSigner) message(t *testing.T, body string) ocommon.DmqMessage {
	t.Helper()
	payload := ocommon.DmqMessagePayload{
		MessageBody: []byte(body),
		ExpiresAt:   soonExpiry(),
	}
	payloadCbor, err := cbor.Encode(payload)
	require.NoError(t, err)
	sig, err := kes.Sign(s.kesSKey, 0, payloadCbor)
	require.NoError(t, err)
	msg := ocommon.DmqMessage{
		Payload:                payload,
		KESSignature:           sig,
		OperationalCertificate: s.opCert,
		ColdVerificationKey:    s.coldVKey,
	}
	require.NoError(t, msg.SetComputedMessageID())
	return msg
}

func (s *dmqSigner) messages(
	t *testing.T,
	prefix string,
	n int,
) []ocommon.DmqMessage {
	t.Helper()
	msgs := make([]ocommon.DmqMessage, 0, n)
	for i := range n {
		msgs = append(msgs, s.message(t, fmt.Sprintf("%s-%d", prefix, i)))
	}
	return msgs
}

// chainLookups is a stake authority and current-slot source that counts how
// often authentication consults chain state.
type chainLookups struct {
	stake       map[ocommon.PoolKeyHash]uint64
	err         error
	stakeCalls  atomic.Int64
	slotLookups atomic.Int64
}

func (c *chainLookups) PoolActiveStake(
	pool ocommon.PoolKeyHash,
) (uint64, error) {
	c.stakeCalls.Add(1)
	if c.err != nil {
		return 0, c.err
	}
	return c.stake[pool], nil
}

func (c *chainLookups) authenticator(
	t *testing.T,
) *ocommon.MessageAuthenticator {
	t.Helper()
	auth, err := ocommon.NewMessageAuthenticator(
		ocommon.MessageAuthenticatorConfig{
			StakeAuthority: c,
			CurrentSlot: func() (uint64, error) {
				c.slotLookups.Add(1)
				return 0, nil
			},
			PoolOpCertIssueNumber: func(ocommon.PoolKeyHash) (uint64, bool, error) {
				return 0, false, nil
			},
		},
	)
	require.NoError(t, err)
	return auth
}

func stakedLookups(signer *dmqSigner) *chainLookups {
	return &chainLookups{
		stake: map[ocommon.PoolKeyHash]uint64{signer.pool(): 1_000},
	}
}

func TestPeerAuthenticatesEachReplyAtOneSlot(t *testing.T) {
	t.Parallel()
	signer := newDMQSigner(t, 1)
	lookups := stakedLookups(signer)
	a, _ := newTestStack(t, StackConfig{
		Authenticator: lookups.authenticator(t),
	})
	b, _ := newTestStack(t, StackConfig{})
	for _, msg := range signer.messages(t, "signed", 3) {
		added, err := b.pool.Add(msg)
		require.NoError(t, err)
		require.True(t, added)
	}

	pa, _ := attachPair(t, a, b)
	waitFor(t, "a admits every signed message", func() bool {
		return a.pool.Len() == 3
	})
	require.NoError(t, pa.Err())
	require.Equal(t, int64(1), lookups.slotLookups.Load())
}

func TestStackAcceptFromPeerAdmitsNoneOfAReplyWithAForgery(t *testing.T) {
	t.Parallel()
	signer := newDMQSigner(t, 2)
	s, reg := newTestStack(t, StackConfig{
		Authenticator: stakedLookups(signer).authenticator(t),
	})
	reply := signer.messages(t, "reply", 3)
	reply[2].KESSignature = bytes.Clone(reply[2].KESSignature)
	reply[2].KESSignature[0] ^= 0xff

	err := s.acceptFromPeer(reply)
	require.ErrorContains(t, err, "invalid message")
	require.Zero(t, s.pool.Len())
	require.Equal(
		t,
		1.0,
		metricValue(t, reg, "dingo_dmq_validation_failures_total", reasonInvalid),
	)
}

func TestStackAcceptFromPeerSkipsHeldMessagesBeforeAuthenticating(
	t *testing.T,
) {
	t.Parallel()
	signer := newDMQSigner(t, 3)
	lookups := stakedLookups(signer)
	s, reg := newTestStack(t, StackConfig{
		Authenticator: lookups.authenticator(t),
	})
	held := signer.message(t, "held")
	_, err := s.pool.Add(held)
	require.NoError(t, err)

	require.NoError(t, s.acceptFromPeer([]ocommon.DmqMessage{held}))
	require.Zero(t, lookups.stakeCalls.Load())
	require.Equal(
		t,
		1.0,
		metricValue(
			t, reg, "dingo_dmq_validation_failures_total", reasonAlreadyReceived,
		),
	)
}

func TestStackAcceptFromPeerStopsAtUnavailableChainState(t *testing.T) {
	t.Parallel()
	signer := newDMQSigner(t, 4)
	lookups := &chainLookups{
		err: fmt.Errorf("%w: snapshot not captured", ErrChainStateUnavailable),
	}
	s, reg := newTestStack(t, StackConfig{
		Authenticator: lookups.authenticator(t),
	})

	require.NoError(t, s.acceptFromPeer(signer.messages(t, "unavailable", 3)))
	require.Zero(t, s.pool.Len())
	require.Equal(t, int64(1), lookups.stakeCalls.Load())
	require.Equal(
		t,
		3.0,
		metricValue(t, reg, "dingo_dmq_validation_failures_total", reasonOther),
	)
}
