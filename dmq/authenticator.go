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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// StakeAuthority reports a pool's active stake for CIP-0137 message
// authorization (phase 2, issue #1949's steps 3-4: pool ID derivation and
// stake distribution lookup). A message's issuing pool must hold stake in
// the current distribution or Verify rejects it.
//
// Composition wires this to the real chain state -- e.g. an adapter over
// ledger.LedgerView.GetPoolStake against the Praos-active epoch, the same
// stake source leader election and Leios committee formation already use
// (see node_leios.go's stake adapters for the equivalent pattern). Dmq stays
// decoupled from ledger/database so it remains usable, and unit-testable,
// standalone -- matching phase 1's own scoping (see doc.go and
// ARCHITECTURE.md's "DMQ Message Pool" section).
type StakeAuthority interface {
	// PoolActiveStake returns poolKeyHash's stake in the current epoch's
	// stake distribution snapshot. A pool absent from the distribution
	// returns (0, nil), matching ledger.LedgerView.GetPoolStake's own
	// zero-for-absent convention.
	PoolActiveStake(poolKeyHash lcommon.PoolKeyHash) (uint64, error)
}

// AuthenticatorConfig configures an Authenticator.
type AuthenticatorConfig struct {
	// StakeAuthority backs the pool-authorization check. Required; a nil
	// StakeAuthority is a configuration error rather than a silent skip of
	// the check.
	StakeAuthority StakeAuthority
	// SlotsPerKESPeriod is the Shelley genesis slotsPerKESPeriod parameter,
	// needed to convert a message's claimed KES period into the slot the
	// underlying KES verifier expects. Required; zero is a configuration
	// error.
	SlotsPerKESPeriod uint64
	// Now, when non-nil, replaces time.Now for expiry checks. Tests use
	// this for deterministic behavior.
	Now func() time.Time
}

// Sentinel errors returned by Authenticator.Verify. Wrap with errors.Is
// rather than matching message text.
var (
	// ErrAuthenticatorMisconfigured is returned by NewAuthenticator when a
	// required Config field is missing.
	ErrAuthenticatorMisconfigured = errors.New(
		"dmq: authenticator missing required configuration",
	)
	// ErrMessageExpired is returned when a message's CIP-0137 expiresAt has
	// already passed.
	ErrMessageExpired = errors.New("dmq: message has expired")
	// ErrMessageIDMismatch is returned when a message's ID does not equal
	// the hash of its own payload.
	ErrMessageIDMismatch = errors.New(
		"dmq: message id does not match hash of its payload",
	)
	// ErrOpCertInvalid is returned when the operational certificate's
	// cold-key signature over the KES verification key does not verify.
	ErrOpCertInvalid = errors.New(
		"dmq: operational certificate cold-key signature invalid",
	)
	// ErrKESPeriodPrecedesCert is returned when a message claims a KES
	// period earlier than the period its own operational certificate was
	// issued at -- never legitimate, since the certificate cannot sign for
	// a period before it existed.
	ErrKESPeriodPrecedesCert = errors.New(
		"dmq: message KES period precedes operational certificate issuance",
	)
	// ErrKESSignatureInvalid is returned when the KES signature over the
	// message payload does not verify.
	ErrKESSignatureInvalid = errors.New("dmq: KES signature invalid")
	// ErrPoolNotInStakeDistribution is returned when the issuing pool holds
	// no stake in the current distribution.
	ErrPoolNotInStakeDistribution = errors.New(
		"dmq: issuing pool holds no stake in the current distribution",
	)
	// ErrOpCertIssueNumberRegressed is returned when a pool's operational
	// certificate issue number is lower than one already seen from that
	// pool -- a stale or replayed certificate.
	ErrOpCertIssueNumberRegressed = errors.New(
		"dmq: operational certificate issue number went backwards",
	)
)

// Authenticator verifies inbound DMQ messages against CIP-0137's
// authentication chain (phase 2, issue #1949): expiration, message-ID
// integrity, pool-ID derivation and stake-distribution authorization, the
// operational certificate's cold-key signature, the KES signature over the
// message payload, and operational-certificate issue-number monotonicity
// (replay protection). Verify returning nil is what makes a message eligible
// for MessageMempool.Add; a rejected message must not be relayed to peers or
// handed to application consumers.
//
// Authenticator deliberately does not reuse
// github.com/blinklabs-io/gouroboros/protocol/common's MessageAuthenticator.
// That type has two mismatches with real Cardano pool credentials: (1) it
// verifies the operational certificate's cold signature over a CBOR
// encoding of [KESVerificationKey, IssueNumber, KESPeriod], but a pool's
// real, already-issued operational certificate is signed over the raw
// OCertSignable byte concatenation cardano-node uses -- the same mismatch
// dingo's own verify_opcert.go documents and fixed for block headers (see
// its "verifyOpCertColdSignature" comment); and (2) its injected
// KES-verifier callback is invoked with the message's own claimed KES
// period standing in for both the certificate's issuance period and the
// slot used to derive it, which collapses the KES evolution offset to zero
// regardless of how many periods have actually elapsed since the
// certificate was issued. CIP-0137 messages carry a pool's real operational
// certificate, so they must verify against the real byte layout and the
// real per-pool evolution offset. Authenticator calls
// gouroboros/ledger's conformance-tested OpCert and KES primitives
// directly instead, the same ones dingo's own block-header verification
// uses.
type Authenticator struct {
	stakeAuthority    StakeAuthority
	slotsPerKesPeriod uint64
	now               func() time.Time

	mu           sync.Mutex
	issueNumbers map[lcommon.PoolKeyHash]uint64
}

// NewAuthenticator constructs an Authenticator. It returns
// ErrAuthenticatorMisconfigured if StakeAuthority is nil or
// SlotsPerKESPeriod is zero.
func NewAuthenticator(cfg AuthenticatorConfig) (*Authenticator, error) {
	if cfg.StakeAuthority == nil {
		return nil, fmt.Errorf(
			"%w: StakeAuthority is required",
			ErrAuthenticatorMisconfigured,
		)
	}
	if cfg.SlotsPerKESPeriod == 0 {
		return nil, fmt.Errorf(
			"%w: SlotsPerKESPeriod must be non-zero",
			ErrAuthenticatorMisconfigured,
		)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Authenticator{
		stakeAuthority:    cfg.StakeAuthority,
		slotsPerKesPeriod: cfg.SlotsPerKESPeriod,
		now:               now,
		issueNumbers:      make(map[lcommon.PoolKeyHash]uint64),
	}, nil
}

// Verify runs the full CIP-0137 authentication chain against msg: expiry,
// message-ID integrity, pool-ID derivation and stake-distribution
// authorization, the opcert cold-key signature, the KES signature over the
// payload, and finally opcert issue-number monotonicity. It returns nil only
// when every check passes.
//
// The stake-authorization check deliberately runs before either signature
// is verified. Deriving a pool ID from ColdVerificationKey needs no
// signature -- anyone can self-sign an internally consistent opcert/KES
// chain over freshly generated keys, so the signature checks only prove the
// sender holds the claimed private keys, not that those keys belong to a
// real, staked pool. Checking authorization first turns away a message from
// an unregistered identity before paying for an ed25519 verify and the
// ~2ms KES verify, rather than after.
//
// The issue-number baseline for msg's pool is not updated until every
// earlier check has passed, so a message that fails cold-signature or KES
// verification cannot poison replay protection for a later, legitimately
// higher-numbered certificate from the same pool.
func (a *Authenticator) Verify(msg *ocommon.DmqMessage) error {
	if msg == nil {
		return errors.New("dmq: message is nil")
	}

	if !msg.IsValidAt(a.now()) {
		return ErrMessageExpired
	}

	expectedID, err := ocommon.ComputeDmqMessageID(msg.Payload)
	if err != nil {
		return fmt.Errorf("dmq: compute message id: %w", err)
	}
	if !bytes.Equal(msg.ID(), expectedID) {
		return ErrMessageIDMismatch
	}

	// Stake authorization before either signature check -- see the doc
	// comment above for why.
	poolKeyHash := lcommon.Blake2b224Hash(msg.ColdVerificationKey)
	stake, err := a.stakeAuthority.PoolActiveStake(poolKeyHash)
	if err != nil {
		return fmt.Errorf("dmq: look up pool stake: %w", err)
	}
	if stake == 0 {
		return ErrPoolNotInStakeDistribution
	}

	opCert := &gledger.OpCert{
		KesVkey:       msg.OperationalCertificate.KESVerificationKey,
		IssueNumber:   msg.OperationalCertificate.IssueNumber,
		KesPeriod:     msg.OperationalCertificate.KESPeriod,
		ColdSignature: msg.OperationalCertificate.ColdSignature,
	}
	if err := gledger.VerifyOpCertSignature(opCert, msg.ColdVerificationKey); err != nil {
		return fmt.Errorf("%w: %w", ErrOpCertInvalid, err)
	}

	if msg.Payload.KESPeriod < msg.OperationalCertificate.KESPeriod {
		return ErrKESPeriodPrecedesCert
	}

	if err := a.verifyKESSignature(msg); err != nil {
		return err
	}

	return a.checkAndAdvanceIssueNumber(
		poolKeyHash,
		msg.OperationalCertificate.IssueNumber,
	)
}

// verifyKESSignature verifies the KES signature over msg's CBOR-wrapped
// payload (CIP-0137's "bstr .cbor messagePayload": a CBOR byte string
// wrapping the CBOR-encoded payload). The evolution offset is the
// message's claimed signing period minus the certificate's issuance
// period; converting that period back into an equivalent slot
// (period * slotsPerKesPeriod) lets it reuse
// gouroboros/ledger.VerifyKesComponents unchanged, the same function dingo
// uses to verify KES signatures on block headers.
func (a *Authenticator) verifyKESSignature(msg *ocommon.DmqMessage) error {
	payloadCbor, err := cbor.Encode(msg.Payload)
	if err != nil {
		return fmt.Errorf("dmq: encode message payload: %w", err)
	}
	wrappedCbor, err := cbor.Encode(payloadCbor)
	if err != nil {
		return fmt.Errorf("dmq: encode wrapped message payload: %w", err)
	}

	slot := msg.Payload.KESPeriod * a.slotsPerKesPeriod
	valid, err := gledger.VerifyKesComponents(
		wrappedCbor,
		msg.KESSignature,
		msg.OperationalCertificate.KESVerificationKey,
		msg.OperationalCertificate.KESPeriod,
		slot,
		a.slotsPerKesPeriod,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrKESSignatureInvalid, err)
	}
	if !valid {
		return ErrKESSignatureInvalid
	}
	return nil
}

// checkAndAdvanceIssueNumber enforces that poolKeyHash's opcert issue
// number never regresses, then records issueNumber as the new baseline. A
// pool seen for the first time has no baseline to compare against and is
// accepted unconditionally, becoming the baseline for the next message.
func (a *Authenticator) checkAndAdvanceIssueNumber(
	poolKeyHash lcommon.PoolKeyHash,
	issueNumber uint64,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	last, seen := a.issueNumbers[poolKeyHash]
	if seen && issueNumber < last {
		return ErrOpCertIssueNumberRegressed
	}
	a.issueNumbers[poolKeyHash] = issueNumber
	return nil
}

// ForgetPool drops the cached opcert issue-number baseline for poolKeyHash.
// Callers should invoke it when a pool is no longer registered/active, to
// bound the cache's memory to genuinely active pools (mirroring
// MessageMempool.RemovePeer and gouroboros' RemoveKESOpCertCacheEntry). The
// cache has no automatic eviction otherwise.
func (a *Authenticator) ForgetPool(poolKeyHash lcommon.PoolKeyHash) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.issueNumbers, poolKeyHash)
}
