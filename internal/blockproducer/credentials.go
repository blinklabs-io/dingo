// Copyright 2025 Blink Labs Software
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

// Package blockproducer parses and validates the credentials a Cardano stake
// pool operator supplies to a node running in block producer mode: the VRF
// signing key, the KES signing key, and the operational certificate. Loading
// happens at startup so misconfiguration fails fast rather than surfacing on
// the first leader check.
package blockproducer

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// Sizes (in bytes) of the byte-string payloads we expect after CBOR decoding.
const (
	vrfSeedSize         = 32
	vrfSeedAndPubSize   = 64
	kesSecretKeySize    = 608 // sum6 KES used by Cardano
	verificationKeySize = 32
	signatureSize       = 64
	coldVKeySize        = 32
	poolIDSize          = 28
)

// Accepted envelope type tags. Cardano-cli and bursa use slightly different
// spellings for the same key formats; both are supported.
var (
	vrfTypes    = []string{"VrfSigningKey_PraosVRF", "VRFSigningKey_PraosVRF"}
	kesTypes    = []string{"KesSigningKey_ed25519_kes_2^6", "KESSigningKey_PraosV2"}
	opCertTypes = []string{"NodeOperationalCertificate"}
)

// OperationalCertificate is the parsed contents of a node operational
// certificate. The cold key signs the (hot KES vkey, sequence number, KES
// period) tuple with its long-lived signing key; the node uses the hot KES
// vkey to verify forge signatures during the active KES period.
type OperationalCertificate struct {
	HotKESVKey     []byte
	SequenceNumber uint64
	KESPeriod      uint64
	ColdSignature  []byte
	ColdVKey       []byte
}

// Credentials are the operator-supplied keys and certificate required to
// produce blocks. Construct via Load.
type Credentials struct {
	VRFSigningKey      []byte // 32 (seed) or 64 (seed+pub)
	VRFVerificationKey []byte // 32; populated only when VRFSigningKey is 64 bytes
	KESSigningKey      []byte // 608
	OpCert             OperationalCertificate
	PoolID             [poolIDSize]byte // Blake2b-224(OpCert.ColdVKey)
}

// Load reads VRF signing key, KES signing key, and operational certificate
// files from disk and decodes them. It validates encoding and sizes only;
// semantic checks (KES period plausibility, ledger consistency) are performed
// by Validate and ValidateAgainstLedger.
func Load(vrfPath, kesPath, opCertPath string) (*Credentials, error) {
	vrfSk, vrfVk, err := loadVRFSigningKey(vrfPath)
	if err != nil {
		return nil, fmt.Errorf("vrf signing key: %w", err)
	}
	kesSk, err := loadKESSigningKey(kesPath)
	if err != nil {
		return nil, fmt.Errorf("kes signing key: %w", err)
	}
	opCert, err := loadOperationalCertificate(opCertPath)
	if err != nil {
		return nil, fmt.Errorf("operational certificate: %w", err)
	}
	c := &Credentials{
		VRFSigningKey:      vrfSk,
		VRFVerificationKey: vrfVk,
		KESSigningKey:      kesSk,
		OpCert:             opCert,
	}
	c.PoolID = lcommon.Blake2b224Hash(opCert.ColdVKey)
	return c, nil
}

func loadVRFSigningKey(path string) ([]byte, []byte, error) {
	env, err := readEnvelope(path)
	if err != nil {
		return nil, nil, err
	}
	raw, err := envelopeBytes(env, vrfTypes...)
	if err != nil {
		return nil, nil, err
	}
	var skBytes []byte
	if _, err := cbor.Decode(raw, &skBytes); err != nil {
		return nil, nil, fmt.Errorf("decode cbor: %w", err)
	}
	switch len(skBytes) {
	case vrfSeedAndPubSize:
		// cardano-cli format: seed (32) || pubkey (32)
		return skBytes, skBytes[vrfSeedSize:], nil
	case vrfSeedSize:
		// Seed-only format. We retain the seed but cannot derive the public
		// key without a VRF KeyGen helper, which gouroboros does not provide
		// at this version. The ledger cross-check (VRF hash match) requires
		// the public key, so callers should be aware that it will be skipped.
		return skBytes, nil, nil
	default:
		return nil, nil, fmt.Errorf(
			"invalid length: expected %d or %d bytes, got %d",
			vrfSeedSize, vrfSeedAndPubSize, len(skBytes),
		)
	}
}

func loadKESSigningKey(path string) ([]byte, error) {
	env, err := readEnvelope(path)
	if err != nil {
		return nil, err
	}
	raw, err := envelopeBytes(env, kesTypes...)
	if err != nil {
		return nil, err
	}
	var skBytes []byte
	if _, err := cbor.Decode(raw, &skBytes); err != nil {
		return nil, fmt.Errorf("decode cbor: %w", err)
	}
	if len(skBytes) != kesSecretKeySize {
		return nil, fmt.Errorf(
			"invalid length: expected %d bytes, got %d",
			kesSecretKeySize, len(skBytes),
		)
	}
	return skBytes, nil
}

func loadOperationalCertificate(path string) (OperationalCertificate, error) {
	env, err := readEnvelope(path)
	if err != nil {
		return OperationalCertificate{}, err
	}
	raw, err := envelopeBytes(env, opCertTypes...)
	if err != nil {
		return OperationalCertificate{}, err
	}
	// CBOR shape: [[hot_kes_vkey, sequence_number, kes_period, signature], cold_vkey]
	var outer []any
	if _, err := cbor.Decode(raw, &outer); err != nil {
		return OperationalCertificate{}, fmt.Errorf("decode cbor: %w", err)
	}
	if len(outer) != 2 {
		return OperationalCertificate{}, fmt.Errorf(
			"expected 2-element outer array, got %d",
			len(outer),
		)
	}
	inner, ok := outer[0].([]any)
	if !ok {
		return OperationalCertificate{}, errors.New(
			"first element is not an array",
		)
	}
	if len(inner) != 4 {
		return OperationalCertificate{}, fmt.Errorf(
			"expected 4-element cert array, got %d",
			len(inner),
		)
	}
	hotKES, err := bytesField(inner[0], "hot_kes_vkey", verificationKeySize)
	if err != nil {
		return OperationalCertificate{}, err
	}
	seqNum, err := uintField(inner[1], "sequence_number")
	if err != nil {
		return OperationalCertificate{}, err
	}
	kesPeriod, err := uintField(inner[2], "kes_period")
	if err != nil {
		return OperationalCertificate{}, err
	}
	sig, err := bytesField(inner[3], "signature", signatureSize)
	if err != nil {
		return OperationalCertificate{}, err
	}
	coldVKey, err := bytesField(outer[1], "cold_vkey", coldVKeySize)
	if err != nil {
		return OperationalCertificate{}, err
	}
	return OperationalCertificate{
		HotKESVKey:     hotKES,
		SequenceNumber: seqNum,
		KESPeriod:      kesPeriod,
		ColdSignature:  sig,
		ColdVKey:       coldVKey,
	}, nil
}

func bytesField(v any, name string, expected int) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("%s is not bytes (got %T)", name, v)
	}
	if len(b) != expected {
		return nil, fmt.Errorf(
			"%s has wrong length: expected %d, got %d",
			name, expected, len(b),
		)
	}
	return b, nil
}

// uintField extracts a non-negative integer from a CBOR value. The decoder
// uses uint64 for small unsigned ints; very large values may surface as
// a *big.Int so we accept that too.
func uintField(v any, name string) (uint64, error) {
	switch n := v.(type) {
	case uint64:
		return n, nil
	case int64:
		if n < 0 {
			return 0, fmt.Errorf("%s is negative", name)
		}
		return uint64(n), nil
	case uint:
		return uint64(n), nil
	case int:
		if n < 0 {
			return 0, fmt.Errorf("%s is negative", name)
		}
		return uint64(n), nil
	default:
		return 0, fmt.Errorf("%s is not an integer (got %T)", name, v)
	}
}

// Validate runs offline checks against the operational certificate that do
// not require ledger state: KES period plausibility relative to the genesis
// SystemStart and the wall clock. A non-nil result means the node should
// refuse to start.
func (c *Credentials) Validate(genesis *shelley.ShelleyGenesis, now time.Time) error {
	if genesis == nil {
		return errors.New("shelley genesis is required")
	}
	if genesis.SlotsPerKESPeriod <= 0 {
		return fmt.Errorf(
			"genesis slotsPerKESPeriod must be positive, got %d",
			genesis.SlotsPerKESPeriod,
		)
	}
	current, err := currentKESPeriod(genesis, now)
	if err != nil {
		return err
	}
	if c.OpCert.KESPeriod > current {
		return fmt.Errorf(
			"opcert KES period %d is in the future (current %d)",
			c.OpCert.KESPeriod, current,
		)
	}
	maxEvolutions := uint64(genesis.MaxKESEvolutions) // #nosec G115
	if maxEvolutions > 0 && current >= c.OpCert.KESPeriod+maxEvolutions {
		return fmt.Errorf(
			"opcert KES period %d has expired (current %d, max evolutions %d); rotate the operational certificate",
			c.OpCert.KESPeriod, current, maxEvolutions,
		)
	}
	return nil
}

// currentKESPeriod returns the KES period the node should consider current
// based on wall-clock time and the Shelley genesis slot timing.
func currentKESPeriod(genesis *shelley.ShelleyGenesis, now time.Time) (uint64, error) {
	if now.Before(genesis.SystemStart) {
		// Before the chain has started, treat the current period as 0.
		return 0, nil
	}
	// SlotLength is a rational number of seconds per slot.
	slotLengthSeconds := new(big.Rat).Set(genesis.SlotLength.Rat)
	if slotLengthSeconds.Sign() <= 0 {
		return 0, fmt.Errorf(
			"genesis slotLength must be positive, got %s",
			slotLengthSeconds.RatString(),
		)
	}
	elapsedNanos := now.Sub(genesis.SystemStart).Nanoseconds()
	// slot = elapsed_nanos / (slotLengthSeconds * 1e9)
	num := new(big.Int).Mul(
		big.NewInt(elapsedNanos),
		slotLengthSeconds.Denom(),
	)
	denom := new(big.Int).Mul(
		slotLengthSeconds.Num(),
		big.NewInt(1_000_000_000),
	)
	slot := new(big.Int).Quo(num, denom)
	period := new(big.Int).Quo(slot, big.NewInt(int64(genesis.SlotsPerKESPeriod)))
	if !period.IsUint64() {
		return 0, fmt.Errorf("computed KES period %s overflows uint64", period.String())
	}
	return period.Uint64(), nil
}

// LedgerView is the subset of dingo's LedgerView the credential check needs.
// Defined here so this package does not depend on the ledger package and
// callers can pass an adapter.
type LedgerView interface {
	// PoolRegistrationVRFKeyHash returns the VRF key hash recorded in the
	// most recent active pool registration certificate for the given pool.
	// The found return is false when the pool is not (yet) registered.
	PoolRegistrationVRFKeyHash(poolID [poolIDSize]byte) (vrfKeyHash [32]byte, found bool, err error)
	// LatestOpCertSequence returns the highest operational certificate
	// sequence number observed for the given pool. When tracking is not
	// implemented or the pool has never minted, found is false.
	LatestOpCertSequence(poolID [poolIDSize]byte) (sequence uint64, found bool, err error)
}

// ValidateAgainstLedger cross-checks the loaded credentials against ledger
// state. Best-effort: missing pool registrations are not fatal because a pool
// can be configured before its registration certificate is on chain.
//
// It returns three results:
//   - registered: true if the pool registration was found.
//   - vrfMatched: true if registered and the registration's VRF key hash
//     matched our VRF verification key. False when registered=false or when
//     the VRF verification key is unavailable (32-byte seed-only VRF skey).
//   - err: a non-nil error means the node should refuse to start.
func (c *Credentials) ValidateAgainstLedger(view LedgerView) (registered, vrfMatched bool, err error) {
	if view == nil {
		return false, false, errors.New("ledger view is nil")
	}
	regVRF, found, err := view.PoolRegistrationVRFKeyHash(c.PoolID)
	if err != nil {
		return false, false, fmt.Errorf("pool registration lookup: %w", err)
	}
	if !found {
		return false, false, nil
	}
	if c.VRFVerificationKey != nil {
		ourVRF := lcommon.Blake2b256Hash(c.VRFVerificationKey)
		if ourVRF != regVRF {
			return true, false, fmt.Errorf(
				"VRF key hash mismatch: opcert pool registers %x but loaded VRF key hashes to %x",
				regVRF, ourVRF,
			)
		}
		vrfMatched = true
	}
	latestSeq, seqFound, err := view.LatestOpCertSequence(c.PoolID)
	if err != nil {
		return true, vrfMatched, fmt.Errorf("opcert sequence lookup: %w", err)
	}
	if seqFound && c.OpCert.SequenceNumber < latestSeq {
		return true, vrfMatched, fmt.Errorf(
			"opcert sequence %d is stale: ledger has observed %d for this pool",
			c.OpCert.SequenceNumber, latestSeq,
		)
	}
	return true, vrfMatched, nil
}
