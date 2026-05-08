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

package blockproducer

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// devnetKeysDir locates the devnet credential fixtures shipped with the
// repository. Tests rely on these being well-formed real keys.
const devnetKeysDir = "../../config/cardano/devnet/keys"

func devnetPaths() (vrf, kes, opcert string) {
	return filepath.Join(devnetKeysDir, "vrf.skey"),
		filepath.Join(devnetKeysDir, "kes.skey"),
		filepath.Join(devnetKeysDir, "opcert.cert")
}

// writeEnvelope writes a TextEnvelope JSON file to a temp path and returns
// the path. Callers can pass arbitrary type/cborHex to construct malformed
// inputs.
func writeEnvelope(t *testing.T, name, envType, cborHex string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	body := fmt.Sprintf(
		`{"type":%q,"description":"test","cborHex":%q}`,
		envType, cborHex,
	)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write envelope: %v", err)
	}
	return path
}

// cborByteString returns the CBOR encoding of a byte string of the given
// length, with bytes filled by repeating the supplied seed.
func cborByteString(t *testing.T, length int, seed byte) string {
	t.Helper()
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = seed + byte(i)
	}
	enc, err := cbor.Encode(buf)
	if err != nil {
		t.Fatalf("cbor encode: %v", err)
	}
	return hex.EncodeToString(enc)
}

func TestLoad_DevnetFixtures(t *testing.T) {
	vrf, kes, opcert := devnetPaths()
	creds, err := Load(vrf, kes, opcert)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := len(creds.VRFSigningKey); got != vrfSeedAndPubSize {
		t.Errorf("VRFSigningKey length = %d, want %d", got, vrfSeedAndPubSize)
	}
	if got := len(creds.VRFVerificationKey); got != verificationKeySize {
		t.Errorf(
			"VRFVerificationKey length = %d, want %d",
			got, verificationKeySize,
		)
	}
	if got := len(creds.KESSigningKey); got != kesSecretKeySize {
		t.Errorf("KESSigningKey length = %d, want %d", got, kesSecretKeySize)
	}
	if got := len(creds.OpCert.HotKESVKey); got != verificationKeySize {
		t.Errorf("OpCert.HotKESVKey length = %d, want %d", got, verificationKeySize)
	}
	if got := len(creds.OpCert.ColdSignature); got != signatureSize {
		t.Errorf("OpCert.ColdSignature length = %d, want %d", got, signatureSize)
	}
	if got := len(creds.OpCert.ColdVKey); got != coldVKeySize {
		t.Errorf("OpCert.ColdVKey length = %d, want %d", got, coldVKeySize)
	}
	zero := [poolIDSize]byte{}
	if creds.PoolID == zero {
		t.Error("PoolID is all zeros, expected derived hash")
	}
}

func TestLoad_PoolIDDerivation(t *testing.T) {
	vrf, kes, opcert := devnetPaths()
	creds, err := Load(vrf, kes, opcert)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	expected := lcommon.Blake2b224Hash(creds.OpCert.ColdVKey)
	if creds.PoolID != expected {
		t.Errorf(
			"PoolID = %x, want %x",
			creds.PoolID, expected,
		)
	}
}

func TestLoad_MissingFiles(t *testing.T) {
	_, kes, opcert := devnetPaths()
	missing := filepath.Join(t.TempDir(), "nope.skey")

	cases := []struct {
		name  string
		paths [3]string // vrf, kes, opcert
	}{
		{"vrf", [3]string{missing, kes, opcert}},
		{"kes", [3]string{filepath.Join(devnetKeysDir, "vrf.skey"), missing, opcert}},
		{"opcert", [3]string{filepath.Join(devnetKeysDir, "vrf.skey"), kes, missing}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(tc.paths[0], tc.paths[1], tc.paths[2])
			if err == nil {
				t.Fatal("expected error for missing file")
			}
			if !errors.Is(err, os.ErrNotExist) {
				t.Errorf("expected os.ErrNotExist, got %v", err)
			}
		})
	}
}

func TestLoad_VRFWrongType(t *testing.T) {
	// Pass a KES envelope at the VRF path.
	wrongVRF := writeEnvelope(t, "vrf.skey",
		"KesSigningKey_ed25519_kes_2^6",
		cborByteString(t, kesSecretKeySize, 0x01),
	)
	_, kes, opcert := devnetPaths()
	_, err := Load(wrongVRF, kes, opcert)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "unexpected envelope type") {
		t.Errorf("error should mention envelope type, got: %v", err)
	}
}

func TestLoad_VRFInvalidHex(t *testing.T) {
	bad := writeEnvelope(t, "vrf.skey", vrfTypes[0], "not-hex-data")
	_, kes, opcert := devnetPaths()
	_, err := Load(bad, kes, opcert)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "decode cborHex") {
		t.Errorf("error should mention hex decode, got: %v", err)
	}
}

func TestLoad_VRFInvalidLength(t *testing.T) {
	bad := writeEnvelope(t, "vrf.skey", vrfTypes[0], cborByteString(t, 16, 0x02))
	_, kes, opcert := devnetPaths()
	_, err := Load(bad, kes, opcert)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "invalid length") {
		t.Errorf("error should mention invalid length, got: %v", err)
	}
}

func TestLoad_VRFSeedOnly(t *testing.T) {
	// 32-byte seed-only form is accepted but VRFVerificationKey is unset.
	good := writeEnvelope(t, "vrf.skey", vrfTypes[0], cborByteString(t, vrfSeedSize, 0x03))
	_, kes, opcert := devnetPaths()
	creds, err := Load(good, kes, opcert)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(creds.VRFSigningKey) != vrfSeedSize {
		t.Errorf(
			"VRFSigningKey length = %d, want %d",
			len(creds.VRFSigningKey), vrfSeedSize,
		)
	}
	if creds.VRFVerificationKey != nil {
		t.Errorf("VRFVerificationKey should be nil for seed-only key, got %d bytes",
			len(creds.VRFVerificationKey))
	}
}

func TestLoad_KESWrongLength(t *testing.T) {
	bad := writeEnvelope(t, "kes.skey", kesTypes[0],
		cborByteString(t, kesSecretKeySize-1, 0x04),
	)
	vrf, _, opcert := devnetPaths()
	_, err := Load(vrf, bad, opcert)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "invalid length") {
		t.Errorf("error should mention invalid length, got: %v", err)
	}
}

func TestLoad_OpCertOuterArrayWrongSize(t *testing.T) {
	// CBOR for a 3-element array of empty byte strings.
	enc, err := cbor.Encode([]any{[]byte{}, []byte{}, []byte{}})
	if err != nil {
		t.Fatalf("cbor encode: %v", err)
	}
	bad := writeEnvelope(t, "opcert.cert", opCertTypes[0], hex.EncodeToString(enc))
	vrf, kes, _ := devnetPaths()
	_, err = Load(vrf, kes, bad)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "outer array") {
		t.Errorf("error should mention outer array, got: %v", err)
	}
}

func TestLoad_OpCertColdVKeyWrongLength(t *testing.T) {
	hotKES := bytes32(0x10)
	sig := bytes64(0x20)
	wrongCold := make([]byte, coldVKeySize-1)
	enc, err := cbor.Encode([]any{
		[]any{hotKES, uint64(0), uint64(0), sig},
		wrongCold,
	})
	if err != nil {
		t.Fatalf("cbor encode: %v", err)
	}
	bad := writeEnvelope(t, "opcert.cert", opCertTypes[0], hex.EncodeToString(enc))
	vrf, kes, _ := devnetPaths()
	_, err = Load(vrf, kes, bad)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "cold_vkey") {
		t.Errorf("error should mention cold_vkey, got: %v", err)
	}
}

func TestLoad_OpCertHotKESWrongLength(t *testing.T) {
	wrongHot := make([]byte, verificationKeySize-1)
	sig := bytes64(0x20)
	cold := bytes32(0x30)
	enc, err := cbor.Encode([]any{
		[]any{wrongHot, uint64(0), uint64(0), sig},
		cold,
	})
	if err != nil {
		t.Fatalf("cbor encode: %v", err)
	}
	bad := writeEnvelope(t, "opcert.cert", opCertTypes[0], hex.EncodeToString(enc))
	vrf, kes, _ := devnetPaths()
	_, err = Load(vrf, kes, bad)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "hot_kes_vkey") {
		t.Errorf("error should mention hot_kes_vkey, got: %v", err)
	}
}

func TestValidate_HappyPath(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := synthGenesis(t, 100, 5, time.Second, systemStart)
	creds := &Credentials{
		OpCert: OperationalCertificate{KESPeriod: 1, SequenceNumber: 0},
	}
	// 250 slots elapsed → KES period 2.
	now := systemStart.Add(250 * time.Second)
	if err := creds.Validate(g, now); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestValidate_KESPeriodInFuture(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := synthGenesis(t, 100, 5, time.Second, systemStart)
	creds := &Credentials{
		OpCert: OperationalCertificate{KESPeriod: 10},
	}
	// 50 slots elapsed → current period 0; opcert period 10 is in the future.
	now := systemStart.Add(50 * time.Second)
	err := creds.Validate(g, now)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "future") {
		t.Errorf("expected 'future' in error, got: %v", err)
	}
}

func TestValidate_KESPeriodExpired(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := synthGenesis(t, 100, 3, time.Second, systemStart)
	creds := &Credentials{
		OpCert: OperationalCertificate{KESPeriod: 1},
	}
	// Period 1 + max evolutions 3 = expires at period 4. At slot 500 →
	// current period 5, which is past expiry.
	now := systemStart.Add(500 * time.Second)
	err := creds.Validate(g, now)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("expected 'expired' in error, got: %v", err)
	}
}

func TestValidate_BeforeSystemStart(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := synthGenesis(t, 100, 5, time.Second, systemStart)
	creds := &Credentials{
		OpCert: OperationalCertificate{KESPeriod: 0},
	}
	// One hour before system start; current period is 0; opcert period 0 is fine.
	now := systemStart.Add(-time.Hour)
	if err := creds.Validate(g, now); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestValidate_ZeroSlotsPerKESPeriod(t *testing.T) {
	systemStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	g := synthGenesis(t, 0, 5, time.Second, systemStart)
	creds := &Credentials{}
	err := creds.Validate(g, systemStart)
	if err == nil {
		t.Fatal("expected error for zero SlotsPerKESPeriod")
	}
}

type fakeView struct {
	registered  bool
	regVRFHash  [32]byte
	regErr      error
	seqFound    bool
	latestSeq   uint64
	seqErr      error
	gotPoolID   [poolIDSize]byte
	seqPoolID   [poolIDSize]byte
	calledPool  bool
	calledSeq   bool
}

func (f *fakeView) PoolRegistrationVRFKeyHash(p [poolIDSize]byte) ([32]byte, bool, error) {
	f.calledPool = true
	f.gotPoolID = p
	return f.regVRFHash, f.registered, f.regErr
}

func (f *fakeView) LatestOpCertSequence(p [poolIDSize]byte) (uint64, bool, error) {
	f.calledSeq = true
	f.seqPoolID = p
	return f.latestSeq, f.seqFound, f.seqErr
}

func TestValidateAgainstLedger_PoolNotRegistered(t *testing.T) {
	creds := &Credentials{
		VRFVerificationKey: bytes32(0xAA),
	}
	creds.PoolID = lcommon.Blake2b224Hash([]byte("cold-vkey"))
	view := &fakeView{registered: false}
	registered, matched, err := creds.ValidateAgainstLedger(view)
	if err != nil {
		t.Errorf("Validate: %v", err)
	}
	if registered || matched {
		t.Errorf("expected registered=false, matched=false; got %v %v", registered, matched)
	}
	if !view.calledPool {
		t.Error("expected PoolRegistrationVRFKeyHash to be called")
	}
}

func TestValidateAgainstLedger_VRFMatch(t *testing.T) {
	vrfPub := bytes32(0xBB)
	creds := &Credentials{VRFVerificationKey: vrfPub}
	creds.PoolID = lcommon.Blake2b224Hash([]byte("cold-vkey"))
	view := &fakeView{
		registered: true,
		regVRFHash: lcommon.Blake2b256Hash(vrfPub),
	}
	registered, matched, err := creds.ValidateAgainstLedger(view)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !registered || !matched {
		t.Errorf("expected registered=true, matched=true; got %v %v", registered, matched)
	}
}

func TestValidateAgainstLedger_VRFMismatch(t *testing.T) {
	creds := &Credentials{VRFVerificationKey: bytes32(0xCC)}
	creds.PoolID = lcommon.Blake2b224Hash([]byte("cold-vkey"))
	view := &fakeView{
		registered: true,
		regVRFHash: lcommon.Blake2b256Hash(bytes32(0xDD)),
	}
	_, _, err := creds.ValidateAgainstLedger(view)
	if err == nil {
		t.Fatal("expected mismatch error")
	}
	if !strings.Contains(err.Error(), "VRF key hash mismatch") {
		t.Errorf("expected mismatch in error, got: %v", err)
	}
}

func TestValidateAgainstLedger_SeedOnlySkipsCheck(t *testing.T) {
	creds := &Credentials{VRFVerificationKey: nil}
	creds.PoolID = lcommon.Blake2b224Hash([]byte("cold-vkey"))
	view := &fakeView{
		registered: true,
		regVRFHash: lcommon.Blake2b256Hash([]byte("anything")),
	}
	registered, matched, err := creds.ValidateAgainstLedger(view)
	if err != nil {
		t.Errorf("Validate: %v", err)
	}
	if !registered {
		t.Error("expected registered=true")
	}
	if matched {
		t.Error("expected matched=false (no VRF pubkey)")
	}
}

func TestValidateAgainstLedger_StaleOpCert(t *testing.T) {
	vrfPub := bytes32(0xEE)
	creds := &Credentials{
		VRFVerificationKey: vrfPub,
		OpCert:             OperationalCertificate{SequenceNumber: 3},
	}
	creds.PoolID = lcommon.Blake2b224Hash([]byte("cold-vkey"))
	view := &fakeView{
		registered: true,
		regVRFHash: lcommon.Blake2b256Hash(vrfPub),
		seqFound:   true,
		latestSeq:  5,
	}
	_, _, err := creds.ValidateAgainstLedger(view)
	if err == nil {
		t.Fatal("expected stale-counter error")
	}
	if !strings.Contains(err.Error(), "stale") {
		t.Errorf("expected stale in error, got: %v", err)
	}
}

func TestValidateAgainstLedger_CounterTrackingNotImplemented(t *testing.T) {
	// When the ledger does not track opcert counters, the check must not
	// produce false positives or block startup.
	vrfPub := bytes32(0xFE)
	creds := &Credentials{
		VRFVerificationKey: vrfPub,
		OpCert:             OperationalCertificate{SequenceNumber: 0},
	}
	creds.PoolID = lcommon.Blake2b224Hash([]byte("cold-vkey"))
	view := &fakeView{
		registered: true,
		regVRFHash: lcommon.Blake2b256Hash(vrfPub),
		seqFound:   false, // tracking returns "not found"
	}
	registered, matched, err := creds.ValidateAgainstLedger(view)
	if err != nil {
		t.Errorf("Validate: %v", err)
	}
	if !registered || !matched {
		t.Errorf("expected registered=true, matched=true; got %v %v", registered, matched)
	}
}

func TestValidateAgainstLedger_NilView(t *testing.T) {
	creds := &Credentials{}
	_, _, err := creds.ValidateAgainstLedger(nil)
	if err == nil {
		t.Fatal("expected nil-view error")
	}
}

// synthGenesis builds a minimal Shelley genesis for KES timing math.
func synthGenesis(
	t *testing.T,
	slotsPerKES, maxKES int,
	slotLen time.Duration,
	systemStart time.Time,
) *shelley.ShelleyGenesis {
	t.Helper()
	// SlotLength is in seconds as a rational.
	rat := big.NewRat(int64(slotLen), int64(time.Second))
	return &shelley.ShelleyGenesis{
		SystemStart:       systemStart,
		SlotsPerKESPeriod: slotsPerKES,
		MaxKESEvolutions:  maxKES,
		SlotLength:        cbor.Rat{Rat: rat},
	}
}

func bytes32(seed byte) []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return b
}

func bytes64(seed byte) []byte {
	b := make([]byte, 64)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return b
}
