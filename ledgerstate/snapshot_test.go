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

package ledgerstate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lbabbage "github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	lconway "github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func TestFindLedgerStateFileLegacy(t *testing.T) {
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	// Create a legacy .lstate file
	lstatePath := filepath.Join(ledgerDir, "12345.lstate")
	err = os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, lstatePath, found)
}

func TestFindLedgerStateFileUTxOHD(t *testing.T) {
	dir := t.TempDir()
	slotDir := filepath.Join(dir, "ledger", "67890")
	err := os.MkdirAll(slotDir, 0o750)
	require.NoError(t, err)

	statePath := filepath.Join(slotDir, "state")
	err = os.WriteFile(statePath, []byte("data"), 0o640)
	require.NoError(t, err)

	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, statePath, found)
}

func TestFindLedgerStateFilePreferUTxOHD(t *testing.T) {
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	// Create both legacy and UTxO-HD files
	lstatePath := filepath.Join(ledgerDir, "12345.lstate")
	err = os.WriteFile(lstatePath, []byte("legacy"), 0o640)
	require.NoError(t, err)

	slotDir := filepath.Join(ledgerDir, "67890")
	err = os.MkdirAll(slotDir, 0o750)
	require.NoError(t, err)

	statePath := filepath.Join(slotDir, "state")
	err = os.WriteFile(statePath, []byte("utxohd"), 0o640)
	require.NoError(t, err)

	// Should prefer UTxO-HD format
	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, statePath, found)
}

func TestFindLedgerStateFileDBSubdir(t *testing.T) {
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "db", "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	lstatePath := filepath.Join(ledgerDir, "55555.lstate")
	err = os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, lstatePath, found)
}

func TestFindLedgerStateFileUTxOHDHighestSlot(t *testing.T) {
	dir := t.TempDir()

	// Create two UTxO-HD slot subdirectories with state files
	for _, slot := range []string{"100", "200"} {
		slotDir := filepath.Join(dir, "ledger", slot)
		err := os.MkdirAll(slotDir, 0o750)
		require.NoError(t, err)

		statePath := filepath.Join(slotDir, "state")
		err = os.WriteFile(statePath, []byte("data"), 0o640)
		require.NoError(t, err)
	}

	// FindLedgerStateFile must return the higher-numbered slot
	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)

	expectedPath := filepath.Join(dir, "ledger", "200", "state")
	require.Equal(t, expectedPath, found)
}

func TestFindLedgerStateFileNotFound(t *testing.T) {
	dir := t.TempDir()
	_, err := FindLedgerStateFile(dir)
	require.ErrorIs(t, err, ErrLedgerDirNotFound)
}

func TestFindUTxOTableFile(t *testing.T) {
	dir := t.TempDir()
	tablesDir := filepath.Join(dir, "ledger", "99999", "tables")
	err := os.MkdirAll(tablesDir, 0o750)
	require.NoError(t, err)

	tvarPath := filepath.Join(tablesDir, "tvar")
	err = os.WriteFile(tvarPath, []byte("data"), 0o640)
	require.NoError(t, err)

	found := FindUTxOTableFile(dir)
	require.Equal(t, tvarPath, found)
}

func TestFindUTxOTableFileCurrentTablesFile(t *testing.T) {
	dir := t.TempDir()
	slotDir := filepath.Join(dir, "ledger", "99999")
	err := os.MkdirAll(slotDir, 0o750)
	require.NoError(t, err)

	tablesPath := filepath.Join(slotDir, "tables")
	err = os.WriteFile(tablesPath, []byte("data"), 0o640)
	require.NoError(t, err)

	found := FindUTxOTableFile(dir)
	require.Equal(t, tablesPath, found)
}

func TestFindUTxOTableFileNotFound(t *testing.T) {
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	found := FindUTxOTableFile(dir)
	require.Empty(t, found)
}

func TestIsLedgerStateFile(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected bool
	}{
		{"numeric slot", "12345", true},
		{"checksum file", "12345.checksum", false},
		{"lock file", "12345.lock", false},
		{"tmp file", "12345.tmp", false},
		{"non-numeric", "state", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(
				t,
				tt.expected,
				isLedgerStateFile(tt.filename),
			)
		})
	}
}

func TestEraName(t *testing.T) {
	require.Equal(t, "Byron", EraName(EraByron))
	require.Equal(t, "Shelley", EraName(EraShelley))
	require.Equal(t, "Allegra", EraName(EraAllegra))
	require.Equal(t, "Mary", EraName(EraMary))
	require.Equal(t, "Alonzo", EraName(EraAlonzo))
	require.Equal(t, "Babbage", EraName(EraBabbage))
	require.Equal(t, "Conway", EraName(EraConway))
	require.Contains(t, EraName(99), "Unknown")
}

func TestParseSnapShotsAcceptsTwoElementSnapshots(t *testing.T) {
	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)

	snapshot, err := cbor.Encode([]any{
		cbor.RawMessage(emptyMap),
		cbor.RawMessage(emptyMap),
	})
	require.NoError(t, err)

	data, err := cbor.Encode([]any{
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
	})
	require.NoError(t, err)

	snapshots, err := ParseSnapShots(data)
	require.NoError(t, err)
	require.NotNil(t, snapshots)
	require.Empty(t, snapshots.Mark.PoolParams)
	require.Empty(t, snapshots.Set.PoolParams)
	require.Empty(t, snapshots.Go.PoolParams)
}

func TestParseSnapShotsAcceptsUTxOHDStakeWithPool(t *testing.T) {
	credHash := toFixed28([]byte("credential hash for snapshot"))
	poolHash := toFixed28([]byte("pool hash for snapshot"))
	stakeMap := encodeCredentialMapEntry(
		t,
		[]any{uint64(0), credHash[:]},
		[]any{uint64(42), poolHash[:]},
	)

	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)

	snapshot, err := cbor.Encode([]any{
		cbor.RawMessage(stakeMap),
		cbor.RawMessage(emptyMap),
	})
	require.NoError(t, err)

	data, err := cbor.Encode([]any{
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
	})
	require.NoError(t, err)

	snapshots, err := ParseSnapShots(data)
	require.NoError(t, err)

	credKey := hex.EncodeToString(credHash[:])
	require.Equal(t, uint64(42), snapshots.Mark.Stake[credKey])
	require.Equal(t, poolHash[:], snapshots.Mark.Delegations[credKey])
	require.Equal(t, uint64(42), snapshots.Set.Stake[credKey])
	require.Equal(t, poolHash[:], snapshots.Set.Delegations[credKey])
	require.Equal(t, uint64(42), snapshots.Go.Stake[credKey])
	require.Equal(t, poolHash[:], snapshots.Go.Delegations[credKey])
}

func TestParsePoolParamsMapAcceptsPoolDistrEntry(t *testing.T) {
	poolHash := toFixed28([]byte("pool hash for distribution"))
	vrfHash := [32]byte{}
	copy(vrfHash[:], []byte("vrf hash for distribution entry"))

	poolMap := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{
			uint64(0),
			[]any{uint64(0), uint64(1)},
			[]any{},
			uint64(0),
			vrfHash[:],
			uint64(0),
			uint64(0),
			[]any{uint64(0), uint64(1)},
			uint64(0),
			[]any{},
		},
	)

	pools, err := parsePoolParamsMap(poolMap)
	require.NoError(t, err)

	pool := pools[hex.EncodeToString(poolHash[:])]
	require.NotNil(t, pool)
	require.Equal(t, poolHash[:], pool.PoolKeyHash)
	require.Equal(t, vrfHash[:], pool.VrfKeyHash)
}

func TestVerifySnapshotDigest(t *testing.T) {
	content := []byte("test snapshot content for hashing")
	h := sha256.Sum256(content)
	expectedDigest := hex.EncodeToString(h[:])

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, content, 0o640)
	require.NoError(t, err)

	err = VerifySnapshotDigest(archivePath, expectedDigest)
	require.NoError(t, err)
}

func TestVerifySnapshotDigestMismatch(t *testing.T) {
	content := []byte("test snapshot content")

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, content, 0o640)
	require.NoError(t, err)

	err = VerifySnapshotDigest(
		archivePath,
		"0000000000000000000000000000000"+
			"000000000000000000000000000000000",
	)
	require.ErrorContains(t, err, "mismatch")
}

func TestVerifyChecksumFileNoChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// No .checksum file - should succeed (not an error)
	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}

func TestVerifyChecksumFileEmptyChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(checksumPath, []byte("  \n"), 0o640)
	require.NoError(t, err)

	// Empty checksum - should succeed (skip verification)
	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}

func TestVerifyChecksumFileMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("00000000"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.ErrorContains(t, err, "mismatch")
}

func TestExtractPParamsDataBabbageGovState(t *testing.T) {
	pparams := testBabbagePParams()
	pparamsData, err := cbor.Encode(pparams)
	require.NoError(t, err)

	govStateData, err := cbor.Encode([]any{
		uint64(1),
		uint64(2),
		cbor.RawMessage(pparamsData),
		uint64(4),
	})
	require.NoError(t, err)

	got, err := extractPParamsData(EraBabbage, govStateData)
	require.NoError(t, err)
	require.Equal(t, pparamsData, []byte(got))
}

func TestExtractPParamsDataConwayGovStateMap(t *testing.T) {
	pparams := testConwayPParams()
	pparamsData, err := cbor.Encode(pparams)
	require.NoError(t, err)

	govStateData, err := cbor.Encode(map[uint64]any{
		0: uint64(1),
		1: uint64(2),
		2: uint64(3),
		3: cbor.RawMessage(pparamsData),
		4: uint64(5),
	})
	require.NoError(t, err)

	got, err := extractPParamsData(EraConway, govStateData)
	require.NoError(t, err)
	require.Equal(t, pparamsData, []byte(got))
}

func TestExtractPParamsDataDetectsEraSpecificType(t *testing.T) {
	alonzoData, err := cbor.Encode(testBabbagePParams())
	require.NoError(t, err)
	conwayData, err := cbor.Encode(testConwayPParams())
	require.NoError(t, err)

	govStateData, err := cbor.Encode([]any{
		uint64(1),
		uint64(2),
		cbor.RawMessage(alonzoData),
		cbor.RawMessage(conwayData),
	})
	require.NoError(t, err)

	got, err := extractPParamsData(EraConway, govStateData)
	require.NoError(t, err)
	require.Equal(t, conwayData, []byte(got))
}

func testBabbagePParams() *lbabbage.BabbageProtocolParameters {
	return &lbabbage.BabbageProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               500,
		A0:                 &cbor.Rat{Rat: big.NewRat(3, 10)},
		Rho:                &cbor.Rat{Rat: big.NewRat(3, 1000)},
		Tau:                &cbor.Rat{Rat: big.NewRat(1, 5)},
		ProtocolMajor:      8,
		ProtocolMinor:      0,
		MinPoolCost:        340000000,
		AdaPerUtxoByte:     4310,
		CostModels:         map[uint][]int64{1: {0}},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(577, 10000)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(721, 10000000)},
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10000000,
			Steps:  10000000000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 50000000,
			Steps:  40000000000,
		},
		MaxValueSize:         5000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

func testConwayPParams() *lconway.ConwayProtocolParameters {
	return &lconway.ConwayProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               500,
		A0:                 &cbor.Rat{Rat: big.NewRat(3, 10)},
		Rho:                &cbor.Rat{Rat: big.NewRat(3, 1000)},
		Tau:                &cbor.Rat{Rat: big.NewRat(1, 5)},
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
			Minor: 0,
		},
		MinPoolCost:    340000000,
		AdaPerUtxoByte: 4310,
		CostModels:     map[uint][]int64{1: {0}},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(577, 10000)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(721, 10000000)},
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10000000,
			Steps:  10000000000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 50000000,
			Steps:  40000000000,
		},
		MaxValueSize:         5000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
		PoolVotingThresholds: lconway.PoolVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
			HardForkInitiation:    cbor.Rat{Rat: big.NewRat(1, 2)},
			PpSecurityGroup:       cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		DRepVotingThresholds: lconway.DRepVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
			UpdateToConstitution:  cbor.Rat{Rat: big.NewRat(1, 2)},
			HardForkInitiation:    cbor.Rat{Rat: big.NewRat(1, 2)},
			PpNetworkGroup:        cbor.Rat{Rat: big.NewRat(1, 2)},
			PpEconomicGroup:       cbor.Rat{Rat: big.NewRat(1, 2)},
			PpTechnicalGroup:      cbor.Rat{Rat: big.NewRat(1, 2)},
			PpGovGroup:            cbor.Rat{Rat: big.NewRat(1, 2)},
			TreasuryWithdrawal:    cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		MinCommitteeSize:        5,
		CommitteeTermLimit:      146,
		GovActionValidityPeriod: 20,
		GovActionDeposit:        100000000000,
		DRepDeposit:             500000000,
		DRepInactivityPeriod:    20,
		MinFeeRefScriptCostPerByte: &cbor.Rat{
			Rat: big.NewRat(1, 1),
		},
	}
}

func TestVerifyChecksumFileWrongLength(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// Valid hex but wrong length (5 bytes instead of 4)
	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("aabbccdd00"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 8")
}

func TestVerifyChecksumFileInvalidHex(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// Valid length but invalid hex characters
	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("GGGGGGGG"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.ErrorContains(t, err, "invalid hex")
}

func TestVerifyChecksumFileValid(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	content := []byte("test data for crc32")
	err := os.WriteFile(lstatePath, content, 0o640)
	require.NoError(t, err)

	// Compute the actual CRC32 for the content
	h := crc32.NewIEEE()
	_, err = h.Write(content)
	require.NoError(t, err)
	checksum := fmt.Sprintf("%08x", h.Sum32())

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte(checksum),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}
