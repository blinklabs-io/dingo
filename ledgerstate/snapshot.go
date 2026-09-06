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
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ErrLedgerDirNotFound is returned when the ledger directory cannot
// be located within an extracted snapshot.
var ErrLedgerDirNotFound = errors.New("ledger directory not found")

// FindLedgerStateFile searches the extracted snapshot directory for
// the ledger state file. It supports two formats:
//   - Legacy: ledger/<slot>.lstate or ledger/<slot>
//   - UTxO-HD: ledger/<slot>/state
//
// Returns the path to the state file.
//
// For trees the caller controls. It resolves pathnames and follows symlinks, so
// what it returns describes the tree only for as long as nobody else can write
// to it — and the name is resolved again by whoever opens it. Use
// OpenSnapshotAtOrBefore for a tree that was vetted, or that lives anywhere a
// concurrent writer might reach; it discovers through a directory handle and
// hands back the files already open. Mithril bootstrap uses that one.
func FindLedgerStateFile(extractedDir string) (string, error) {
	return FindLedgerStateFileAtOrBefore(extractedDir, ^uint64(0))
}

// FindLedgerStateFileAtOrBefore searches the extracted snapshot directory for
// the newest ledger state whose filename slot is at or before maxSlot. Mithril
// ancillary archives can contain a newer ledger state from the node's volatile
// database in addition to states anchored by certified ImmutableDB content.
// Callers that use a ledger state as a trust anchor must cap selection at the
// certified immutable tip.
func FindLedgerStateFileAtOrBefore(
	extractedDir string,
	maxSlot uint64,
) (string, error) {
	ledgerDir, err := findLedgerDir(extractedDir)
	if err != nil {
		return "", err
	}

	entries, err := os.ReadDir(ledgerDir)
	if err != nil {
		return "", fmt.Errorf(
			"reading ledger directory: %w",
			err,
		)
	}

	// Check for UTxO-HD directory format: ledger/<slot>/state
	var utxoHDDirs []string
	var legacyFiles []string

	for _, e := range entries {
		name := e.Name()
		slot, parseErr := strconv.ParseUint(
			stripLedgerSuffix(name),
			10,
			64,
		)
		if parseErr != nil || slot > maxSlot {
			continue
		}
		if e.IsDir() {
			// UTxO-HD format: directory named by slot number
			statePath := filepath.Join(
				ledgerDir, name, "state",
			)
			if _, err := os.Stat(statePath); err == nil {
				utxoHDDirs = append(utxoHDDirs, name)
			}
			continue
		}
		// Legacy format: .lstate files or numeric slot filenames
		if strings.HasSuffix(name, ".lstate") ||
			strings.HasSuffix(name, "_snapshot") ||
			isLedgerStateFile(name) {
			legacyFiles = append(legacyFiles, name)
		}
	}

	// Prefer UTxO-HD format (newer)
	utxoHDDirs = sortNumericDesc(utxoHDDirs)
	if len(utxoHDDirs) > 0 {
		return filepath.Join(
			ledgerDir, utxoHDDirs[0], "state",
		), nil
	}

	legacyFiles = sortNumericSuffixDesc(legacyFiles)
	if len(legacyFiles) > 0 {
		return filepath.Join(ledgerDir, legacyFiles[0]), nil
	}

	return "", fmt.Errorf(
		"no ledger state files at or before slot %d found in %s",
		maxSlot,
		ledgerDir,
	)
}

// stripLedgerSuffix removes known ledger state file suffixes
// (.lstate, _snapshot) so the numeric slot can be parsed.
func stripLedgerSuffix(name string) string {
	for _, suffix := range []string{".lstate", "_snapshot"} {
		name = strings.TrimSuffix(name, suffix)
	}
	return name
}

// FindUTxOTableFile searches for the UTxO table file in UTxO-HD
// format. Current snapshots store the table as ledger/<slot>/tables,
// while older exports used ledger/<slot>/tables/tvar. Returns an
// empty string if not found (legacy format).
func FindUTxOTableFile(extractedDir string) string {
	ledgerDir, err := findLedgerDir(extractedDir)
	if err != nil {
		return ""
	}

	entries, err := os.ReadDir(ledgerDir)
	if err != nil {
		return ""
	}

	// Find the most recent slot directory with tables/tvar
	var dirs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, ok := findUTxOTableInSlot(
			filepath.Join(ledgerDir, e.Name()),
		); ok {
			dirs = append(dirs, e.Name())
		}
	}

	dirs = sortNumericDesc(dirs)
	if len(dirs) == 0 {
		return ""
	}

	path, _ := findUTxOTableInSlot(
		filepath.Join(ledgerDir, dirs[0]),
	)
	return path
}

// FindUTxOTableFileForState returns the UTxO-HD table that belongs to the
// selected ledger state. Legacy ledger-state files embed their UTxO table and
// return an empty path.
func FindUTxOTableFileForState(statePath string) string {
	if filepath.Base(statePath) != "state" {
		return ""
	}
	path, _ := findUTxOTableInSlot(filepath.Dir(statePath))
	return path
}

func findUTxOTableInSlot(slotDir string) (string, bool) {
	candidates := []string{
		filepath.Join(slotDir, "tables"),
		filepath.Join(slotDir, "tables", "tvar"),
	}
	for _, path := range candidates {
		info, err := os.Stat(path)
		if err == nil && !info.IsDir() {
			return path, true
		}
	}
	return "", false
}

// findLedgerDir locates the ledger directory within an extracted
// snapshot.
func findLedgerDir(extractedDir string) (string, error) {
	candidates := []string{
		filepath.Join(extractedDir, "ledger"),
		filepath.Join(extractedDir, "db", "ledger"),
	}

	for _, c := range candidates {
		info, err := os.Stat(c)
		if err == nil && info.IsDir() {
			return c, nil
		}
	}

	return "", fmt.Errorf(
		"%w in %s (checked ledger/ and db/ledger/)",
		ErrLedgerDirNotFound,
		extractedDir,
	)
}

// sortNumericDesc filters names to only those that parse as uint64
// and sorts them in descending numeric order. Non-numeric names are
// silently excluded.
func sortNumericDesc(names []string) []string {
	var numeric []string
	for _, n := range names {
		if _, err := strconv.ParseUint(n, 10, 64); err == nil {
			numeric = append(numeric, n)
		}
	}
	slices.SortFunc(numeric, func(a, b string) int {
		na, _ := strconv.ParseUint(a, 10, 64)
		nb, _ := strconv.ParseUint(b, 10, 64)
		if na > nb {
			return -1
		}
		if na < nb {
			return 1
		}
		return 0
	})
	return numeric
}

// sortNumericSuffixDesc filters names to only those whose stripped
// suffix parses as uint64, and sorts descending by that numeric
// value. Non-numeric names (after stripping) are excluded.
func sortNumericSuffixDesc(names []string) []string {
	var numeric []string
	for _, n := range names {
		if _, err := strconv.ParseUint(
			stripLedgerSuffix(n), 10, 64,
		); err == nil {
			numeric = append(numeric, n)
		}
	}
	slices.SortFunc(numeric, func(a, b string) int {
		na, _ := strconv.ParseUint(
			stripLedgerSuffix(a), 10, 64,
		)
		nb, _ := strconv.ParseUint(
			stripLedgerSuffix(b), 10, 64,
		)
		if na > nb {
			return -1
		}
		if na < nb {
			return 1
		}
		return 0
	})
	return numeric
}

// isLedgerStateFile checks if a filename looks like a Cardano node
// ledger state file. These are typically named with slot numbers.
func isLedgerStateFile(name string) bool {
	// Skip known non-ledger files
	if strings.HasSuffix(name, ".checksum") ||
		strings.HasSuffix(name, ".lock") ||
		strings.HasSuffix(name, ".tmp") {
		return false
	}
	// Legacy format: just a number (the slot number)
	for _, c := range name {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(name) > 0
}

// ParseSnapshot reads and partially decodes a Cardano node ledger
// state snapshot file. The UTxO map, cert state, and stake snapshots
// are kept as raw CBOR for streaming decode later.
//
// Note: The entire file is read into memory because the CBOR parsing
// pipeline (decodeRawArray, cbor.Decode) requires a contiguous byte
// slice and does not support io.Reader streaming. For legacy-format
// mainnet snapshots the embedded UTxO map can be hundreds of MB.
// A future optimization could use mmap (syscall.Mmap) to avoid
// copying the file contents into Go heap memory, which would keep
// the OS page cache as the backing store rather than allocating a
// separate heap buffer.
func ParseSnapshot(path string) (*RawLedgerState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading snapshot file: %w", err)
	}

	return parseSnapshotData(data)
}

// parseSnapshotData decodes the snapshot CBOR data. It handles both
// the legacy format (ExtLedgerState directly) and the UTxO-HD format
// where a version wrapper precedes the state:
//   - Legacy: [<LedgerState>, <HeaderState>]
//   - UTxO-HD: [<version>, [<LedgerState>, <HeaderState>]]
func parseSnapshotData(data []byte) (*RawLedgerState, error) {
	outer, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding outer structure: %w",
			err,
		)
	}
	if len(outer) < 2 {
		return nil, fmt.Errorf(
			"outer structure has %d elements, expected 2",
			len(outer),
		)
	}

	// Detect UTxO-HD format: first element is a small integer
	// (version number), not an array (the telescope).
	var version uint64
	isUTxOHD := false
	if _, err := cbor.Decode(outer[0], &version); err == nil {
		isUTxOHD = true
		// UTxO-HD format: [version, ExtLedgerState]
		inner, err := decodeRawArray(outer[1])
		if err != nil {
			return nil, fmt.Errorf(
				"decoding ExtLedgerState (UTxO-HD v%d): %w",
				version,
				err,
			)
		}
		outer = inner
		if len(outer) < 2 {
			return nil, fmt.Errorf(
				"ExtLedgerState has %d elements, expected 2",
				len(outer),
			)
		}
	}

	// Extract all era boundaries from the telescope before
	// navigating to the current era. This gives us the full
	// epoch history needed for SlotToTime/TimeToSlot.
	telescopeData := cbor.RawMessage(outer[0])
	var boundsWarning error
	eraBounds, boundsErr := extractAllEraBounds(telescopeData)
	if boundsErr != nil {
		// Non-fatal: era bounds extraction can fail for older
		// snapshot formats. Epoch generation will fall back to
		// the single-epoch path.
		boundsWarning = boundsErr
		eraBounds = nil
	}

	// Navigate the HardFork telescope to find the current era
	eraIndex, currentState, err := navigateTelescope(
		telescopeData,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"navigating telescope: %w",
			err,
		)
	}

	// Parse the current era's state
	result, err := parseCurrentEra(eraIndex, currentState)
	if err != nil {
		return nil, err
	}
	result.UTxOHD = isUTxOHD

	result.EraBounds = eraBounds
	result.EraBoundsWarning = boundsWarning

	// Extract nonces from the HeaderState (outer[1]).
	// HeaderState = [WithOrigin AnnTip, ChainDepState telescope]
	nonces, nonceErr := parsePraosNonces(outer[1])
	if nonceErr != nil {
		if result.EraIndex >= EraShelley {
			return nil, fmt.Errorf(
				"extracting Praos HeaderState: %w", nonceErr,
			)
		}
		slog.Debug("nonce extraction skipped for pre-Praos state")
	} else if nonces != nil {
		result.EpochNonce = nonces.EpochNonce
		result.EvolvingNonce = nonces.EvolvingNonce
		result.CandidateNonce = nonces.CandidateNonce
		result.LastEpochBlockNonce = nonces.LastEpochBlockNonce
		result.OpCertCounters = nonces.OpCertCounters
	}

	return result, nil
}

// parseCurrentEra decodes the current era wrapper and extracts the
// NewEpochState fields.
func parseCurrentEra(
	eraIndex int,
	data []byte,
) (*RawLedgerState, error) {
	// Current = [<Bound>, <ShelleyLedgerState>]
	current, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding Current wrapper: %w",
			err,
		)
	}
	if len(current) < 2 {
		return nil, fmt.Errorf(
			"current wrapper has %d elements, expected 2",
			len(current),
		)
	}

	// Parse the Bound to get the era start slot and epoch.
	// Bound = [RelativeTime, SlotNo, EpochNo]
	eraBoundSlot, eraBoundEpoch, err := parseBound(
		current[0],
	)
	if err != nil {
		return nil, fmt.Errorf("parsing era bound: %w", err)
	}

	// ShelleyLedgerState:
	//   Legacy:  [tip, NewEpochState, transition]
	//   UTxO-HD: [version, [tip, NewEpochState, transition]]
	shelleyState, err := decodeRawArray(current[1])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding ShelleyLedgerState: %w",
			err,
		)
	}
	if len(shelleyState) < 2 {
		return nil, fmt.Errorf(
			"ShelleyLedgerState has %d elements, "+
				"expected at least 2",
			len(shelleyState),
		)
	}

	// Detect UTxO-HD version wrapper(s). The ShelleyLedgerState
	// may have one or more version prefixes:
	//   Wrapped:  [version, [tip, NES, transition]]
	//   Nested:   [version, [version2, [tip, NES, transition]]]
	//   Flat:     [version, tip, NES, transition]
	// Loop to peel off all version layers. Cap iterations to
	// guard against pathological nesting in malformed input.
	const maxVersionLayers = 5
	for versionDepth := 0; len(shelleyState) >= 2 &&
		versionDepth < maxVersionLayers; versionDepth++ {
		var ssVersion uint64
		if _, decErr := cbor.Decode(
			shelleyState[0], &ssVersion,
		); decErr != nil {
			break // First element is not an integer
		}
		if len(shelleyState) == 2 {
			// Wrapped: [version, [inner...]]
			inner, innerErr := decodeRawArray(
				shelleyState[1],
			)
			if innerErr != nil {
				return nil, fmt.Errorf(
					"decoding ShelleyLedgerState "+
						"inner (v%d): %w",
					ssVersion,
					innerErr,
				)
			}
			shelleyState = inner
			continue
		}
		// Flat: [version, tip, NES, transition, ...]
		shelleyState = shelleyState[1:]
		break
	}

	if len(shelleyState) < 2 {
		return nil, fmt.Errorf(
			"ShelleyLedgerState inner has %d elements, "+
				"expected at least 2",
			len(shelleyState),
		)
	}

	// Parse the tip (WithOrigin encoding)
	tip, err := parseTip(cbor.RawMessage(shelleyState[0]))
	if err != nil {
		return nil, fmt.Errorf("parsing tip: %w", err)
	}

	// NewEpochState = [epoch, blocks-prev, blocks-cur, EpochState,
	//                  reward-update, pool-distr, stashed]
	nes, err := decodeRawArray(shelleyState[1])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding NewEpochState: %w",
			err,
		)
	}
	if len(nes) < 4 {
		return nil, fmt.Errorf(
			"NewEpochState has %d elements, expected at least 4",
			len(nes),
		)
	}

	// Decode epoch number
	var epoch uint64
	if _, err := cbor.Decode(nes[0], &epoch); err != nil {
		return nil, fmt.Errorf("decoding epoch: %w", err)
	}

	// nesBprev and nesBcur. Both are mandatory strict fields of
	// NewEpochState, so the length check above already guarantees they are
	// present, and a map that will not decode means this is not a
	// NewEpochState rather than a snapshot that omits them.
	blocksPrev, err := parseBlocksMade(nes[1])
	if err != nil {
		return nil, fmt.Errorf("decoding blocks made in previous epoch: %w", err)
	}
	blocksCur, err := parseBlocksMade(nes[2])
	if err != nil {
		return nil, fmt.Errorf("decoding blocks made in current epoch: %w", err)
	}

	// EpochState = [AccountState, LedgerState, SnapShots, NonMyopic]
	es, err := decodeRawArray(nes[3])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding EpochState: %w",
			err,
		)
	}
	if len(es) < 4 {
		return nil, fmt.Errorf(
			"EpochState has %d elements, expected at least 4",
			len(es),
		)
	}

	// AccountState = [treasury, reserves]
	var acctState []uint64
	if _, err := cbor.Decode(es[0], &acctState); err != nil {
		return nil, fmt.Errorf(
			"decoding AccountState: %w",
			err,
		)
	}
	if len(acctState) < 2 {
		return nil, fmt.Errorf(
			"AccountState has %d elements, expected at least 2",
			len(acctState),
		)
	}
	treasury := acctState[0]
	reserves := acctState[1]

	// LedgerState_inner = [CertState, UTxOState]
	// Haskell encodes CertState first for sharing optimization.
	ls, err := decodeRawArray(es[1])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding inner LedgerState: %w",
			err,
		)
	}
	if len(ls) < 2 {
		return nil, fmt.Errorf(
			"inner LedgerState has %d elements, expected 2",
			len(ls),
		)
	}

	// UTxOState = [UTxO, deposited, fees, GovState, ...]
	utxoState, err := decodeRawArray(ls[1])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding UTxOState: %w",
			err,
		)
	}
	if len(utxoState) < 1 {
		return nil, fmt.Errorf(
			"UTxOState has %d elements, expected at least 1",
			len(utxoState),
		)
	}

	// UTxOState[2] is the fee pot accumulated so far this epoch. It is one
	// of the three addends of the reward pot (see ledger/rewards: the pot is
	// incentives + fees), so a reward round computed without it understates
	// every pool's reward. Decoding it is what lets a Mithril bootstrap seed
	// a complete RewardAdaPots row rather than a partial one. Older eras may
	// carry a shorter array, so its absence is tolerated and left at zero.
	var fees uint64
	if len(utxoState) > 2 {
		if _, err := cbor.Decode(utxoState[2], &fees); err != nil {
			return nil, fmt.Errorf("decoding UTxOState fees: %w", err)
		}
	}

	result := &RawLedgerState{
		EraIndex:      eraIndex,
		Epoch:         epoch,
		Tip:           tip,
		Treasury:      treasury,
		Reserves:      reserves,
		Fees:          fees,
		EraBoundSlot:  eraBoundSlot,
		EraBoundEpoch: eraBoundEpoch,
		UTxOData:      utxoState[0], // The UTxO map
		CertStateData: ls[0],        // [VState, PState, DState]
		SnapShotsData: es[2],        // mark/set/go
		BlocksPrev:    blocksPrev,
		BlocksCur:     blocksCur,
	}
	if len(nes) > 5 {
		result.PoolDistrData = nes[5]
	}

	// GovState (index 3 in UTxOState)
	if len(utxoState) > 3 {
		result.GovStateData = utxoState[3]
		pparamsData, prevPParamsData, pparamsErr := extractPParamsData(
			eraIndex,
			utxoState[3],
		)
		if pparamsErr != nil {
			return nil, fmt.Errorf(
				"extracting protocol parameters: %w",
				pparamsErr,
			)
		}
		result.PParamsData = pparamsData
		result.PrevPParamsData = prevPParamsData
	}

	return result, nil
}

// parseTip decodes the tip from a ShelleyLedgerState.
//
// The tip uses the WithOrigin encoding:
//   - Origin (genesis): empty array []
//   - At tip: [ShelleyTip] where ShelleyTip = [slot, blockNo, hash]
//
// Legacy format may encode directly as [slot, hash].
func parseTip(data cbor.RawMessage) (*SnapshotTip, error) {
	var tipArr []cbor.RawMessage
	if _, err := cbor.Decode(data, &tipArr); err != nil {
		return nil, fmt.Errorf("decoding tip: %w", err)
	}

	// WithOrigin encoding: empty array = Origin.
	// Mithril snapshots should always have a tip; Origin
	// means no blocks have been applied which is invalid.
	if len(tipArr) == 0 {
		return nil, errors.New("tip is Origin (empty)")
	}

	// WithOrigin At: array(1) containing the ShelleyTip
	if len(tipArr) == 1 {
		var innerTip []cbor.RawMessage
		if _, err := cbor.Decode(
			tipArr[0], &innerTip,
		); err != nil {
			return nil, fmt.Errorf(
				"decoding ShelleyTip: %w", err,
			)
		}
		// ShelleyTip = [slot, blockNo, hash]
		if len(innerTip) < 3 {
			return nil, fmt.Errorf(
				"ShelleyTip has %d elements, expected 3",
				len(innerTip),
			)
		}
		var slot uint64
		if _, err := cbor.Decode(
			innerTip[0], &slot,
		); err != nil {
			return nil, fmt.Errorf(
				"decoding tip slot: %w", err,
			)
		}
		var blockHash []byte
		if _, err := cbor.Decode(
			innerTip[2], &blockHash,
		); err != nil {
			return nil, fmt.Errorf(
				"decoding tip hash: %w", err,
			)
		}
		return &SnapshotTip{
			Slot:      slot,
			BlockHash: blockHash,
		}, nil
	}

	// Legacy format: [slot, hash] directly
	if len(tipArr) < 2 {
		return nil, fmt.Errorf(
			"legacy tip has %d elements, expected at least 2",
			len(tipArr),
		)
	}
	var slot uint64
	if _, err := cbor.Decode(
		tipArr[0], &slot,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding tip slot: %w", err,
		)
	}
	var blockHash []byte
	if _, err := cbor.Decode(
		tipArr[1], &blockHash,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding tip hash: %w", err,
		)
	}
	return &SnapshotTip{
		Slot:      slot,
		BlockHash: blockHash,
	}, nil
}

// parseBound decodes a telescope Bound from CBOR. The Haskell type is:
//
//	Bound = [RelativeTime, SlotNo, EpochNo]
//
// Returns the slot and epoch from the bound.
func parseBound(data []byte) (uint64, uint64, error) {
	var boundArr []cbor.RawMessage
	if _, err := cbor.Decode(data, &boundArr); err != nil {
		return 0, 0, fmt.Errorf("decoding bound array: %w", err)
	}
	if len(boundArr) < 3 {
		return 0, 0, fmt.Errorf(
			"bound has %d elements, expected 3",
			len(boundArr),
		)
	}
	// boundArr[0] is RelativeTime (skip)
	var slot uint64
	if _, err := cbor.Decode(boundArr[1], &slot); err != nil {
		return 0, 0, fmt.Errorf(
			"decoding bound slot: %w", err,
		)
	}
	var epoch uint64
	if _, err := cbor.Decode(boundArr[2], &epoch); err != nil {
		return 0, 0, fmt.Errorf(
			"decoding bound epoch: %w", err,
		)
	}
	return slot, epoch, nil
}

// praosNonces holds the nonces extracted from the PraosState in the
// HeaderState's ChainDepState telescope.
type praosNonces struct {
	// OpCertCounters is the certified per-pool operational-certificate counter
	// state. Keys are 28-byte pool cold-key hashes encoded as strings.
	OpCertCounters map[string]uint64
	// EvolvingNonce is the rolling nonce (eta_v) updated with each
	// block's VRF output. This is needed as the starting nonce for
	// block processing after a mithril snapshot restore.
	EvolvingNonce []byte
	// EpochNonce is the epoch nonce (eta_0) used for VRF leader
	// election in the current epoch.
	EpochNonce []byte
	// CandidateNonce is the current Praos candidate nonce (eta_c)
	// at the imported tip.
	CandidateNonce []byte
	// LastEpochBlockNonce is the Praos last applied block hash used
	// in epoch nonce calculation.
	LastEpochBlockNonce []byte
}

// parsePraosNonces extracts the evolving nonce and epoch nonce from
// the HeaderState CBOR.
//
// HeaderState = [WithOrigin AnnTip, HardForkState ChainDepState]
//
// The ChainDepState telescope has the same structure as the ledger
// telescope. The current era's state (Praos/TPraos) is:
//
//	[lastSlot, ocertCounters, evolvingNonce, candidateNonce,
//	 epochNonce, labNonce, lastEpochBlockNonce]
//
// Nonce encoding: [0] = NeutralNonce, [1, hash] = Nonce(hash)
func parsePraosNonces(headerStateData []byte) (*praosNonces, error) {
	hs, err := decodeRawArray(headerStateData)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding HeaderState: %w", err,
		)
	}
	if len(hs) < 2 {
		return nil, fmt.Errorf(
			"HeaderState has %d elements, expected 2",
			len(hs),
		)
	}

	// Navigate the ChainDepState telescope
	eraIdx, chainDepState, err := navigateTelescope(
		cbor.RawMessage(hs[1]),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"navigating ChainDepState telescope: %w", err,
		)
	}

	// The current era entry is [Bound, PraosState]
	currentEra, err := decodeRawArray(chainDepState)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding ChainDepState current era: %w", err,
		)
	}
	if len(currentEra) < 2 {
		return nil, fmt.Errorf(
			"ChainDepState current has %d elements, "+
				"expected 2 (era=%d)",
			len(currentEra), eraIdx,
		)
	}

	// PraosState may have a version wrapper:
	//   [version, [lastSlot, ocertCounters, nonces...]]
	// Or directly:
	//   [lastSlot, ocertCounters, nonces...]
	praosState, err := decodeRawArray(currentEra[1])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding PraosState: %w", err,
		)
	}

	// Peel version wrapper(s). The wrapped format is
	// [version, [inner...]] where the second element is an
	// array. The flat format is [lastSlot, ocertCounters,
	// nonces...] where lastSlot is also an integer, so we
	// distinguish them by checking whether the second element
	// decodes as an array.
	const maxPraosVersionLayers = 5
	for versionDepth := 0; len(praosState) >= 2 &&
		versionDepth < maxPraosVersionLayers; versionDepth++ {
		var ver uint64
		if _, decErr := cbor.Decode(
			praosState[0], &ver,
		); decErr != nil {
			break // First element is not an integer
		}
		// Try to decode the second element as an array. If it
		// succeeds, the format is [version, [inner...]] and we
		// unwrap. If it fails, the first integer is lastSlot
		// (flat format) and we must not drop it.
		inner, innerErr := decodeRawArray(praosState[1])
		if innerErr != nil {
			break // Flat format — first element is lastSlot
		}
		praosState = inner
	}

	return extractPraosNonces(praosState)
}

// extractPraosNonces extracts the nonce fields from a decoded
// PraosState array. Split out from parsePraosNonces so tests can
// exercise field-index handling without constructing the full
// HeaderState/ChainDepState/Telescope CBOR wrapping.
//
// PraosState (Conway/Babbage, ouroboros-consensus
// Ouroboros.Consensus.Protocol.Praos):
//
//	[0] lastSlot
//	[1] ocertCounters
//	[2] evolvingNonce
//	[3] candidateNonce
//	[4] epochNonce
//	[5] previousEpochNonce
//	[6] labNonce
//	[7] lastEpochBlockNonce
//
// The cardano-ledger epoch-boundary formula uses
// `lastEpochBlockNonce` (index 7), not `labNonce`:
//
//	newEpochNonce = candidateNonce ⭒ lastEpochBlockNonce
//
// Reading the wrong index yields the wrong eta0 for the next
// epoch, which manifests as VRF verification failure on every
// header in the first post-bootstrap epoch.
//
// Older snapshots predating `previousEpochNonce` had a 7-element
// PraosState; in that shape `lastEpochBlockNonce` was at index 6.
// We dispatch on length so a Mithril snapshot from either shape
// produces the right value in `result.LastEpochBlockNonce`.
func extractPraosNonces(praosState [][]byte) (*praosNonces, error) {
	// lastEpochBlockNonce is always the final element of PraosState —
	// index 7 in the 8-field shape, index 6 in the older 7-field shape.
	// Reject unknown lengths rather than guess: a future shape that
	// extends the array would push the final element past index 7, and
	// silently reading index 7 would yield the wrong nonce.
	var lastEpochBlockNonceIdx int
	switch len(praosState) {
	case 8:
		lastEpochBlockNonceIdx = 7
	case 7:
		lastEpochBlockNonceIdx = 6
	default:
		return nil, fmt.Errorf(
			"unsupported PraosState length %d, expected 7 or 8",
			len(praosState),
		)
	}

	opCertCounters, err := decodeOpCertCounters(praosState[1])
	if err != nil {
		return nil, fmt.Errorf("decoding opcert counters: %w", err)
	}

	result := &praosNonces{OpCertCounters: opCertCounters}

	evolvingNonce, err := decodeNonce(praosState[2])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding evolving nonce: %w", err,
		)
	}
	if evolvingNonce != nil && len(evolvingNonce) != 32 {
		return nil, fmt.Errorf(
			"invalid evolving nonce length %d, expected 32",
			len(evolvingNonce),
		)
	}
	result.EvolvingNonce = evolvingNonce

	candidateNonce, err := decodeNonce(praosState[3])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding candidate nonce: %w", err,
		)
	}
	if candidateNonce != nil && len(candidateNonce) != 32 {
		return nil, fmt.Errorf(
			"invalid candidate nonce length %d, expected 32",
			len(candidateNonce),
		)
	}
	result.CandidateNonce = candidateNonce

	epochNonce, err := decodeNonce(praosState[4])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding epoch nonce: %w", err,
		)
	}
	if epochNonce != nil && len(epochNonce) != 32 {
		return nil, fmt.Errorf(
			"invalid epoch nonce length %d, expected 32",
			len(epochNonce),
		)
	}
	result.EpochNonce = epochNonce

	lastEpochBlockNonce, err := decodeNonce(
		praosState[lastEpochBlockNonceIdx],
	)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding last epoch block nonce: %w", err,
		)
	}
	if lastEpochBlockNonce != nil && len(lastEpochBlockNonce) != 32 {
		return nil, fmt.Errorf(
			"invalid last epoch block nonce length %d, expected 32",
			len(lastEpochBlockNonce),
		)
	}
	result.LastEpochBlockNonce = lastEpochBlockNonce

	return result, nil
}

// decodeOpCertCounters decodes the Praos ocertCounters map. Every key is a
// BlockIssuer key hash and must therefore be a 28-byte byte string. Rejecting
// malformed and duplicate entries keeps the certified HeaderState an
// unambiguous baseline for subsequent block validation.
func decodeOpCertCounters(data []byte) (map[string]uint64, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64, len(entries))
	for i, entry := range entries {
		var poolKeyHash []byte
		if _, err := cbor.Decode(entry.KeyRaw, &poolKeyHash); err != nil {
			return nil, fmt.Errorf("decoding key %d: %w", i, err)
		}
		if len(poolKeyHash) != 28 {
			return nil, fmt.Errorf(
				"key %d has length %d, expected 28", i, len(poolKeyHash),
			)
		}
		var counter uint64
		if _, err := cbor.Decode(entry.ValueRaw, &counter); err != nil {
			return nil, fmt.Errorf("decoding value %d: %w", i, err)
		}
		key := string(poolKeyHash)
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate pool key at entry %d", i)
		}
		result[key] = counter
	}
	return result, nil
}

// decodeNonce decodes a Cardano Nonce CBOR value.
// NeutralNonce = [0], Nonce = [1, hash_bytes]
func decodeNonce(data []byte) ([]byte, error) {
	var nonceArr []cbor.RawMessage
	if _, err := cbor.Decode(data, &nonceArr); err != nil {
		return nil, fmt.Errorf("decoding nonce: %w", err)
	}
	if len(nonceArr) == 0 {
		return nil, errors.New("empty nonce array")
	}
	var tag uint64
	if _, err := cbor.Decode(nonceArr[0], &tag); err != nil {
		return nil, fmt.Errorf("decoding nonce tag: %w", err)
	}
	if tag == 0 {
		return nil, nil // NeutralNonce
	}
	if tag == 1 {
		if len(nonceArr) < 2 {
			return nil, fmt.Errorf(
				"nonce tag 1 but missing hash element "+
					"(array length %d)",
				len(nonceArr),
			)
		}
		var hash []byte
		if _, err := cbor.Decode(
			nonceArr[1], &hash,
		); err != nil {
			return nil, fmt.Errorf(
				"decoding nonce hash: %w", err,
			)
		}
		return hash, nil
	}
	return nil, fmt.Errorf("unexpected nonce tag %d", tag)
}

// ParseSnapShots decodes the stake distribution snapshots
// (mark, set, go) from the EpochState.
// SnapShots = [mark, set, go, fee]
// Each SnapShot is [Stake, Delegations, PoolParams] in older ledger
// states. Current UTxO-HD snapshots encode [StakeWithPool, PoolParams],
// where each stake value is [Coin, PoolKeyHash].
func ParseSnapShots(data cbor.RawMessage) (*ParsedSnapShots, error) {
	ss, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf("decoding SnapShots: %w", err)
	}
	if len(ss) < 3 {
		return nil, fmt.Errorf(
			"SnapShots has %d elements, expected at least 3",
			len(ss),
		)
	}

	// Warnings from snapshot parsers indicate skipped entries.
	var warnings []error

	mark, err := parseSnapShot(ss[0])
	if err != nil {
		if mark == nil {
			return nil, fmt.Errorf(
				"parsing mark snapshot: %w", err,
			)
		}
		warnings = append(warnings, fmt.Errorf(
			"mark: %w", err,
		))
	}

	set, err := parseSnapShot(ss[1])
	if err != nil {
		if set == nil {
			return nil, fmt.Errorf(
				"parsing set snapshot: %w", err,
			)
		}
		warnings = append(warnings, fmt.Errorf(
			"set: %w", err,
		))
	}

	goSnap, err := parseSnapShot(ss[2])
	if err != nil {
		if goSnap == nil {
			return nil, fmt.Errorf(
				"parsing go snapshot: %w", err,
			)
		}
		warnings = append(warnings, fmt.Errorf(
			"go: %w", err,
		))
	}

	var fee uint64
	if len(ss) > 3 {
		if _, err := cbor.Decode(ss[3], &fee); err != nil {
			// Fee might be optional or zero, don't fail
			fee = 0
		}
	}

	return &ParsedSnapShots{
		Mark: *mark,
		Set:  *set,
		Go:   *goSnap,
		Fee:  fee,
	}, errors.Join(warnings...)
}

// parseSnapShot decodes a single SnapShot.
// SnapShot = [Stake, Delegations, PoolParams] or
// [StakeWithPool, PoolParams].
func parseSnapShot(
	data []byte,
) (*ParsedSnapShot, error) {
	snap, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf("decoding SnapShot: %w", err)
	}
	if len(snap) < 2 {
		return nil, fmt.Errorf(
			"SnapShot has %d elements, expected at least 2",
			len(snap),
		)
	}

	var warnings []error
	var stake map[string]uint64
	var stakeTags map[string]uint8
	var delegations map[string][]byte
	var poolParams map[string]*ParsedPool

	if len(snap) == 2 {
		stake, stakeTags, delegations, err = parseStakeWithPoolMap(snap[0])
		if err != nil {
			if stake == nil || delegations == nil {
				return nil, fmt.Errorf(
					"parsing stake-with-pool map: %w", err,
				)
			}
			warnings = append(warnings, err)
		}

		poolParams, err = parsePoolParamsMap(snap[1])
		if err != nil {
			if poolParams == nil {
				return nil, fmt.Errorf(
					"parsing pool params map: %w",
					err,
				)
			}
			warnings = append(warnings, err)
		}
	} else {
		// Parse Stake: map[Credential]Coin
		// Warnings from these parsers indicate skipped entries,
		// not fatal errors, so we collect them.
		stake, stakeTags, err = parseStakeMap(snap[0])
		if err != nil {
			if stake == nil {
				return nil, fmt.Errorf(
					"parsing stake map: %w", err,
				)
			}
			warnings = append(warnings, err)
		}

		// Parse Delegations: map[Credential]PoolKeyHash
		delegations, err = parseDelegationMap(snap[1])
		if err != nil {
			if delegations == nil {
				return nil, fmt.Errorf(
					"parsing delegation map: %w", err,
				)
			}
			warnings = append(warnings, err)
		}

		// Parse PoolParams: map[PoolKeyHash]PoolParams
		poolParams, err = parsePoolParamsMap(snap[2])
		if err != nil {
			if poolParams == nil {
				return nil, fmt.Errorf(
					"parsing pool params map: %w", err,
				)
			}
			warnings = append(warnings, err)
		}
	}

	return &ParsedSnapShot{
		Stake:       stake,
		StakeTags:   stakeTags,
		Delegations: delegations,
		PoolParams:  poolParams,
	}, errors.Join(warnings...)
}

// ParseActivePoolDistribution decodes NewEpochState.pool-distr:
// map[PoolKeyHash][UnitInterval, active stake, VrfKeyHash, LeiosKey]. Older
// states omit active stake and/or LeiosKey. The UnitInterval is the exact
// active stake fraction (sigma) used by Praos leader eligibility.
func ParseActivePoolDistribution(
	data cbor.RawMessage,
) ([]ParsedActivePoolStake, error) {
	if len(data) == 0 {
		return nil, nil
	}
	mapData, totalActiveStake, hasTotal, err := activePoolDistributionMapData(
		data,
	)
	if err != nil {
		return nil, err
	}
	entries, err := decodeMapEntries(mapData)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding active pool distribution: %w", err,
		)
	}
	result := make([]ParsedActivePoolStake, 0, len(entries))
	for idx, entry := range entries {
		var poolKeyHash []byte
		if _, err := cbor.Decode(entry.KeyRaw, &poolKeyHash); err != nil {
			return nil, fmt.Errorf(
				"active pool distribution entry %d: decoding pool key hash: %w",
				idx,
				err,
			)
		}
		if len(poolKeyHash) != 28 {
			return nil, fmt.Errorf(
				"active pool distribution entry %d: pool key hash has %d bytes, expected 28",
				idx,
				len(poolKeyHash),
			)
		}

		fields, err := decodeRawArray(entry.ValueRaw)
		if err != nil {
			return nil, fmt.Errorf(
				"active pool distribution entry %d: decoding value: %w",
				idx,
				err,
			)
		}
		if len(fields) < 2 || len(fields) > 4 {
			return nil, fmt.Errorf(
				"active pool distribution entry %d: value has %d fields, expected 2, 3, or 4",
				idx,
				len(fields),
			)
		}

		stakeNumerator, stakeDenominator, ok := parseRational(fields[0])
		if !ok {
			return nil, fmt.Errorf(
				"active pool distribution entry %d: stake fraction is not a non-negative uint64 ratio",
				idx,
			)
		}

		vrfFieldIdx := 1
		if len(fields) >= 3 {
			vrfFieldIdx = 2
			var activeStake uint64
			if _, err := cbor.Decode(fields[1], &activeStake); err != nil {
				return nil, fmt.Errorf(
					"active pool distribution entry %d: decoding active stake: %w",
					idx,
					err,
				)
			}
			if hasTotal {
				actual := new(big.Rat).SetFrac(
					new(big.Int).SetUint64(stakeNumerator),
					new(big.Int).SetUint64(stakeDenominator),
				)
				expected := new(big.Rat).SetFrac(
					new(big.Int).SetUint64(activeStake),
					new(big.Int).SetUint64(totalActiveStake),
				)
				if actual.Cmp(expected) != 0 {
					return nil, fmt.Errorf(
						"active pool distribution entry %d: stake fraction does not match active stake",
						idx,
					)
				}
				stakeNumerator = activeStake
				stakeDenominator = totalActiveStake
			}
		}

		var vrfKeyHash []byte
		if _, err := cbor.Decode(fields[vrfFieldIdx], &vrfKeyHash); err != nil {
			return nil, fmt.Errorf(
				"active pool distribution entry %d: decoding VRF key hash: %w",
				idx,
				err,
			)
		}
		if len(vrfKeyHash) != 32 {
			return nil, fmt.Errorf(
				"active pool distribution entry %d: VRF key hash has %d bytes, expected 32",
				idx,
				len(vrfKeyHash),
			)
		}

		var leiosKey *lcommon.LeiosKey
		if len(fields) == 4 {
			leiosKey, err = decodeOptionalLeiosKey(fields[3])
			if err != nil {
				return nil, fmt.Errorf(
					"active pool distribution entry %d: %w",
					idx,
					err,
				)
			}
		}
		var leiosKeyPublic, leiosKeyPossessionProof []byte
		if leiosKey != nil {
			leiosKeyPublic = append([]byte(nil), leiosKey.PublicKey...)
			leiosKeyPossessionProof = append(
				[]byte(nil), leiosKey.PossessionProof...,
			)
		}

		result = append(result, ParsedActivePoolStake{
			PoolKeyHash:             slices.Clone(poolKeyHash),
			StakeNumerator:          stakeNumerator,
			StakeDenominator:        stakeDenominator,
			VrfKeyHash:              slices.Clone(vrfKeyHash),
			LeiosKeyPublic:          leiosKeyPublic,
			LeiosKeyPossessionProof: leiosKeyPossessionProof,
		})
	}
	return result, nil
}

func activePoolDistributionMapData(
	data cbor.RawMessage,
) ([]byte, uint64, bool, error) {
	if len(data) == 0 {
		return nil, 0, false, errors.New("active pool distribution is empty")
	}
	switch data[0] >> 5 {
	case 5:
		return data, 0, false, nil
	case 4:
		fields, err := decodeRawArray(data)
		if err != nil {
			return nil, 0, false, fmt.Errorf(
				"decoding active pool distribution container: %w",
				err,
			)
		}
		if len(fields) != 2 {
			return nil, 0, false, fmt.Errorf(
				"active pool distribution container has %d fields, expected 2",
				len(fields),
			)
		}
		var totalActiveStake uint64
		if _, err := cbor.Decode(fields[1], &totalActiveStake); err != nil {
			return nil, 0, false, fmt.Errorf(
				"decoding active pool distribution total stake: %w",
				err,
			)
		}
		if totalActiveStake == 0 {
			return nil, 0, false, errors.New(
				"active pool distribution total stake is zero",
			)
		}
		return fields[0], totalActiveStake, true, nil
	default:
		return nil, 0, false, fmt.Errorf(
			"decoding active pool distribution: expected map or container array, got major type %d",
			data[0]>>5,
		)
	}
}

// parseStakeMap decodes a credential -> coin map. Handles both
// definite and indefinite-length maps. Returns a warning if any
// entries were skipped due to decode errors.
// parseStakeMap decodes a credential->coin map. The returned tag map carries
// each credential's type alongside, because the result is keyed by hash alone
// and a script credential can share a hash with a key credential; attributing
// a script delegator's stake to a key credential would misdirect both its
// reward and its contribution to leadership stake.
func parseStakeMap(
	data cbor.RawMessage,
) (map[string]uint64, map[string]uint8, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"decoding stake map: %w", err,
		)
	}

	result := make(map[string]uint64, len(entries))
	tags := make(map[string]uint8, len(entries))
	var skipped int
	for _, entry := range entries {
		cred, err := parseCredential(entry.KeyRaw)
		if err != nil {
			skipped++
			continue
		}

		var amount uint64
		if _, err := cbor.Decode(
			entry.ValueRaw, &amount,
		); err != nil {
			skipped++
			continue
		}

		if cred.Hash == nil {
			skipped++
			continue
		}
		key := hex.EncodeToString(cred.Hash)
		result[key] = amount
		// The credential type is discarded by the map key, which is the
		// hash alone. Reward and leadership stake are attributed per
		// credential, and a script credential and a key credential can share
		// a hash, so the type has to travel alongside or a script
		// delegator's stake is credited to a key credential.
		tags[key] = uint8(cred.Type) // #nosec G115 -- 0 or 1
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"stake map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return result, tags, warning
}

// parseStakeWithPoolMap decodes the UTxO-HD compact snapshot map:
// map[Credential][Coin, PoolKeyHash].
// parseStakeWithPoolMap decodes the compact UTxO-HD shape. It returns the
// credential types alongside for the same reason parseStakeMap does: the maps
// are keyed by credential hash alone, and a script credential can share a hash
// with a key credential, so attributing a script delegator's stake to a key
// credential would misdirect both its reward and its share of leadership
// stake. This is the shape current snapshots use, so it is the path that
// decides whether that attribution is right in practice.
func parseStakeWithPoolMap(
	data cbor.RawMessage,
) (map[string]uint64, map[string]uint8, map[string][]byte, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(
			"decoding stake-with-pool map: %w", err,
		)
	}

	stake := make(map[string]uint64, len(entries))
	tags := make(map[string]uint8, len(entries))
	delegations := make(map[string][]byte, len(entries))
	var skipped int
	for _, entry := range entries {
		cred, err := parseCredential(entry.KeyRaw)
		if err != nil || cred.Hash == nil {
			skipped++
			continue
		}

		value, err := decodeRawArray(entry.ValueRaw)
		if err != nil || len(value) < 2 {
			skipped++
			continue
		}

		var amount uint64
		if _, err := cbor.Decode(value[0], &amount); err != nil {
			skipped++
			continue
		}

		var poolHash []byte
		if _, err := cbor.Decode(
			value[1], &poolHash,
		); err != nil || len(poolHash) != 28 {
			skipped++
			continue
		}

		credKey := hex.EncodeToString(cred.Hash)
		stake[credKey] = amount
		tags[credKey] = uint8(cred.Type) // #nosec G115 -- 0 or 1
		delegations[credKey] = poolHash
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"stake-with-pool map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return stake, tags, delegations, warning
}

// parseDelegationMap decodes a credential -> pool key hash map.
// Handles both definite and indefinite-length maps. Returns a
// warning if any entries were skipped due to decode errors.
// parseBlocksMade decodes a NewEpochState BlocksMade field: a CBOR map from a
// 28-byte pool cold-key hash to the number of blocks that pool minted in the
// epoch the field describes.
//
// Unlike the stake and delegation maps, a malformed entry is an error rather
// than a skipped one. Those maps drop an entry the node then simply does not
// pay; dropping a block count instead lowers one pool's beta and the epoch
// total that every other pool's beta divides by, so a silently partial map
// yields a complete-looking reward distribution at the wrong amounts for every
// pool at once. An absent map is not representable in the reference either:
// nesBprev and nesBcur are strict, non-optional fields.
func parseBlocksMade(data cbor.RawMessage) (map[string]uint64, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64, len(entries))
	for i, entry := range entries {
		var poolKeyHash []byte
		if _, err := cbor.Decode(entry.KeyRaw, &poolKeyHash); err != nil {
			return nil, fmt.Errorf("entry %d: decoding pool key hash: %w", i, err)
		}
		if len(poolKeyHash) != credentialHashSize {
			return nil, fmt.Errorf(
				"entry %d: pool key hash is %d bytes, expected %d",
				i, len(poolKeyHash), credentialHashSize,
			)
		}
		var blocks uint64
		if _, err := cbor.Decode(entry.ValueRaw, &blocks); err != nil {
			return nil, fmt.Errorf(
				"entry %d: decoding block count for pool %x: %w",
				i, poolKeyHash, err,
			)
		}
		key := string(poolKeyHash)
		if _, dup := result[key]; dup {
			return nil, fmt.Errorf(
				"entry %d: duplicate pool key hash %x", i, poolKeyHash,
			)
		}
		result[key] = blocks
	}
	return result, nil
}

func parseDelegationMap(
	data cbor.RawMessage,
) (map[string][]byte, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding delegation map: %w", err,
		)
	}

	result := make(map[string][]byte, len(entries))
	var skipped int
	for _, entry := range entries {
		cred, err := parseCredential(entry.KeyRaw)
		if err != nil {
			skipped++
			continue
		}

		if cred.Hash == nil {
			skipped++
			continue
		}

		var poolHash []byte
		if _, err := cbor.Decode(
			entry.ValueRaw, &poolHash,
		); err != nil || len(poolHash) != 28 {
			skipped++
			continue
		}

		result[hex.EncodeToString(cred.Hash)] = poolHash
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"delegation map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return result, warning
}

// parsePoolParamsMap decodes a pool key hash -> pool params map.
// Handles both definite and indefinite-length maps. Returns a
// warning if any entries were skipped due to decode errors.
func parsePoolParamsMap(
	data cbor.RawMessage,
) (map[string]*ParsedPool, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding pool params map: %w", err,
		)
	}

	result := make(
		map[string]*ParsedPool,
		len(entries),
	)
	var skipped int
	for _, entry := range entries {
		var poolKeyHash []byte
		if _, pErr := cbor.Decode(
			entry.KeyRaw, &poolKeyHash,
		); pErr != nil || len(poolKeyHash) != 28 {
			skipped++
			continue
		}

		pool, err := parsePoolParamsOrDistr(
			poolKeyHash, entry.ValueRaw,
		)
		if err != nil {
			skipped++
			continue
		}

		result[hex.EncodeToString(poolKeyHash)] = pool
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"pool params map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return result, warning
}

// AggregatePoolStake aggregates per-credential stake into per-pool
// totals, producing PoolStakeSnapshot models suitable for database
// storage.
func AggregatePoolStake(
	snap *ParsedSnapShot,
	epoch uint64,
	snapshotType string,
	capturedSlot uint64,
) []*models.PoolStakeSnapshot {
	if snap == nil || snap.Delegations == nil {
		return nil
	}

	// Build per-pool aggregation
	type poolAgg struct {
		totalStake     uint64
		delegatorCount uint64
	}
	poolMap := make(map[string]*poolAgg)

	for credHex, poolHash := range snap.Delegations {
		poolHex := hex.EncodeToString(poolHash)
		agg, ok := poolMap[poolHex]
		if !ok {
			agg = &poolAgg{}
			poolMap[poolHex] = agg
		}

		// Add this credential's stake to the pool total.
		// Only count delegators that have non-zero stake so the
		// count is consistent with totalStake.
		if stake, ok := snap.Stake[credHex]; ok && stake > 0 {
			agg.totalStake += stake
			agg.delegatorCount++
		}
	}

	// Convert to models, skipping pools with zero stake
	// (delegators without a stake entry should not produce
	// misleading snapshot records).
	snapshots := make(
		[]*models.PoolStakeSnapshot,
		0,
		len(poolMap),
	)
	for poolHex, agg := range poolMap {
		if agg.totalStake == 0 {
			continue
		}
		poolKeyHash, err := hex.DecodeString(poolHex)
		if err != nil {
			// poolHex was self-encoded via hex.EncodeToString,
			// so decode should never fail.
			continue
		}

		pool := snap.PoolParams[poolHex]
		var leiosKeyPublic, leiosKeyPossessionProof []byte
		if pool != nil {
			leiosKeyPublic = append([]byte(nil), pool.LeiosKeyPublic...)
			leiosKeyPossessionProof = append(
				[]byte(nil), pool.LeiosKeyPossessionProof...,
			)
		}

		snapshots = append(snapshots, &models.PoolStakeSnapshot{
			Epoch:                   epoch,
			SnapshotType:            snapshotType,
			PoolKeyHash:             poolKeyHash,
			TotalStake:              types.Uint64(agg.totalStake),
			DelegatorCount:          agg.delegatorCount,
			CapturedSlot:            capturedSlot,
			LeiosKeyPublic:          leiosKeyPublic,
			LeiosKeyPossessionProof: leiosKeyPossessionProof,
			CalculationVersion:      models.RewardStakeCalculationVersion,
		})
	}

	return snapshots
}

// VerifySnapshotDigest computes the SHA-256 digest of a snapshot
// archive file and compares it against the expected digest from the
// Mithril aggregator.
func VerifySnapshotDigest(
	archivePath string,
	expectedDigest string,
) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf(
			"opening archive for digest verification: %w",
			err,
		)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf(
			"computing archive digest: %w",
			err,
		)
	}

	actualDigest := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(actualDigest, expectedDigest) {
		return fmt.Errorf(
			"snapshot digest mismatch: expected %s, got %s",
			expectedDigest,
			actualDigest,
		)
	}

	return nil
}

// VerifyChecksumFile verifies the CRC32 checksum of a ledger state
// file against its companion .checksum file.
func VerifyChecksumFile(lstatePath string) error {
	checksumPath := lstatePath + ".checksum"

	// Read expected checksum
	checksumData, err := os.ReadFile(checksumPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No checksum file is not an error - older snapshots
			// may not have one
			return nil
		}
		return fmt.Errorf(
			"reading checksum file: %w",
			err,
		)
	}

	expectedHex := strings.TrimSpace(string(checksumData))
	if expectedHex == "" {
		return nil // Empty checksum file, skip verification
	}
	decoded, err := hex.DecodeString(expectedHex)
	if err != nil {
		return fmt.Errorf(
			"invalid hex in checksum file %s: %w",
			checksumPath, err,
		)
	}
	if len(decoded) != 4 {
		return fmt.Errorf(
			"checksum file %s has %d hex chars, expected 8 (CRC32)",
			checksumPath, len(expectedHex),
		)
	}

	// Compute actual CRC32
	f, err := os.Open(lstatePath)
	if err != nil {
		return fmt.Errorf(
			"opening lstate for checksum: %w",
			err,
		)
	}
	defer f.Close()

	h := crc32.NewIEEE()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("computing CRC32: %w", err)
	}

	actualHex := fmt.Sprintf("%08x", h.Sum32())
	if !strings.EqualFold(actualHex, expectedHex) {
		return fmt.Errorf(
			"lstate CRC32 mismatch: expected %s, got %s",
			expectedHex,
			actualHex,
		)
	}

	return nil
}
