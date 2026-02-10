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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
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
func FindLedgerStateFile(extractedDir string) (string, error) {
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
		"no ledger state files found in %s",
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
// format. Returns the path to tables/tvar if it exists, or empty
// string if not found (legacy format).
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
		tvarPath := filepath.Join(
			ledgerDir, e.Name(), "tables", "tvar",
		)
		if _, err := os.Stat(tvarPath); err == nil {
			dirs = append(dirs, e.Name())
		}
	}

	dirs = sortNumericDesc(dirs)
	if len(dirs) == 0 {
		return ""
	}

	return filepath.Join(
		ledgerDir, dirs[0], "tables", "tvar",
	)
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
	if _, err := cbor.Decode(outer[0], &version); err == nil {
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

	result.EraBounds = eraBounds
	result.EraBoundsWarning = boundsWarning

	// Extract epoch nonce from the HeaderState (outer[1]).
	// HeaderState = [WithOrigin AnnTip, ChainDepState telescope]
	epochNonce, nonceErr := parseEpochNonce(outer[1])
	if nonceErr != nil {
		slog.Debug(
			"epoch nonce extraction failed (non-fatal)",
			"error", nonceErr,
		)
	} else if epochNonce != nil {
		result.EpochNonce = epochNonce
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

	result := &RawLedgerState{
		EraIndex:      eraIndex,
		Epoch:         epoch,
		Tip:           tip,
		Treasury:      treasury,
		Reserves:      reserves,
		EraBoundSlot:  eraBoundSlot,
		EraBoundEpoch: eraBoundEpoch,
		UTxOData:      utxoState[0], // The UTxO map
		CertStateData: ls[0],        // [VState, PState, DState]
		SnapShotsData: es[2],        // mark/set/go
	}

	// GovState (index 3 in UTxOState)
	if len(utxoState) > 3 {
		result.GovStateData = utxoState[3]

		// Extract current protocol parameters from GovState.
		// Conway: [proposals, committee, constitution,
		//          cur_pparams, prev_pparams, ...]
		// Shelley-Babbage: [cur_proposals, future_proposals,
		//                   cur_pparams, prev_pparams, ...]
		govFields, govErr := decodeRawArray(utxoState[3])
		if govErr == nil {
			pparamsIdx := 2 // Shelley through Babbage
			if eraIndex >= EraConway {
				pparamsIdx = 3
			}
			if len(govFields) > pparamsIdx {
				result.PParamsData = govFields[pparamsIdx]
			}
		}
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

// parseEpochNonce extracts the epoch nonce from the HeaderState CBOR.
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
func parseEpochNonce(headerStateData []byte) ([]byte, error) {
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

	// Peel version wrapper(s). Cap iterations to guard
	// against pathological nesting in malformed input.
	const maxPraosVersionLayers = 5
	for versionDepth := 0; len(praosState) >= 2 &&
		versionDepth < maxPraosVersionLayers; versionDepth++ {
		var ver uint64
		if _, decErr := cbor.Decode(
			praosState[0], &ver,
		); decErr != nil {
			break // First element is not an integer
		}
		if len(praosState) == 2 {
			// Wrapped: [version, [inner...]]
			inner, innerErr := decodeRawArray(praosState[1])
			if innerErr != nil {
				return nil, fmt.Errorf(
					"decoding PraosState "+
						"inner (v%d): %w",
					ver,
					innerErr,
				)
			}
			praosState = inner
			continue
		}
		// Flat: [version, lastSlot, ocertCounters, nonces...]
		praosState = praosState[1:]
		break
	}

	// PraosState = [lastSlot, ocertCounters, evolvingNonce,
	//               candidateNonce, epochNonce, labNonce,
	//               lastEpochBlockNonce]
	if len(praosState) < 5 {
		return nil, fmt.Errorf(
			"PraosState has %d elements, expected at least 5",
			len(praosState),
		)
	}

	return decodeNonce(praosState[4])
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
// Each SnapShot = [Stake, Delegations, PoolParams]
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
// SnapShot = [Stake, Delegations, PoolParams]
func parseSnapShot(
	data []byte,
) (*ParsedSnapShot, error) {
	snap, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf("decoding SnapShot: %w", err)
	}
	if len(snap) < 3 {
		return nil, fmt.Errorf(
			"SnapShot has %d elements, expected 3",
			len(snap),
		)
	}

	// Parse Stake: map[Credential]Coin
	// Warnings from these parsers indicate skipped entries,
	// not fatal errors, so we collect them.
	var warnings []error
	stake, err := parseStakeMap(snap[0])
	if err != nil {
		if stake == nil {
			return nil, fmt.Errorf(
				"parsing stake map: %w", err,
			)
		}
		warnings = append(warnings, err)
	}

	// Parse Delegations: map[Credential]PoolKeyHash
	delegations, err := parseDelegationMap(snap[1])
	if err != nil {
		if delegations == nil {
			return nil, fmt.Errorf(
				"parsing delegation map: %w", err,
			)
		}
		warnings = append(warnings, err)
	}

	// Parse PoolParams: map[PoolKeyHash]PoolParams
	poolParams, err := parsePoolParamsMap(snap[2])
	if err != nil {
		if poolParams == nil {
			return nil, fmt.Errorf(
				"parsing pool params map: %w", err,
			)
		}
		warnings = append(warnings, err)
	}

	return &ParsedSnapShot{
		Stake:       stake,
		Delegations: delegations,
		PoolParams:  poolParams,
	}, errors.Join(warnings...)
}

// parseStakeMap decodes a credential -> coin map. Handles both
// definite and indefinite-length maps. Returns a warning if any
// entries were skipped due to decode errors.
func parseStakeMap(
	data cbor.RawMessage,
) (map[string]uint64, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding stake map: %w", err,
		)
	}

	result := make(map[string]uint64, len(entries))
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
		result[hex.EncodeToString(cred.Hash)] = amount
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"stake map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return result, warning
}

// parseDelegationMap decodes a credential -> pool key hash map.
// Handles both definite and indefinite-length maps. Returns a
// warning if any entries were skipped due to decode errors.
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

		pool, err := parsePoolParams(
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

		snapshots = append(snapshots, &models.PoolStakeSnapshot{
			Epoch:          epoch,
			SnapshotType:   snapshotType,
			PoolKeyHash:    poolKeyHash,
			TotalStake:     types.Uint64(agg.totalStake),
			DelegatorCount: agg.delegatorCount,
			CapturedSlot:   capturedSlot,
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
