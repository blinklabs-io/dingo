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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/go-scls"
	sclscardano "github.com/blinklabs-io/go-scls/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// SCLSParityComment is the manifest comment of an export of the UTxO set. It
// declares the era and the namespaces the file covers and that the file is a
// parity export: it is not a bootstrap artifact and nothing may restart a node
// from it.
const SCLSParityComment = "profile=parity era=conway namespaces=utxo/v0 bootstrap=false"

const sclsPartialSuffix = ".partial"

// SCLSSource is the ledger state an SCLS export reads. Every method answers
// for one point of the ledger.
type SCLSSource interface {
	// Tip returns the point the state equals; ok is false for an empty set.
	Tip() (point ocommon.Point, ok bool, err error)
	// Era returns the era index of the state's tip.
	Era() (int, error)
	// EachUtxo calls fn for every live output in ascending (transaction ID,
	// output index) order, with the output's ledger CBOR, until fn fails.
	// The slices are valid only during the call.
	EachUtxo(fn func(txID []byte, index uint32, cbor []byte) error) error
}

// SCLSSnapshotSource serves a live UTxO set to an export through one read
// transaction, so every answer is of the same state whatever the set is doing.
type SCLSSnapshotSource struct {
	snap *badgerutxo.Store
}

var _ SCLSSource = (*SCLSSnapshotSource)(nil)

// NewSCLSSource opens a snapshot of store for an export. It refuses, with
// badgerutxo.ErrNotCheckpointable or badgerutxo.ErrBootstrapIncomplete, a set
// that carries a failure mark, a pending merge or rollback, or an unfinished
// bootstrap. The caller releases the snapshot with Close.
func NewSCLSSource(store *badgerutxo.Store) (*SCLSSnapshotSource, error) {
	snap, err := store.CheckpointableSnapshot()
	if err != nil {
		return nil, fmt.Errorf("snapshot the UTxO set: %w", err)
	}
	return &SCLSSnapshotSource{snap: snap}, nil
}

// Close releases the snapshot.
func (s *SCLSSnapshotSource) Close() error { return s.snap.Close() }

// Tip implements SCLSSource.
func (s *SCLSSnapshotSource) Tip() (ocommon.Point, bool, error) {
	return s.snap.Tip()
}

// Era implements SCLSSource from the era of the newest epoch row.
func (s *SCLSSnapshotSource) Era() (int, error) {
	rows, err := s.snap.Epochs()
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, errors.New(
			"the UTxO set holds no epoch rows, so its era is unknown",
		)
	}
	newest := rows[len(rows)-1]
	return int(newest.Era), nil //nolint:gosec // era indexes are small
}

// EachUtxo implements SCLSSource.
func (s *SCLSSnapshotSource) EachUtxo(
	fn func(txID []byte, index uint32, cbor []byte) error,
) error {
	return s.snap.Each(func(o badgerutxo.Output) error {
		txID, index := splitRef(o.Ref)
		return fn(txID, index, o.Cbor)
	})
}

func splitRef(r badgerutxo.Ref) ([]byte, uint32) {
	const hashLen = 32
	return r[:hashLen], binary.BigEndian.Uint32(r[hashLen:])
}

// SCLSExportConfig configures ExportSCLS.
type SCLSExportConfig struct {
	// Source is the state to export.
	Source SCLSSource
	// Output is the file to create. It must not exist.
	Output string
	// Tool names the exporting program and its version.
	Tool string
	// Now supplies the creation time; nil selects time.Now.
	Now func() time.Time

	// verify reads the finished file back and returns its global root. Only
	// tests set it; nil verifies the whole file with scls.Verify.
	verify func(io.Reader) (scls.Hash, error)
}

// SCLSExportResult describes a published export.
type SCLSExportResult struct {
	Era        int
	Slot       uint64
	BlockHash  []byte
	Namespaces []string
	Entries    uint64
	Root       scls.Hash
}

// ExportSCLS writes the UTxO set of cfg.Source as an SCLS file (CIP-0165) at
// cfg.Output. Outputs stream in key order, one at a time, so memory does not
// grow with the set. The file is staged beside the destination, finalized,
// verified in full against the root the writer computed, and only then
// hard-linked to cfg.Output, which never replaces a file that appeared there
// meanwhile; a failure or a cancelled ctx removes the staged file. Only a
// Conway state is exported.
func ExportSCLS(
	ctx context.Context,
	cfg SCLSExportConfig,
) (res SCLSExportResult, err error) {
	if cfg.Source == nil || cfg.Output == "" {
		return res, errors.New(
			"scls export: a source and an output are required",
		)
	}
	if _, err := os.Lstat(cfg.Output); err == nil {
		return res, fmt.Errorf("scls export: %w: %s", os.ErrExist, cfg.Output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return res, fmt.Errorf("scls export: inspect output: %w", err)
	}
	tip, ok, err := cfg.Source.Tip()
	if err != nil {
		return res, fmt.Errorf("scls export: read tip: %w", err)
	}
	if !ok {
		return res, errors.New("scls export: the UTxO set has no tip")
	}
	era, err := cfg.Source.Era()
	if err != nil {
		return res, fmt.Errorf("scls export: read era: %w", err)
	}
	if era != EraConway {
		return res, fmt.Errorf(
			"scls export: the tip is in the %s era, and only a Conway state is exported",
			EraName(era),
		)
	}

	partial := cfg.Output + sclsPartialSuffix
	// The file is public ledger state, so it takes the umask like other command
	// output rather than the owner-only mode of a node's private checkpoints.
	f, err := os.OpenFile( // #nosec G302,G304 -- public output at an operator-chosen path
		partial,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o666,
	)
	if errors.Is(err, fs.ErrExist) {
		return res, fmt.Errorf(
			"scls export: staged file %s %w: an interrupted export left it, "+
				"or another export is writing it; remove it once no export runs",
			partial, os.ErrExist,
		)
	}
	if err != nil {
		return res, fmt.Errorf("scls export: create staged file: %w", err)
	}
	staged := true
	defer func() {
		if staged {
			_ = f.Close()
			_ = os.Remove(partial)
		}
	}()

	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	w, err := scls.NewWriter(f, scls.WithSummary(scls.ManifestSummary{
		CreatedAt: now().UTC().Format(time.RFC3339),
		Tool:      cfg.Tool,
		Comment:   SCLSParityComment,
	}))
	if err != nil {
		return res, err
	}
	var entries uint64
	err = cfg.Source.EachUtxo(func(txID []byte, index uint32, c []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries++
		return addSCLSUtxo(w, txID, index, c)
	})
	if err != nil {
		return res, fmt.Errorf("scls export: write outputs: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	root, err := w.Close(tip.Slot)
	if err != nil {
		return res, fmt.Errorf("scls export: finalize: %w", err)
	}
	if err := f.Sync(); err != nil {
		return res, fmt.Errorf("scls export: sync staged file: %w", err)
	}
	if err := f.Close(); err != nil {
		return res, fmt.Errorf("scls export: close staged file: %w", err)
	}
	if err := verifySCLS(ctx, partial, root, cfg.verify); err != nil {
		return res, err
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	// A hard link publishes without replacing a file created at the output
	// while the export ran, which a rename would silently clobber.
	if err := os.Link(partial, cfg.Output); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return res, fmt.Errorf(
				"scls export: %w: %s",
				os.ErrExist,
				cfg.Output,
			)
		}
		return res, fmt.Errorf("scls export: publish: %w", err)
	}
	staged = false
	if err := os.Remove(partial); err != nil {
		return res, fmt.Errorf(
			"scls export: %s is published, but removing the staged file failed: %w",
			cfg.Output,
			err,
		)
	}
	if d, err := os.Open(filepath.Dir(cfg.Output)); err == nil { // #nosec G304 -- the output directory
		// Directories cannot be synced on every platform; a failure only
		// narrows the crash window.
		_ = d.Sync()
		_ = d.Close()
	}
	return SCLSExportResult{
		Era:        era,
		Slot:       tip.Slot,
		BlockHash:  tip.Hash,
		Namespaces: []string{sclscardano.UTxOV0Name},
		Entries:    entries,
		Root:       root,
	}, nil
}

// addSCLSUtxo adds one output through the typed utxo/v0 payload codec. The
// stored tx_out is decoded, re-encoded in the ledger's own form (ledgerTxOut),
// and that form is wrapped as the namespace's byte-string payload, decoded and
// encoded back, so the codec rejects what it cannot represent.
func addSCLSUtxo(w *scls.Writer, txID []byte, index uint32, c []byte) error {
	if index > math.MaxUint16 {
		return fmt.Errorf(
			"output index %d of %x does not fit the 16-bit key of %s",
			index, txID, sclscardano.UTxOV0Name,
		)
	}
	stored, err := decodeSCLSTxOut(c)
	if err != nil {
		return fmt.Errorf("output %x#%d: %w", txID, index, err)
	}
	txOut, err := decodeSCLSTxOut(ledgerTxOut(stored))
	if err != nil {
		return fmt.Errorf("output %x#%d: ledger form: %w", txID, index, err)
	}
	payload, err := sclscardano.EncodeTxOut(txOut)
	if err != nil {
		return fmt.Errorf("output %x#%d: %w", txID, index, err)
	}
	key, err := sclscardano.UTxOKeyCodec{}.EncodeKey(sclscardano.UTxOKey{
		TransactionID: [32]byte(txID),
		OutputIndex:   uint16(index),
	})
	if err != nil {
		return err
	}
	return w.AddEntry(sclscardano.UTxOV0Name, key, payload)
}

// verifySCLS verifies the staged file in full and requires its root to be the
// one the writer reported.
func verifySCLS(
	ctx context.Context,
	path string,
	want scls.Hash,
	verify func(io.Reader) (scls.Hash, error),
) error {
	f, err := os.Open(path) // #nosec G304 -- staged by this export
	if err != nil {
		return fmt.Errorf("scls export: open staged file: %w", err)
	}
	defer f.Close()
	if verify == nil {
		verify = func(r io.Reader) (scls.Hash, error) {
			vr, err := scls.Verify(r, scls.VerifyFull)
			if err != nil {
				return scls.Hash{}, err
			}
			return vr.RootHash, nil
		}
	}
	// scls.Verify reads to EOF without a context, so a cancelled export would
	// otherwise scan the whole staged file before its cleanup runs.
	got, err := verify(ctxReader{ctx: ctx, r: f})
	if err != nil {
		return fmt.Errorf("scls export: verify staged file: %w", err)
	}
	if got != want {
		return fmt.Errorf(
			"scls export: verified root %x differs from the written root %x",
			got, want,
		)
	}
	return nil
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// decodeSCLSTxOut decodes a tx_out with the utxo/v0 payload codec.
func decodeSCLSTxOut(c []byte) (sclscardano.TxOut, error) {
	wrapped, err := cbor.Encode(c)
	if err != nil {
		return sclscardano.TxOut{}, fmt.Errorf("wrap tx_out: %w", err)
	}
	return sclscardano.DecodeTxOut(wrapped)
}
