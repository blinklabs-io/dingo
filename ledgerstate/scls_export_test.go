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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/go-scls"
	sclscardano "github.com/blinklabs-io/go-scls/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const sclsTestComment = "profile=parity era=conway namespaces=utxo/v0 bootstrap=false"

func sclsTxID(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

// sclsOutput encodes a ledger tx_out: the Babbage map form for even b, the
// Shelley array form for odd b.
func sclsOutput(t *testing.T, b byte, coin uint64) []byte {
	t.Helper()
	addr := append([]byte{0x01}, bytes.Repeat([]byte{b}, 56)...)
	var v any = map[uint]any{0: addr, 1: coin}
	if b%2 == 1 {
		v = []any{addr, coin}
	}
	data, err := cbor.Encode(v)
	require.NoError(t, err)
	return data
}

type sclsFixture struct {
	store *badgerutxo.Store
	tip   ocommon.Point
	outs  map[badgerutxo.Ref][]byte
}

// newSCLSFixture opens a set at slot 5000 in epoch era, holding outputs for
// every (txid byte, index) pair in refs.
func newSCLSFixture(
	t *testing.T,
	era uint64,
	refs ...badgerutxo.Ref,
) *sclsFixture {
	t.Helper()
	store, err := badgerutxo.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	tip := ocommon.Point{Slot: 5000, Hash: sclsTxID(0xEE)}
	outs := make(map[badgerutxo.Ref][]byte, len(refs))
	var added []badgerutxo.Output
	for i, r := range refs {
		c := sclsOutput(t, r[0], uint64(1_000_000+i))
		outs[r] = c
		added = append(added, badgerutxo.Output{Ref: r, Cbor: c})
	}
	require.NoError(t, store.Bootstrap(
		ocommon.Point{Slot: 1, Hash: sclsTxID(0xAA)},
		func(put func(badgerutxo.Output) error) error { return nil },
	))
	_, err = store.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Changes: []badgerutxo.Change{{Point: tip, Added: added}},
		Epochs: []badgerutxo.EpochRow{
			{Epoch: 0, StartSlot: 0, Era: era, Value: []byte{0}},
		},
	})
	require.NoError(t, err)
	return &sclsFixture{store: store, tip: tip, outs: outs}
}

func (f *sclsFixture) source(t *testing.T) *SCLSSnapshotSource {
	t.Helper()
	src, err := NewSCLSSource(f.store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })
	return src
}

func sclsRefs() []badgerutxo.Ref {
	return []badgerutxo.Ref{
		badgerutxo.NewRef(sclsTxID(0x30), 1),
		badgerutxo.NewRef(sclsTxID(0x10), 0),
		badgerutxo.NewRef(sclsTxID(0x10), 256),
		badgerutxo.NewRef(sclsTxID(0x20), 0xFFFF),
		badgerutxo.NewRef(sclsTxID(0x10), 2),
	}
}

func requireNoSCLSFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		t.Errorf("unexpected file left behind: %s", e.Name())
	}
}

// readSCLS reads every entry of path with the go-scls reader.
func readSCLS(t *testing.T, path string) ([]scls.Entry, *scls.Manifest) {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	r, err := scls.NewReader(f)
	require.NoError(t, err)
	var entries []scls.Entry
	for {
		chunk, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.Equal(t, sclscardano.UTxOV0Name, chunk.Namespace)
		entries = append(entries, chunk.Entries...)
	}
	return entries, r.Manifest()
}

func TestExportSCLSWritesTheUTxOSetInKeyOrder(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	out := filepath.Join(t.TempDir(), "state.scls")
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	res, err := ExportSCLS(t.Context(), SCLSExportConfig{
		Source: f.source(t),
		Output: out,
		Tool:   "dingo test",
		Now:    func() time.Time { return created },
	})
	require.NoError(t, err)
	require.Equal(t, EraConway, res.Era)
	require.Equal(t, uint64(5000), res.Slot)
	require.Equal(t, f.tip.Hash, res.BlockHash)
	require.Equal(t, []string{"utxo/v0"}, res.Namespaces)
	require.Equal(t, uint64(len(sclsRefs())), res.Entries)

	entries, manifest := readSCLS(t, out)
	require.Len(t, entries, len(sclsRefs()))
	for i, e := range entries {
		require.Len(t, e.Key, 34)
		if i > 0 {
			require.Positive(t, bytes.Compare(e.Key, entries[i-1].Key))
		}
		var ref badgerutxo.Ref
		copy(ref[:32], e.Key[:32])
		binary.BigEndian.PutUint32(
			ref[32:],
			uint32(binary.BigEndian.Uint16(e.Key[32:])),
		)
		want, ok := f.outs[ref]
		require.True(t, ok, "unexpected key %x", e.Key)
		txOut, err := sclscardano.DecodeTxOut(e.Value)
		require.NoError(t, err)
		reEncoded, err := sclscardano.EncodeTxOut(txOut)
		require.NoError(t, err)
		require.Equal(t, e.Value, reEncoded)
		wrapped, err := cbor.Encode(want)
		require.NoError(t, err)
		require.Equal(t, wrapped, e.Value)
	}
	require.Equal(t, uint64(5000), manifest.SlotNo)
	require.Equal(t, res.Root, manifest.RootHash)
	require.Equal(t, uint64(len(sclsRefs())), manifest.TotalEntries)

	vf, err := os.Open(out)
	require.NoError(t, err)
	defer vf.Close()
	verified, err := scls.Verify(vf, scls.VerifyFull)
	require.NoError(t, err)
	require.Equal(t, res.Root, verified.RootHash)
	_, err = os.Stat(out + ".partial")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestExportSCLSDeclaresParityCoverage(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	out := filepath.Join(t.TempDir(), "state.scls")
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_, err := ExportSCLS(t.Context(), SCLSExportConfig{
		Source: f.source(t),
		Output: out,
		Tool:   "dingo v9.9.9",
		Now:    func() time.Time { return created },
	})
	require.NoError(t, err)
	_, manifest := readSCLS(t, out)
	require.Equal(t, "dingo v9.9.9", manifest.Summary.Tool)
	require.Equal(t, sclsTestComment, manifest.Summary.Comment)
	require.Equal(t, "2026-01-02T03:04:05Z", manifest.Summary.CreatedAt)
	require.Len(t, manifest.Namespaces, 1)
	require.Equal(t, "utxo/v0", manifest.Namespaces[0].Name)
}

func TestExportSCLSRejectsAnOutputIndexBeyondTheKeyWidth(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway,
		badgerutxo.NewRef(sclsTxID(0x10), 0),
		badgerutxo.NewRef(sclsTxID(0x20), 0x10000),
	)
	dir := t.TempDir()
	_, err := ExportSCLS(t.Context(), SCLSExportConfig{
		Source: f.source(t),
		Output: filepath.Join(dir, "state.scls"),
		Tool:   "dingo test",
	})
	require.ErrorContains(t, err, "output index 65536")
	requireNoSCLSFiles(t, dir)
}

func TestExportSCLSRefusesAnExistingOutput(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	dir := t.TempDir()
	out := filepath.Join(dir, "state.scls")
	require.NoError(t, os.WriteFile(out, []byte("keep"), 0o600))
	_, err := ExportSCLS(t.Context(), SCLSExportConfig{
		Source: f.source(t), Output: out, Tool: "dingo test",
	})
	require.ErrorIs(t, err, os.ErrExist)
	got, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), got)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// cancelAfterSource cancels the export once n outputs were read, mid-stream.
type cancelAfterSource struct {
	SCLSSource
	n      int
	cancel context.CancelFunc
}

func (s cancelAfterSource) EachUtxo(
	fn func(txID []byte, index uint32, cbor []byte) error,
) error {
	seen := 0
	return s.SCLSSource.EachUtxo(func(txID []byte, i uint32, c []byte) error {
		if seen == s.n {
			s.cancel()
		}
		seen++
		return fn(txID, i, c)
	})
}

func TestExportSCLSCancellationLeavesNothing(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := ExportSCLS(ctx, SCLSExportConfig{
		Source: cancelAfterSource{f.source(t), 2, cancel},
		Output: filepath.Join(dir, "state.scls"),
		Tool:   "dingo test",
	})
	require.ErrorIs(t, err, context.Canceled)
	requireNoSCLSFiles(t, dir)
}

func TestExportSCLSCancellationStopsVerification(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var read int64
	_, err := ExportSCLS(ctx, SCLSExportConfig{
		Source: f.source(t),
		Output: filepath.Join(dir, "state.scls"),
		Tool:   "dingo test",
		verify: func(r io.Reader) (scls.Hash, error) {
			cancel()
			n, err := io.Copy(io.Discard, r)
			read = n
			return scls.Hash{}, err
		},
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, read, "verification kept reading after cancellation")
	requireNoSCLSFiles(t, dir)
}

func TestExportSCLSLeavesNothingWhenVerificationFails(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	for name, verify := range map[string]func(io.Reader) (scls.Hash, error){
		"error": func(io.Reader) (scls.Hash, error) {
			return scls.Hash{}, errors.New("verify failed")
		},
		"root mismatch": func(io.Reader) (scls.Hash, error) {
			return scls.Hash{1}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			_, err := ExportSCLS(t.Context(), SCLSExportConfig{
				Source: f.source(t),
				Output: filepath.Join(dir, "state.scls"),
				Tool:   "dingo test",
				verify: verify,
			})
			require.Error(t, err)
			requireNoSCLSFiles(t, dir)
		})
	}
}

func TestExportSCLSRefusesANonConwayTip(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraBabbage, sclsRefs()...)
	dir := t.TempDir()
	_, err := ExportSCLS(t.Context(), SCLSExportConfig{
		Source: f.source(t),
		Output: filepath.Join(dir, "state.scls"),
		Tool:   "dingo test",
	})
	require.ErrorContains(t, err, "Conway")
	requireNoSCLSFiles(t, dir)
}

func TestNewSCLSSourceRefusesASetWithAFailureMark(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	require.NoError(t, f.store.MarkFailed(5001))
	_, err := NewSCLSSource(f.store)
	require.ErrorIs(t, err, badgerutxo.ErrNotCheckpointable)
}

func TestExportSCLSFailsOnAPayloadTheCodecRejects(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	// A bare unsigned integer is neither the array nor the map form of a tx_out.
	_, err := f.store.Apply(
		ocommon.Point{Slot: 5001, Hash: sclsTxID(0xEF)},
		[]badgerutxo.Output{{
			Ref:  badgerutxo.NewRef(sclsTxID(0x40), 0),
			Cbor: []byte{0x01},
		}},
		nil,
	)
	require.NoError(t, err)
	dir := t.TempDir()
	_, err = ExportSCLS(t.Context(), SCLSExportConfig{
		Source: f.source(t),
		Output: filepath.Join(dir, "state.scls"),
		Tool:   "dingo test",
	})
	require.ErrorIs(t, err, sclscardano.ErrPayload)
	requireNoSCLSFiles(t, dir)
}

// TestExportSCLSNeverReplacesAnOutputCreatedDuringTheExport requires the
// publish to refuse, not replace, a file that appears at the output after the
// export started.
func TestExportSCLSNeverReplacesAnOutputCreatedDuringTheExport(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	dir := t.TempDir()
	out := filepath.Join(dir, "state.scls")
	_, err := ExportSCLS(t.Context(), SCLSExportConfig{
		Source: f.source(t),
		Output: out,
		Tool:   "dingo test",
		verify: func(r io.Reader) (scls.Hash, error) {
			vr, err := scls.Verify(r, scls.VerifyFull)
			if err != nil {
				return scls.Hash{}, err
			}
			if err := os.WriteFile(out, []byte("keep"), 0o600); err != nil {
				return scls.Hash{}, err
			}
			return vr.RootHash, nil
		},
	})
	require.ErrorIs(t, err, os.ErrExist)
	got, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), got)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// TestExportSCLSNamesALeftoverStagedFile requires a staged file left by an
// interrupted export to be refused, kept, and named in the error with the
// remedy.
func TestExportSCLSNamesALeftoverStagedFile(t *testing.T) {
	t.Parallel()
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	dir := t.TempDir()
	out := filepath.Join(dir, "state.scls")
	require.NoError(t, os.WriteFile(out+".partial", []byte("left"), 0o600))
	_, err := ExportSCLS(t.Context(), SCLSExportConfig{
		Source: f.source(t), Output: out, Tool: "dingo test",
	})
	require.ErrorIs(t, err, os.ErrExist)
	require.ErrorContains(t, err, out+".partial")
	require.ErrorContains(t, err, "remove it")
	got, err := os.ReadFile(out + ".partial")
	require.NoError(t, err)
	require.Equal(t, []byte("left"), got)
	_, err = os.Stat(out)
	require.ErrorIs(t, err, os.ErrNotExist)
}
