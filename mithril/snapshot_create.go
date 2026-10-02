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

package mithril

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/klauspost/compress/zstd"
)

// Object names inside one snapshot's store prefix. The immutable archives are
// named by file number (see immutableArchiveName).
const (
	artifactMetadataName = "artifact.json"
	digestsArchiveName   = "digests.tar.zst"
	ancillaryArchiveName = "ancillary.tar.zst"
	digestsJSONName      = "digests.json"
	immutableDirName     = "immutable"
	compressionZstd      = "zstandard"
)

func immutableArchiveName(num uint64) string {
	return fmt.Sprintf("%05d.tar.zst", num)
}

// CreateSnapshotConfig describes one snapshot production run.
type CreateSnapshotConfig struct {
	// Network is the Cardano network name recorded in the artifact.
	Network string
	// DBDir is a cardano-node style database directory holding a sealed
	// immutable/ directory and the ledger/ snapshots taken at its tip.
	DBDir string
	// AncillarySigningKey signs the ancillary manifest. Clients verify it
	// with the matching ancillary verification key.
	AncillarySigningKey ed25519.PrivateKey
	// CardanoNodeVersion is recorded verbatim in the artifact.
	CardanoNodeVersion string
	// CreatedAt is recorded in the artifact; the zero value means now.
	// Everything else in the output is a function of DBDir alone.
	CreatedAt time.Time
	// Store receives the produced objects.
	Store ArtifactStore
	// Logger receives progress; nil discards it.
	Logger *slog.Logger
}

// CreateSnapshot produces a Mithril Cardano database (v2) artifact from
// cfg.DBDir into cfg.Store: one zstd tar archive per immutable file trio, the
// digest list, an ancillary archive holding the newest ledger state with an
// Ed25519-signed manifest, and the artifact metadata. The metadata object is
// written last, so a snapshot is listed only once it is complete.
//
// Output is deterministic: archive entries are sorted, carry no timestamps or
// owners, and compression is single-threaded, so two runs over the same
// directory produce identical bytes and the same artifact hash.
func CreateSnapshot(
	ctx context.Context,
	cfg CreateSnapshotConfig,
) (*CardanoDatabaseSnapshot, error) {
	if cfg.Network == "" {
		return nil, errors.New("snapshot network is required")
	}
	if len(cfg.AncillarySigningKey) != ed25519.PrivateKeySize {
		return nil, errors.New("ancillary signing key is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("artifact store is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	root, err := os.OpenRoot(cfg.DBDir)
	if err != nil {
		return nil, fmt.Errorf("opening database directory: %w", err)
	}
	defer root.Close()

	trios, err := countImmutableTrios(root)
	if err != nil {
		return nil, err
	}
	// The ledger state is read first: it fixes the beacon epoch, and a
	// directory without one is rejected before any archive is uploaded.
	ancillary, epoch, err := readAncillary(root)
	if err != nil {
		return nil, err
	}

	lastNum := uint64(trios - 1) // #nosec G115 -- len is non-negative
	digests, digestByName, immutableBytes, err := digestImmutables(
		root, trios,
	)
	if err != nil {
		return nil, err
	}
	leaves, err := digestMerkleLeaves(digests, lastNum)
	if err != nil {
		return nil, fmt.Errorf("building digest merkle leaves: %w", err)
	}
	rootHash, err := computeMMRRoot(leaves)
	if err != nil {
		return nil, fmt.Errorf("computing digest merkle root: %w", err)
	}
	digestJSON, err := json.Marshal(digests)
	if err != nil {
		return nil, fmt.Errorf("encoding digest list: %w", err)
	}
	ancillaryEntries, ancillaryDigests, err := ancillary.entries(
		cfg.AncillarySigningKey,
	)
	if err != nil {
		return nil, err
	}

	createdAt := cfg.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	artifact := &CardanoDatabaseSnapshot{
		MerkleRoot: hex.EncodeToString(rootHash),
		Network:    cfg.Network,
		Beacon: Beacon{
			Epoch:               epoch,
			ImmutableFileNumber: lastNum,
		},
		TotalDbSizeUncompressed: immutableBytes + ancillary.size,
		Digests: CardanoDatabaseDigests{
			SizeUncompressed: int64(len(digestJSON)),
		},
		Immutables: CardanoDatabaseImmutables{
			AverageSizeUncompressed: immutableBytes / int64(trios),
		},
		Ancillary: CardanoDatabaseAncillary{
			SizeUncompressed: ancillary.size,
		},
		CardanoNodeVersion: cfg.CardanoNodeVersion,
		CreatedAt:          createdAt.UTC().Format(time.RFC3339Nano),
	}
	artifact.Hash = artifact.ComputeHash()

	for num := range lastNum + 1 {
		if err := putImmutableArchive(
			ctx, cfg.Store, root, artifact.Hash, num, digestByName,
		); err != nil {
			return nil, err
		}
		if num%1000 == 0 {
			logger.Info(
				"immutable archive written",
				"component", "mithril",
				"immutable_file_number", num,
				"immutable_files", trios,
			)
		}
	}
	if err := putTarZst(
		ctx, cfg.Store, path.Join(artifact.Hash, digestsArchiveName),
		[]tarEntry{bytesEntry(digestsJSONName, digestJSON)},
	); err != nil {
		return nil, fmt.Errorf("writing digests archive: %w", err)
	}
	if err := putTarZst(
		ctx, cfg.Store, path.Join(artifact.Hash, ancillaryArchiveName),
		ancillaryEntries,
	); err != nil {
		return nil, fmt.Errorf("writing ancillary archive: %w", err)
	}
	if err := checkArchived(ancillaryEntries, ancillaryDigests); err != nil {
		return nil, err
	}
	// Written last: listing treats the metadata as the completion marker.
	metadata, err := json.Marshal(artifact)
	if err != nil {
		return nil, fmt.Errorf("encoding artifact metadata: %w", err)
	}
	if err := cfg.Store.Put(
		ctx, path.Join(artifact.Hash, artifactMetadataName),
		strings.NewReader(string(metadata)),
	); err != nil {
		return nil, fmt.Errorf("writing artifact metadata: %w", err)
	}
	logger.Info(
		"snapshot created",
		"component", "mithril",
		"hash", artifact.Hash,
		"epoch", epoch,
		"immutable_file_number", lastNum,
	)
	return artifact, nil
}

// digestImmutables returns the SHA-256 digest of every immutable file, in the
// shape of the certified digest list, and the files' total size.
func digestImmutables(
	root *os.Root,
	count int,
) ([]CardanoDatabaseDigestEntry, map[string]string, int64, error) {
	immutable, err := root.OpenRoot(immutableDirName)
	if err != nil {
		return nil, nil, 0, fmt.Errorf(
			"opening immutable directory: %w", err,
		)
	}
	defer immutable.Close()
	entries := make([]CardanoDatabaseDigestEntry, 0, 3*count)
	byName := make(map[string]string, 3*count)
	var total int64
	for num := range count {
		for _, ext := range immutableFileExtensions {
			name := fmt.Sprintf("%05d.%s", num, ext)
			sum, size, err := sha256FileInRoot(immutable, name)
			if err != nil {
				return nil, nil, 0, fmt.Errorf(
					"digesting %s: %w", name, err,
				)
			}
			byName[name] = sum
			entries = append(entries, CardanoDatabaseDigestEntry{
				ImmutableFileName: name,
				Digest:            sum,
			})
			total += size
		}
	}
	return entries, byName, total, nil
}

// countImmutableTrios returns how many immutable file numbers the immutable
// directory holds, requiring numbers 0..N each to have their chunk, primary
// and secondary file.
func countImmutableTrios(root *os.Root) (int, error) {
	entries, err := fs.ReadDir(root.FS(), immutableDirName)
	if err != nil {
		return 0, fmt.Errorf("reading immutable directory: %w", err)
	}
	byNum := map[uint64]map[string]bool{}
	for _, e := range entries {
		num, ok := immutableFileNumberFromName(e.Name())
		if !ok || e.IsDir() {
			continue
		}
		ext := strings.TrimPrefix(path.Ext(e.Name()), ".")
		if !slices.Contains(immutableFileExtensions, ext) {
			continue
		}
		if byNum[num] == nil {
			byNum[num] = map[string]bool{}
		}
		byNum[num][ext] = true
	}
	if len(byNum) == 0 {
		return 0, errors.New("immutable directory holds no immutable files")
	}
	for num := range uint64(len(byNum)) { // #nosec G115 -- len is non-negative
		for _, ext := range immutableFileExtensions {
			if !byNum[num][ext] {
				return 0, fmt.Errorf(
					"immutable file %05d.%s is missing: numbers must be "+
						"contiguous from 0", num, ext,
				)
			}
		}
	}
	return len(byNum), nil
}

// tarEntry is one file of a produced archive. When sum is set it receives the
// file's bytes as they are archived.
type tarEntry struct {
	name string
	size int64
	open func() (io.ReadCloser, error)
	sum  hash.Hash
}

func bytesEntry(name string, data []byte) tarEntry {
	return tarEntry{
		name: name,
		size: int64(len(data)),
		open: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(data)), nil
		},
	}
}

// putTarZst streams entries, in the order given, as a zstd-compressed tar to
// store key.
func putTarZst(
	ctx context.Context,
	store ArtifactStore,
	key string,
	entries []tarEntry,
) error {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(writeTarZst(pw, entries))
	}()
	err := store.Put(ctx, key, pr)
	// Unblocks the writer when Put stopped reading early.
	pr.CloseWithError(err)
	return err
}

func writeTarZst(w io.Writer, entries []tarEntry) error {
	// A single encoder goroutine keeps block boundaries, and so the output
	// bytes, independent of the host's CPU count.
	zw, err := zstd.NewWriter(w, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	tw := tar.NewWriter(zw)
	for _, entry := range entries {
		if err := writeTarEntry(tw, entry); err != nil {
			_ = zw.Close()
			return err
		}
	}
	if err := tw.Close(); err != nil {
		_ = zw.Close()
		return err
	}
	return zw.Close()
}

func writeTarEntry(tw *tar.Writer, entry tarEntry) error {
	f, err := entry.open()
	if err != nil {
		return fmt.Errorf("opening %s: %w", entry.name, err)
	}
	defer f.Close()
	if err := tw.WriteHeader(&tar.Header{
		Name:     entry.name,
		Typeflag: tar.TypeReg,
		Mode:     0o644,
		Size:     entry.size,
		ModTime:  time.Unix(0, 0),
		Format:   tar.FormatUSTAR,
	}); err != nil {
		return err
	}
	var dst io.Writer = tw
	if entry.sum != nil {
		dst = io.MultiWriter(tw, entry.sum)
	}
	n, err := io.Copy(dst, f)
	if err != nil {
		return fmt.Errorf("archiving %s: %w", entry.name, err)
	}
	if n != entry.size {
		return fmt.Errorf(
			"%s changed while archiving: expected %d bytes, read %d",
			entry.name, entry.size, n,
		)
	}
	return nil
}

func rootEntry(root *os.Root, rel string) (tarEntry, error) {
	info, err := root.Stat(filepath.FromSlash(rel))
	if err != nil {
		return tarEntry{}, err
	}
	if !info.Mode().IsRegular() {
		return tarEntry{}, fmt.Errorf("%s is not a regular file", rel)
	}
	return tarEntry{
		name: rel,
		size: info.Size(),
		open: func() (io.ReadCloser, error) {
			return root.Open(filepath.FromSlash(rel))
		},
		sum: sha256.New(),
	}, nil
}

// putImmutableArchive archives the three files of immutable number num under
// prefix and checks the bytes archived against the digests already taken, so a
// file rewritten between the two passes fails the run instead of publishing an
// archive its own digest list does not describe.
func putImmutableArchive(
	ctx context.Context,
	store ArtifactStore,
	root *os.Root,
	prefix string,
	num uint64,
	digests map[string]string,
) error {
	entries := make([]tarEntry, 0, len(immutableFileExtensions))
	for _, ext := range immutableFileExtensions {
		rel := immutableDirName + "/" + fmt.Sprintf("%05d.%s", num, ext)
		entry, err := rootEntry(root, rel)
		if err != nil {
			return fmt.Errorf("reading %s: %w", rel, err)
		}
		entries = append(entries, entry)
	}
	if err := putTarZst(
		ctx, store, path.Join(prefix, immutableArchiveName(num)), entries,
	); err != nil {
		return fmt.Errorf("writing immutable archive %05d: %w", num, err)
	}
	return checkArchived(entries, digests)
}

// checkArchived reports a file whose archived bytes differ from the digest
// taken before archiving. Entries are keyed in want by base name for
// immutable files and by full path for ledger files, so both spellings are
// tried.
func checkArchived(entries []tarEntry, want map[string]string) error {
	for _, entry := range entries {
		if entry.sum == nil {
			continue
		}
		digest, ok := want[entry.name]
		if !ok {
			digest, ok = want[path.Base(entry.name)]
		}
		if !ok || hex.EncodeToString(entry.sum.Sum(nil)) != digest {
			return fmt.Errorf("%s changed while archiving", entry.name)
		}
	}
	return nil
}

// ancillaryState is the ledger state selected for the ancillary archive.
type ancillaryState struct {
	files []tarEntry
	size  int64
}

// readAncillary selects the newest ledger state under root and returns it with
// the epoch it was taken in. Only the state file and the UTxO table the
// importer reads are archived.
func readAncillary(root *os.Root) (*ancillaryState, uint64, error) {
	files, _, err := ledgerstate.OpenNewestSnapshot(root)
	if err != nil {
		return nil, 0, fmt.Errorf("selecting ledger state: %w", err)
	}
	defer files.Close()
	state, err := ledgerstate.ParseSnapshotFile(files.State)
	if err != nil {
		return nil, 0, fmt.Errorf("parsing ledger state: %w", err)
	}
	paths := []string{files.StatePath}
	if files.Table != nil {
		paths = append(paths, files.TablePath)
	}
	slices.Sort(paths)
	out := &ancillaryState{}
	for _, rel := range paths {
		entry, err := rootEntry(root, rel)
		if err != nil {
			return nil, 0, fmt.Errorf("reading ledger file %s: %w", rel, err)
		}
		out.files = append(out.files, entry)
		out.size += entry.size
	}
	return out, state.Epoch, nil
}

// entries returns the ancillary archive entries: the ledger files followed by
// the manifest signed over their digests. The files are hashed here, in a
// separate pass, because the manifest has to precede the archive upload no
// differently from any other file's digest.
func (a *ancillaryState) entries(
	key ed25519.PrivateKey,
) ([]tarEntry, map[string]string, error) {
	manifest := ancillaryManifest{Data: map[string]string{}}
	for _, entry := range a.files {
		f, err := entry.open()
		if err != nil {
			return nil, nil, err
		}
		sum, _, err := sha256Reader(f, entry.name)
		_ = f.Close()
		if err != nil {
			return nil, nil, err
		}
		manifest.Data[entry.name] = sum
	}
	manifest.Signature = hex.EncodeToString(
		ed25519.Sign(key, manifest.computeHash()),
	)
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding ancillary manifest: %w", err)
	}
	return append(
		slices.Clone(a.files),
		bytesEntry(ancillaryManifestFilename, data),
	), manifest.Data, nil
}

// PruneSnapshots deletes all but the keep newest complete snapshots from
// store and returns the hashes it removed. A keep below one retains
// everything, and a snapshot still being produced is never counted or
// removed, since it has no metadata yet.
func PruneSnapshots(
	ctx context.Context,
	store ArtifactStore,
	keep int,
) ([]string, error) {
	if keep < 1 {
		return nil, nil
	}
	snapshots, err := ListSnapshots(ctx, store)
	if err != nil {
		return nil, err
	}
	if len(snapshots) <= keep {
		return nil, nil
	}
	var removed []string
	for _, snapshot := range snapshots[keep:] {
		// The metadata goes first so a removal interrupted part way leaves
		// an unlisted remainder rather than a listed snapshot with missing
		// archives.
		for _, key := range []string{
			path.Join(snapshot.Hash, artifactMetadataName),
			snapshot.Hash,
		} {
			if err := store.DeletePrefix(ctx, key); err != nil {
				return removed, fmt.Errorf(
					"removing snapshot %s: %w", snapshot.Hash, err,
				)
			}
		}
		removed = append(removed, snapshot.Hash)
	}
	return removed, nil
}

// ParseSigningKey parses an Ed25519 signing key in the Mithril
// JSON-hex format (or raw hex): a 32-byte seed or a 64-byte private key.
func ParseSigningKey(data string) (ed25519.PrivateKey, error) {
	key, err := ParseVerificationKey(data)
	if err != nil {
		return nil, fmt.Errorf("parsing signing key: %w", err)
	}
	switch len(key.RawKeyBytes) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(key.RawKeyBytes), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(key.RawKeyBytes), nil
	default:
		return nil, fmt.Errorf(
			"signing key has unexpected size %d",
			len(key.RawKeyBytes),
		)
	}
}
