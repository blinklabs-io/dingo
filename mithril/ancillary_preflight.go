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
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/klauspost/compress/zstd"
)

func readVerifiedAncillaryManifest(
	ctx context.Context,
	file *os.File,
	verificationKey string,
	decoders *decoderMemoryBudget,
) (*ancillaryManifest, []byte, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("seeking ancillary archive: %w", err)
	}
	countingFile := &countingReader{reader: file}
	release, err := decoders.reserve(ctx, maxZstdWindowSize, maxZstdDecoderMemory)
	if err != nil {
		return nil, nil, err
	}
	defer release()
	zr, err := zstd.NewReader(
		countingFile,
		zstd.WithDecoderMaxWindow(maxZstdWindowSize),
		zstd.WithDecoderMaxMemory(maxZstdDecoderMemory),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating ancillary preflight reader: %w", err)
	}
	defer zr.Close()
	tr := tar.NewReader(&archiveStreamReader{
		ctx: ctx,
		reader: &expansionReader{
			reader: zr,
			read:   &countingFile.read,
			ratio:  maxExpansionRatio,
			floor:  expansionFloor,
		},
		maxBytes: archiveStreamLimit(ancillaryExpandedMaxBytes, maxAncillaryEntries),
	})
	var entries int
	var expanded int64
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil, nil, errors.New("ancillary archive has no signed manifest")
		}
		if err != nil {
			return nil, nil, fmt.Errorf("reading ancillary archive: %w", err)
		}
		entries++
		if entries > maxAncillaryEntries {
			return nil, nil, fmt.Errorf(
				"%w: ancillary entry limit reached", ErrExtractLimitExceeded,
			)
		}
		name := path.Clean(header.Name)
		if name == ancillaryManifestFilename {
			if header.Typeflag != tar.TypeReg && header.Typeflag != '\x00' {
				return nil, nil, errors.New("ancillary manifest is not a regular file")
			}
			data, err := io.ReadAll(io.LimitReader(tr, maxAncillaryManifestBytes+1))
			if err != nil {
				return nil, nil, fmt.Errorf("reading ancillary manifest: %w", err)
			}
			if len(data) > maxAncillaryManifestBytes {
				return nil, nil, errors.New("ancillary manifest exceeds size limit")
			}
			manifest, err := parseAndVerifyAncillaryManifest(data, verificationKey)
			if err != nil {
				return nil, nil, err
			}
			return manifest, data, nil
		}
		remaining := ancillaryExpandedMaxBytes - expanded
		if remaining <= 0 || header.Size > remaining {
			return nil, nil, fmt.Errorf(
				"%w: ancillary expanded byte limit reached", ErrExtractLimitExceeded,
			)
		}
		written, err := io.Copy(io.Discard, io.LimitReader(tr, remaining+1))
		if err != nil {
			return nil, nil, fmt.Errorf("scanning ancillary archive: %w", err)
		}
		expanded += written
		if expanded > ancillaryExpandedMaxBytes {
			return nil, nil, fmt.Errorf(
				"%w: ancillary expanded byte limit reached", ErrExtractLimitExceeded,
			)
		}
	}
}

func parseAndVerifyAncillaryManifest(
	data []byte,
	verificationKey string,
) (*ancillaryManifest, error) {
	var manifest ancillaryManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parsing ancillary manifest: %w", err)
	}
	if len(manifest.Data) == 0 {
		return nil, errors.New("ancillary manifest lists no files")
	}
	if len(manifest.Data) >= maxAncillaryEntries {
		return nil, errors.New("ancillary manifest lists too many files")
	}
	if manifest.Signature == "" {
		return nil, errors.New("ancillary manifest has no signature")
	}
	for name, digest := range manifest.Data {
		if path.Clean(name) != name || !validRelPath(name) ||
			(!strings.HasPrefix(name, "ledger/") &&
				!ancillaryMemberPattern.MatchString(name)) {
			return nil, fmt.Errorf(
				"%w: ancillary manifest path %q",
				ErrExtractUnexpectedMember, name,
			)
		}
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size {
			return nil, fmt.Errorf(
				"ancillary manifest contains invalid digest for %s", name,
			)
		}
	}
	key, err := ParseVerificationKey(verificationKey)
	if err != nil {
		return nil, fmt.Errorf("parsing ancillary verification key: %w", err)
	}
	if len(key.RawKeyBytes) != ed25519.PublicKeySize {
		return nil, fmt.Errorf(
			"ancillary verification key has unexpected size %d",
			len(key.RawKeyBytes),
		)
	}
	signature, err := decodeHexString(manifest.Signature)
	if err != nil {
		return nil, fmt.Errorf("decoding ancillary manifest signature: %w", err)
	}
	if len(signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf(
			"ancillary manifest signature has unexpected size %d", len(signature),
		)
	}
	if !ed25519.Verify(
		ed25519.PublicKey(key.RawKeyBytes), manifest.computeHash(), signature,
	) {
		return nil, errors.New("ancillary manifest signature is invalid")
	}
	return &manifest, nil
}

func verifiedAncillaryLimits(
	manifest *ancillaryManifest,
	manifestData []byte,
	budget *bootstrapExtractionBudget,
) archiveLimits {
	allowed := map[string]struct{}{ancillaryManifestFilename: {}}
	digests := make(map[string]string, len(manifest.Data)+1)
	for name, digest := range manifest.Data {
		allowed[name] = struct{}{}
		digests[name] = digest
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			allowed[parent] = struct{}{}
		}
	}
	manifestDigest := sha256.Sum256(manifestData)
	digests[ancillaryManifestFilename] = hex.EncodeToString(manifestDigest[:])
	// The extractor counts entries before cleaning their names, so one "./"
	// root entry is admitted on top of the manifest's own names.
	return archiveLimits{
		maxEntries:     len(allowed) + 1,
		maxMemberBytes: maxExtractFileSize,
		maxTotalBytes:  ancillaryExpandedMaxBytes,
		allow: func(name string) bool {
			_, ok := allowed[name]
			return ok
		},
		digests: digests,
		budget:  budget,
	}
}
