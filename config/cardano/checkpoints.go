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

package cardano

import (
	"encoding/json"
	"fmt"
	"strings"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// checkpointEntry is a single block-number -> block-hash checkpoint as
// stored in the CheckpointsFile JSON.
type checkpointEntry struct {
	BlockNo uint64 `json:"blockNo"`
	Hash    string `json:"hash"`
}

// checkpointsFile mirrors the on-disk layout of the checkpoints JSON:
// {"checkpoints": [{"blockNo": N, "hash": "..."}]}
type checkpointsFile struct {
	Checkpoints []checkpointEntry `json:"checkpoints"`
}

// blake2b256Hex returns the lower-case hex blake2b-256 hash of data,
// matching the encoding used for CheckpointsFileHash and block hashes.
func blake2b256Hex(data []byte) string {
	return lcommon.Blake2b256Hash(data).String()
}

func isHexString(value string) bool {
	for _, r := range value {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return false
	}
	return true
}

// parseCheckpoints decodes a checkpoints file into a map keyed by block
// number with lower-case hex block hashes as values. When expectedHash
// is non-empty, the blake2b-256 hash of data is verified against it
// first, failing closed on mismatch. Conflicting duplicate entries for
// the same block number are rejected.
func parseCheckpoints(
	data []byte,
	expectedHash string,
) (map[uint64]string, error) {
	if expectedHash != "" {
		actual := blake2b256Hex(data)
		if !strings.EqualFold(actual, expectedHash) {
			return nil, fmt.Errorf(
				"checkpoints file hash mismatch: expected %s, computed %s",
				expectedHash,
				actual,
			)
		}
	}
	var parsed checkpointsFile
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parse checkpoints: %w", err)
	}
	out := make(map[uint64]string, len(parsed.Checkpoints))
	for _, entry := range parsed.Checkpoints {
		hash := strings.ToLower(strings.TrimSpace(entry.Hash))
		if hash == "" {
			return nil, fmt.Errorf(
				"checkpoint for block %d has empty hash",
				entry.BlockNo,
			)
		}
		if !isHexString(hash) {
			return nil, fmt.Errorf(
				"checkpoint for block %d has non-hex hash %q",
				entry.BlockNo,
				entry.Hash,
			)
		}
		if existing, ok := out[entry.BlockNo]; ok && existing != hash {
			return nil, fmt.Errorf(
				"conflicting checkpoint for block %d: %s vs %s",
				entry.BlockNo,
				existing,
				hash,
			)
		}
		out[entry.BlockNo] = hash
	}
	return out, nil
}

// Checkpoints returns the parsed chain checkpoints keyed by block number
// (height). It is nil when no CheckpointsFile is configured.
func (c *CardanoNodeConfig) Checkpoints() map[uint64]string {
	return c.checkpoints
}

// loadCheckpoints loads and validates the CheckpointsFile from the
// regular filesystem, relative to the config directory when not absolute.
func (c *CardanoNodeConfig) loadCheckpoints() error {
	if c.CheckpointsFile == "" {
		return nil
	}
	data, err := c.loadOptionalConfigFile(c.CheckpointsFile)
	if err != nil {
		return err
	}
	cps, err := parseCheckpoints(
		replaceGenesisLineEndings(data),
		c.CheckpointsFileHash,
	)
	if err != nil {
		return err
	}
	c.checkpoints = cps
	return nil
}

// loadCheckpointsFromEmbed loads and validates the CheckpointsFile from
// the embedded filesystem.
func (c *CardanoNodeConfig) loadCheckpointsFromEmbed() error {
	if c.CheckpointsFile == "" {
		return nil
	}
	data, err := c.loadOptionalConfigFileFromEmbedFS(c.CheckpointsFile)
	if err != nil {
		return err
	}
	cps, err := parseCheckpoints(
		replaceGenesisLineEndings(data),
		c.CheckpointsFileHash,
	)
	if err != nil {
		return err
	}
	c.checkpoints = cps
	return nil
}
