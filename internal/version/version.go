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

package version

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// These are populated at build time
var (
	Version    string
	CommitHash string
)

// BlockHeaderProtocolMinor identifies Dingo-forged blocks without changing
// the ledger protocol major version used for era selection.
const BlockHeaderProtocolMinor uint64 = 69

func GetVersionString() string {
	if Version != "" {
		return fmt.Sprintf("%s (commit %s)", Version, CommitHash)
	} else {
		return fmt.Sprintf("devel (commit %s)", CommitHash)
	}
}

// HeaderProtocolMinor returns the legacy identifier unless version signalling
// is enabled. Version is the same linker-populated value used by the CLI.
func HeaderProtocolMinor(enabled bool) (uint64, error) {
	if !enabled {
		return BlockHeaderProtocolMinor, nil
	}
	return encodeHeaderVersion(Version)
}

// SoftwareTag selects what a forged header reports in its minor protocol
// version (self_reported_software_tag).
type SoftwareTag string

const (
	// SoftwareTagIdentifier reports only the Dingo identifier (69).
	SoftwareTagIdentifier SoftwareTag = "identifier"
	// SoftwareTagVersion reports the identifier plus the release version.
	SoftwareTagVersion SoftwareTag = "version"
	// SoftwareTagObfuscated reports a fresh random value for every block.
	SoftwareTagObfuscated SoftwareTag = "obfuscated"
)

// ParseSoftwareTag validates the configured software tag. Empty means
// identifier.
func ParseSoftwareTag(mode string) (SoftwareTag, error) {
	switch m := SoftwareTag(mode); m {
	case "":
		return SoftwareTagIdentifier, nil
	case SoftwareTagIdentifier, SoftwareTagVersion, SoftwareTagObfuscated:
		return m, nil
	}
	return "", fmt.Errorf(
		"invalid blockSoftwareTag %q: expected %q, %q or %q", mode,
		SoftwareTagIdentifier, SoftwareTagVersion, SoftwareTagObfuscated,
	)
}

// NextHeaderProtocolMinor returns the minor protocol version for one forged
// header. Obfuscated mode ignores the build version and returns a fresh
// uniformly random value over the whole range the header CDDL allows for
// self_reported_software_tag (uint .size 4), so neither the Dingo identifier
// nor a release version can be read from the block.
func NextHeaderProtocolMinor(mode SoftwareTag) (uint64, error) {
	if mode != SoftwareTagObfuscated {
		return HeaderProtocolMinor(mode == SoftwareTagVersion)
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("generate obfuscated software tag: %w", err)
	}
	return uint64(binary.BigEndian.Uint32(b[:])), nil
}

var releaseVersion = regexp.MustCompile(`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)

func encodeHeaderVersion(value string) (uint64, error) {
	parts := releaseVersion.FindStringSubmatch(value)
	if parts == nil {
		return 0, fmt.Errorf("version software tag requires a build version major.minor.patch, got %q", value)
	}
	// Reject empty identifiers and numeric prerelease identifiers with leading zeros.
	for i, suffix := range parts[4:] {
		if suffix == "" {
			continue
		}
		for id := range strings.SplitSeq(suffix[1:], ".") {
			if id == "" {
				return 0, fmt.Errorf("invalid build version %q", value)
			}
			if i == 0 && len(id) > 1 && id[0] == '0' && strings.Trim(id, "0123456789") == "" {
				return 0, fmt.Errorf("invalid build version %q", value)
			}
		}
	}
	var numbers [3]uint64
	for i, width := range []int{6, 8, 8} {
		n, err := strconv.ParseUint(parts[i+1], 10, width)
		if err != nil {
			return 0, fmt.Errorf("version software tag component %q exceeds %d bits", parts[i+1], width)
		}
		numbers[i] = n
	}
	payload := numbers[0]<<16 | numbers[1]<<8 | numbers[2]
	return payload<<8 | BlockHeaderProtocolMinor, nil
}
