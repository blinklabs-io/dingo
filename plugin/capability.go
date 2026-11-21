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

package plugin

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ParseCapability parses a capability written "<name>/v<N>".
//
// The name is one or more dot-separated segments of lowercase letters and
// digits. The segments "provider" and "config" are reserved because they are
// the keys of a selection in the nested plugins configuration tree. N is a
// non-negative decimal integer without leading zeros that fits in 32 bits.
func ParseCapability(s string) (Capability, error) {
	c := Capability(s)
	if err := c.validate(); err != nil {
		return "", err
	}
	return c, nil
}

// Valid reports whether c is a well-formed capability. It says nothing about
// whether a particular Host speaks it; see Host.Speaks.
func (c Capability) Valid() bool {
	return c.validate() == nil
}

// Name returns the capability name without its contract version. A malformed
// capability is returned unchanged.
func (c Capability) Name() string {
	name, _, ok := strings.Cut(string(c), "/")
	if !ok {
		return string(c)
	}
	return name
}

func (c Capability) validate() error {
	s := string(c)
	if s == "" {
		return fmt.Errorf("unknown plugin capability %q: empty", s)
	}
	name, version, ok := strings.Cut(s, "/")
	if !ok {
		return fmt.Errorf(
			"unknown plugin capability %q: missing contract version, want <name>/v<N>",
			s,
		)
	}
	if err := validateCapabilityName(name); err != nil {
		return fmt.Errorf("unknown plugin capability %q: %w", s, err)
	}
	if _, err := parseContractVersion(version); err != nil {
		return fmt.Errorf("unknown plugin capability %q: %w", s, err)
	}
	return nil
}

func validateCapabilityName(name string) error {
	if name == "" {
		return errors.New("empty name")
	}
	for segment := range strings.SplitSeq(name, ".") {
		if segment == "" {
			return errors.New("empty name segment")
		}
		for _, r := range segment {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
				return fmt.Errorf(
					"name segment %q must be lowercase letters and digits",
					segment,
				)
			}
		}
		if segment == "provider" || segment == "config" {
			return fmt.Errorf("name segment %q is reserved", segment)
		}
	}
	return nil
}

func parseContractVersion(version string) (uint32, error) {
	digits, ok := strings.CutPrefix(version, "v")
	if !ok || digits == "" {
		return 0, fmt.Errorf("contract version %q must be v<N>", version)
	}
	if len(digits) > 1 && digits[0] == '0' {
		return 0, fmt.Errorf(
			"contract version %q must not have leading zeros",
			version,
		)
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("contract version %q must be v<N>", version)
		}
	}
	n, err := strconv.ParseUint(digits, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("contract version %q is out of range", version)
	}
	return uint32(n), nil
}
