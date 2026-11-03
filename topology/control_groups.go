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

package topology

import (
	"errors"
	"strings"
)

// PeerControlGroup declares relay endpoints under one operator's control.
// Different identities assert independent control; DNS or valency groups do
// not supply that assertion. These groups are trusted operator configuration.
type PeerControlGroup struct {
	Identity     string                         `json:"identity"     yaml:"identity"`
	AccessPoints []TopologyConfigP2PAccessPoint `json:"accessPoints" yaml:"accessPoints"`
}

// ClonePeerControlGroups preserves nil (network defaults) versus an explicit
// empty list (no configured groups), and owns each endpoint slice.
func ClonePeerControlGroups(groups []PeerControlGroup) []PeerControlGroup {
	if groups == nil {
		return nil
	}
	result := make([]PeerControlGroup, len(groups))
	for i, group := range groups {
		result[i] = group
		result[i].AccessPoints = append(
			[]TopologyConfigP2PAccessPoint(nil),
			group.AccessPoints...)
	}
	return result
}

// Validate rejects incomplete trusted control declarations.
func (g PeerControlGroup) Validate() error {
	if strings.TrimSpace(g.Identity) == "" ||
		strings.TrimSpace(g.Identity) != g.Identity ||
		len(g.Identity) > 128 {
		return errors.New("peer control identity must contain 1-128 characters")
	}
	if len(g.AccessPoints) == 0 {
		return errors.New("peer control group has no endpoints")
	}
	for _, ap := range g.AccessPoints {
		if err := validatePeerSnapshotRelay(ap); err != nil {
			return err
		}
		if ap.Port == 0 {
			return errors.New("peer control endpoint requires a non-zero port")
		}
	}
	return nil
}
