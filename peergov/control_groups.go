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

package peergov

import (
	"net"
	"strconv"
)

// configuredCorroborationIdentityLocked deliberately excludes pool identities:
// a pool key does not prove independence from a configured operator. The
// origin association comes from trusted discovery, never peer sharing or the
// connection's remote IP. Conflicting origins fail closed.
func (p *PeerGovernor) configuredCorroborationIdentityLocked(
	peer *Peer,
) string {
	if peer == nil {
		return ""
	}
	origins := []string{p.normalizeAddress(peer.Address)}
	if origin, ok := p.ledgerKnownAddrs[origins[0]]; ok {
		origins = append(origins, origin)
	}
	identity := ""
	for _, group := range p.config.ControlGroups {
		if group.Validate() != nil {
			continue
		}
		for _, ap := range group.AccessPoints {
			endpoint := p.normalizeAddress(
				net.JoinHostPort(
					ap.Address,
					strconv.FormatUint(uint64(ap.Port), 10),
				),
			)
			for _, origin := range origins {
				if endpoint != origin {
					continue
				}
				candidate := "configured:" + group.Identity
				if identity != "" && identity != candidate {
					peer.configuredControlConflict = true
					return ""
				}
				identity = candidate
			}
		}
	}
	if peer.configuredControlConflict {
		return ""
	}
	if identity != "" {
		if peer.configuredControlIdentity != "" &&
			peer.configuredControlIdentity != identity {
			peer.configuredControlConflict = true
			return ""
		}
		peer.configuredControlIdentity = identity
	}
	return identity
}
