// Copyright 2024 Blink Labs Software
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

/*
// ConnectionManagerTag represents the various tags that can be associated with a host or connection
type ConnectionManagerTag uint16

const (
	ConnectionManagerTagNone ConnectionManagerTag = iota

	ConnectionManagerTagHostLocalRoot
	ConnectionManagerTagHostPublicRoot
	ConnectionManagerTagHostBootstrapPeer
	ConnectionManagerTagHostP2PLedger
	ConnectionManagerTagHostP2PGossip

	ConnectionManagerTagRoleInitiator
	ConnectionManagerTagRoleResponder
	// TODO: add more tags
)

func (c ConnectionManagerTag) String() string {
	tmp := map[ConnectionManagerTag]string{
		ConnectionManagerTagHostLocalRoot:     "HostLocalRoot",
		ConnectionManagerTagHostPublicRoot:    "HostPublicRoot",
		ConnectionManagerTagHostBootstrapPeer: "HostBootstrapPeer",
		ConnectionManagerTagHostP2PLedger:     "HostP2PLedger",
		ConnectionManagerTagHostP2PGossip:     "HostP2PGossip",
		ConnectionManagerTagRoleInitiator:     "RoleInitiator",
		ConnectionManagerTagRoleResponder:     "RoleResponder",
		// TODO: add more tags to match those added above
	}
	ret, ok := tmp[c]
	if !ok {
		return "Unknown"
	}
	return ret
}

*/

type PeerSource uint16

const (
	PeerSourceUnknown               = 0
	PeerSourceTopologyLocalRoot     = 1
	PeerSourceTopologyPublicRoot    = 2
	PeerSourceTopologyBootstrapPeer = 3
	PeerSourceP2PLedger             = 4
	PeerSourceP2PGossip             = 5
)

type Peer struct {
	AccessPoints []PeerAccessPoint
	Source       PeerSource
	// TODO
}

type PeerAccessPoint struct {
	Address string
	Port    uint
}
