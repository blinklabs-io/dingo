// Copyright 2025 Blink Labs Software
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

package ouroboros

import (
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
)

// PeersharingServerConnOpts returns server connection options for the PeerSharing protocol.
func PeersharingServerConnOpts(
	shareRequestFunc opeersharing.ShareRequestFunc,
) []opeersharing.PeerSharingOptionFunc {
	return []opeersharing.PeerSharingOptionFunc{
		opeersharing.WithShareRequestFunc(shareRequestFunc),
	}
}

// PeersharingClientConnOpts returns client connection options for the PeerSharing protocol.
func PeersharingClientConnOpts() []opeersharing.PeerSharingOptionFunc {
	// We don't provide any client options, but we have this here for consistency
	return nil
}
