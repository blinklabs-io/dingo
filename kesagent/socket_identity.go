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

package kesagent

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

var (
	errUnsafeSocket = errors.New("kesagent: unsafe service socket")
	errWrongPeer    = errors.New("kesagent: unexpected service identity")
)

type socketIdentity struct {
	info     os.FileInfo
	parent   os.FileInfo
	ownerUID uint32
	abstract bool
}

func inspectSocketIdentity(path string) (socketIdentity, error) {
	if isAbstractSocketPath(path) {
		return socketIdentity{abstract: true}, nil
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return socketIdentity{}, fmt.Errorf(
			"kesagent: inspect service socket directory: %w",
			err,
		)
	}
	if !parent.IsDir() || parent.Mode().Perm()&0o022 != 0 {
		return socketIdentity{}, fmt.Errorf(
			"%w: service socket directory is not protected from group or other writes",
			errUnsafeSocket,
		)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return socketIdentity{}, fmt.Errorf(
			"kesagent: inspect service socket: %w",
			err,
		)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return socketIdentity{}, fmt.Errorf(
			"%w: configured path is not a Unix socket",
			errUnsafeSocket,
		)
	}
	if info.Mode().Perm()&0o002 != 0 {
		return socketIdentity{}, fmt.Errorf(
			"%w: service socket grants other write access",
			errUnsafeSocket,
		)
	}
	ownerUID, err := socketOwnerUID(info)
	if err != nil {
		return socketIdentity{}, err
	}
	return socketIdentity{info: info, parent: parent, ownerUID: ownerUID}, nil
}

func authenticateSocketPeer(
	conn net.Conn,
	path string,
	before socketIdentity,
	pinnedUID uint32,
	pinned bool,
) (uint32, error) {
	peerUID, err := unixPeerUID(conn)
	if err != nil {
		return 0, fmt.Errorf("kesagent: authenticate service socket peer: %w", err)
	}
	if before.abstract {
		if peerUID != currentEUID() {
			return 0, fmt.Errorf(
				"%w: abstract socket peer does not run as the client user",
				errWrongPeer,
			)
		}
	} else {
		after, err := inspectSocketIdentity(path)
		if err != nil {
			return 0, err
		}
		if after.abstract ||
			!os.SameFile(before.info, after.info) ||
			!os.SameFile(before.parent, after.parent) {
			return 0, fmt.Errorf(
				"%w: service socket path changed while connecting",
				errUnsafeSocket,
			)
		}
		if peerUID != before.ownerUID {
			return 0, fmt.Errorf(
				"%w: connected process does not own the service socket",
				errWrongPeer,
			)
		}
	}
	if pinned && peerUID != pinnedUID {
		return 0, fmt.Errorf(
			"%w: service owner changed after its identity was pinned",
			errWrongPeer,
		)
	}
	return peerUID, nil
}
