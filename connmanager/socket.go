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

package connmanager

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// socketControl is a helper function for setting socket options on outbound and
// listener sockets
func socketControl(network, address string, c syscall.RawConn) error {
	var innerErr error
	err := c.Control(func(fd uintptr) {
		err := unix.SetsockoptInt(
			int(fd),
			unix.SOL_SOCKET,
			unix.SO_REUSEADDR,
			1,
		)
		if err != nil {
			innerErr = err
			return
		}
		err = unix.SetsockoptInt(
			int(fd),
			unix.SOL_SOCKET,
			unix.SO_REUSEPORT,
			1,
		)
		if err != nil {
			innerErr = err
			return
		}
	})
	if innerErr != nil {
		return innerErr
	}
	if err != nil {
		return err
	}
	return nil
}
