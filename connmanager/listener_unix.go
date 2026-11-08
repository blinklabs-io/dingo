//go:build !windows

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

package connmanager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"github.com/blinklabs-io/dingo/internal/securefs"
	"golang.org/x/sys/unix"
)

type publishedUnixListener struct {
	net.Listener
	path string
	root *os.Root
	name string
	info os.FileInfo
}

func (l *publishedUnixListener) Addr() net.Addr {
	return &net.UnixAddr{Name: l.path, Net: "unix"}
}

func (l *publishedUnixListener) Close() error {
	return errors.Join(
		l.Listener.Close(),
		removeUnixSocketIfSame(l.root, l.name, l.info),
		l.root.Close(),
	)
}

func removeUnixSocketIfSame(
	root *os.Root,
	name string,
	expected os.FileInfo,
) error {
	current, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !os.SameFile(expected, current) {
		return nil
	}
	return root.Remove(name)
}

func validateUnixSocketParent(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close() //nolint:errcheck // validation error is authoritative
	info, err := dir.Stat()
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("unix socket parent has unsupported metadata")
	}
	if int64(stat.Uid) != int64(os.Geteuid()) && stat.Uid != 0 {
		return errors.New("unix socket parent must be owned by the current user or root")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return errors.New(
			"unix socket parent must not be writable by group or other users",
		)
	}
	return nil
}

// createUnixSocketListener publishes a configured-mode socket only after it
// is ready. The private staging directory prevents a client from connecting
// between bind and chmod, while the no-replace rename preserves the final
// path if another entry appears after stale-socket preparation.
func createUnixSocketListener(
	ctx context.Context,
	listenConfig net.ListenConfig,
	address string,
	mode os.FileMode,
) (net.Listener, error) {
	if mode == 0 {
		if err := prepareUnixSocketPath(address); err != nil {
			return nil, err
		}
		return listenConfig.Listen(ctx, "unix", address)
	}
	mode = mode.Perm()
	if mode&0o002 != 0 {
		return nil, fmt.Errorf(
			"unix socket mode %04o permits writes by other users",
			mode,
		)
	}

	parent := filepath.Dir(address)
	base := filepath.Base(address)
	root, err := os.OpenRoot(parent)
	if err != nil {
		return nil, fmt.Errorf("open unix socket parent: %w", err)
	}
	closeRootOnError := true
	defer func() {
		if closeRootOnError {
			_ = root.Close()
		}
	}()
	if err := validateUnixSocketParent(root); err != nil {
		return nil, fmt.Errorf("validate unix socket parent: %w", err)
	}
	if err := prepareUnixSocketPathInRoot(root, base, address); err != nil {
		return nil, err
	}
	stagingDir, err := os.MkdirTemp(parent, "")
	if err != nil {
		return nil, fmt.Errorf("create unix socket staging directory: %w", err)
	}
	stagedPath := filepath.Join(stagingDir, "s")
	removeStaging := func() {
		_ = os.Remove(stagedPath)
		_ = os.Remove(stagingDir)
	}
	defer removeStaging()
	maxPathLen := len(unix.RawSockaddrUnix{}.Path) - 1
	if len(stagedPath) > maxPathLen {
		return nil, fmt.Errorf(
			"secure unix socket staging path is %d bytes; platform limit is %d; shorten the parent directory by at least %d bytes",
			len(stagedPath),
			maxPathLen,
			len(stagedPath)-maxPathLen,
		)
	}
	listener, err := listenConfig.Listen(ctx, "unix", stagedPath)
	if err != nil {
		return nil, err
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		return nil, errors.New("unix listener has unexpected concrete type")
	}
	unixListener.SetUnlinkOnClose(false)
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = listener.Close()
		}
	}()
	if err := os.Chmod(stagedPath, mode); err != nil {
		return nil, fmt.Errorf("set unix socket mode: %w", err)
	}
	stagedInfo, err := os.Lstat(stagedPath)
	if err != nil {
		return nil, fmt.Errorf("inspect staged unix socket: %w", err)
	}

	stageName := filepath.Join(filepath.Base(stagingDir), "s")
	if err := securefs.RenameNoReplace(root, stageName, base); err != nil {
		return nil, fmt.Errorf("publish unix socket: %w", err)
	}
	publishedInfo, err := root.Lstat(base)
	if err != nil || !os.SameFile(stagedInfo, publishedInfo) {
		_ = removeUnixSocketIfSame(root, base, stagedInfo)
		if err != nil {
			return nil, fmt.Errorf("inspect published unix socket: %w", err)
		}
		return nil, errors.New("published unix socket identity changed")
	}

	closeOnError = false
	closeRootOnError = false
	return &publishedUnixListener{
		Listener: listener,
		path:     address,
		root:     root,
		name:     base,
		info:     publishedInfo,
	}, nil
}

// socketControl is a helper function for setting socket options on outbound and listener sockets
func socketControl(network, address string, c syscall.RawConn) error {
	var innerErr error
	err := c.Control(func(fd uintptr) {
		err := unix.SetsockoptInt(
			int(fd), //nolint:gosec // fd fits in int on all supported platforms
			unix.SOL_SOCKET,
			unix.SO_REUSEADDR,
			1,
		)
		if err != nil {
			innerErr = err
			return
		}
		err = unix.SetsockoptInt(
			int(fd), //nolint:gosec // fd fits in int on all supported platforms
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

// createPipeListener should never be called on non-Windows systems
func createPipeListener(_, _ string) (net.Listener, error) {
	return nil, errors.New("pipe listener not supported on non-Windows systems")
}
