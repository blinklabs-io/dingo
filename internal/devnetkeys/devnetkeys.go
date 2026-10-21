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

// Package devnetkeys provides the local-only credentials used by Dingo's
// single-node development network.
package devnetkeys

import (
	"crypto/rand"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

//go:embed keys/vrf.skey keys/kes.skey keys/opcert.cert
//go:embed keys/faucet.skey keys/faucet.vkey keys/faucet.addr
var localKeys embed.FS

// InstallLocalTestKeys copies the local DevNet producer and faucet credentials
// into dir with owner-only permissions. Replacement is atomic on Unix; Windows
// uses os.Root.Rename and does not guarantee atomic replacement.
func InstallLocalTestKeys(dir string) error {
	return installLocalTestKeys(dir, []string{
		"vrf.skey", "kes.skey", "opcert.cert",
		"faucet.skey", "faucet.vkey", "faucet.addr",
	})
}

// InstallLocalTestKeysInRoot copies the local DevNet credentials into dir,
// resolving every operation beneath root.
func InstallLocalTestKeysInRoot(root *os.Root, dir string) error {
	return installLocalTestKeysInRoot(root, dir, []string{
		"vrf.skey", "kes.skey", "opcert.cert",
		"faucet.skey", "faucet.vkey", "faucet.addr",
	})
}

// InstallLocalTestFaucet copies only the bundled faucet credentials into dir
// with owner-only permissions, preserving existing producer credentials.
func InstallLocalTestFaucet(dir string) error {
	return installLocalTestKeys(dir, []string{
		"faucet.skey", "faucet.vkey", "faucet.addr",
	})
}

// InstallLocalTestFaucetInRoot copies the bundled faucet credentials into dir,
// resolving every operation beneath root.
func InstallLocalTestFaucetInRoot(root *os.Root, dir string) error {
	return installLocalTestKeysInRoot(root, dir, []string{
		"faucet.skey", "faucet.vkey", "faucet.addr",
	})
}

// RestrictLocalTestDirectory limits an opened DevNet directory to its owner.
func RestrictLocalTestDirectory(root *os.Root) error {
	dirFile, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("opening local DevNet directory: %w", err)
	}
	if err := restrictLocalTestKeyPath(dirFile, 0o700); err != nil {
		_ = dirFile.Close()
		return fmt.Errorf("restricting local DevNet directory: %w", err)
	}
	if err := dirFile.Close(); err != nil {
		return fmt.Errorf("closing local DevNet directory: %w", err)
	}
	return nil
}

func installLocalTestKeys(dir string, names []string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating local DevNet key directory: %w", err)
	}
	dirInfo, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("checking local DevNet key directory: %w", err)
	}
	if dirInfo.Mode()&os.ModeSymlink != 0 || !dirInfo.IsDir() {
		return fmt.Errorf("local DevNet key path %q is not a directory", dir)
	}
	root, err := openVerifiedLocalTestKeyRoot(dir, dirInfo)
	if err != nil {
		return err
	}
	defer root.Close() //nolint:errcheck // read/write errors are reported below
	return installLocalTestKeysIntoRoot(root, names)
}

func installLocalTestKeysInRoot(
	root *os.Root,
	dir string,
	names []string,
) error {
	current := root
	for part := range strings.SplitSeq(
		filepath.ToSlash(filepath.Clean(dir)),
		"/",
	) {
		if part == "." {
			continue
		}
		next, err := openLocalTestKeyChild(current, part)
		if current != root {
			_ = current.Close()
		}
		if err != nil {
			return err
		}
		current = next
	}
	if current != root {
		defer current.Close() //nolint:errcheck // read/write errors are reported below
	}
	return installLocalTestKeysIntoRoot(current, names)
}

// openLocalTestKeyChild opens one directory component beneath parent without
// following a symlink, creating it when absent, so a symlinked intermediate
// directory cannot redirect the key files to another directory in the root.
func openLocalTestKeyChild(parent *os.Root, name string) (*os.Root, error) {
	if err := parent.Mkdir(name, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("creating local DevNet key directory: %w", err)
	}
	info, err := parent.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("checking local DevNet key directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("local DevNet key path %q is not a directory", name)
	}
	child, err := parent.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("opening local DevNet key directory: %w", err)
	}
	opened, err := child.Stat(".")
	if err != nil {
		_ = child.Close()
		return nil, fmt.Errorf("checking opened local DevNet key directory: %w", err)
	}
	if !opened.IsDir() || !os.SameFile(info, opened) {
		_ = child.Close()
		return nil, fmt.Errorf(
			"local DevNet key directory %q changed while it was opened",
			name,
		)
	}
	return child, nil
}

func installLocalTestKeysIntoRoot(root *os.Root, names []string) error {
	if err := RestrictLocalTestDirectory(root); err != nil {
		return err
	}
	for _, name := range names {
		data, err := localKeys.ReadFile("keys/" + name)
		if err != nil {
			return fmt.Errorf(
				"reading bundled local DevNet key %q: %w",
				name,
				err,
			)
		}
		if err := installLocalTestKey(root, name, data); err != nil {
			return err
		}
	}
	return nil
}

func openVerifiedLocalTestKeyRoot(
	dir string,
	expected os.FileInfo,
) (*os.Root, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("opening local DevNet key directory: %w", err)
	}
	opened, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("checking opened local DevNet key directory: %w", err)
	}
	if !opened.IsDir() || !os.SameFile(expected, opened) {
		_ = root.Close()
		return nil, fmt.Errorf(
			"local DevNet key directory %q changed while it was opened",
			dir,
		)
	}
	return root, nil
}

func installLocalTestKey(
	root *os.Root,
	name string,
	data []byte,
) error {
	tmpName := ".dingo-devnet-key-" + rand.Text()
	tmp, err := root.OpenFile(
		tmpName,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return fmt.Errorf("creating temporary local DevNet key %q: %w", name, err)
	}
	defer func() {
		if tmp != nil {
			_ = tmp.Close()
		}
		_ = root.Remove(tmpName)
	}()
	if err := restrictLocalTestKeyPath(tmp, 0o600); err != nil {
		return fmt.Errorf("restricting temporary local DevNet key %q: %w", name, err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("writing temporary local DevNet key %q: %w", name, err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("syncing temporary local DevNet key %q: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temporary local DevNet key %q: %w", name, err)
	}
	tmp = nil
	if err := root.Rename(tmpName, name); err != nil {
		return fmt.Errorf("installing local DevNet key %q: %w", name, err)
	}
	if err := syncLocalTestKeyDirectory(root); err != nil {
		return fmt.Errorf("syncing local DevNet key directory: %w", err)
	}
	return nil
}
