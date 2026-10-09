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
	"embed"
	"fmt"
	"os"
	"path/filepath"
)

//go:embed keys/vrf.skey keys/kes.skey keys/opcert.cert
var localKeys embed.FS

// InstallLocalTestKeys copies the local DevNet producer credentials into dir
// with owner-only permissions.
func InstallLocalTestKeys(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating local DevNet key directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("restricting local DevNet key directory: %w", err)
	}
	for _, name := range []string{"vrf.skey", "kes.skey", "opcert.cert"} {
		data, err := localKeys.ReadFile("keys/" + name)
		if err != nil {
			return fmt.Errorf(
				"reading bundled local DevNet key %q: %w",
				name,
				err,
			)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return fmt.Errorf("writing local DevNet key %q: %w", name, err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			return fmt.Errorf("restricting local DevNet key %q: %w", name, err)
		}
	}
	return nil
}
