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

package sops

import (
	"errors"
	"fmt"
	"os"

	sopsapi "github.com/getsops/sops/v3"
	"github.com/getsops/sops/v3/aes"
	scommon "github.com/getsops/sops/v3/cmd/sops/common"
	"github.com/getsops/sops/v3/decrypt"
	"github.com/getsops/sops/v3/gcpkms"
	skeys "github.com/getsops/sops/v3/keys"
	jsonstore "github.com/getsops/sops/v3/stores/json"
	"github.com/getsops/sops/v3/version"
)

func Decrypt(data []byte) ([]byte, error) {
	ret, err := decrypt.Data(data, "json")
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func Encrypt(data []byte) ([]byte, error) {
	input := &jsonstore.Store{}
	output := &jsonstore.Store{}

	// prevent double encryption
	branches, err := input.LoadPlainFile(data)
	if err != nil {
		return nil, fmt.Errorf("error loading data: %w", err)
	}
	for _, branch := range branches {
		for _, b := range branch {
			if b.Key == "sops" {
				return nil, errors.New("already encrypted")
			}
		}
	}

	// create tree and encrypt
	tree := sopsapi.Tree{Branches: branches}

	// Configure Google KMS from env to encrypt
	rid := os.Getenv("DINGO_GCP_KMS_RESOURCE_ID")
	if rid == "" {
		return nil, errors.New("DINGO_GCP_KMS_RESOURCE_ID not set: SOPS requires at least one master key to encrypt")
	}
	keys := []skeys.MasterKey{}
	for _, k := range gcpkms.MasterKeysFromResourceIDString(rid) {
		keys = append(keys, k)
	}
	tree.Metadata = sopsapi.Metadata{
		KeyGroups: []sopsapi.KeyGroup{keys},
		Version:   version.Version,
	}

	dataKey, errs := tree.GenerateDataKey()
	if len(errs) > 0 {
		return nil, fmt.Errorf("failed generating data key: %v", errs)
	}
	if err := scommon.EncryptTree(scommon.EncryptTreeOpts{
		DataKey: dataKey,
		Tree:    &tree,
		Cipher:  aes.NewCipher(),
	}); err != nil {
		return nil, fmt.Errorf("failed encrypt: %w", err)
	}

	encrypted, err := output.EmitEncryptedFile(tree)
	if err != nil {
		return nil, fmt.Errorf("failed output: %w", err)
	}
	return encrypted, nil
}
