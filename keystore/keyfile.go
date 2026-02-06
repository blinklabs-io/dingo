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

package keystore

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/vrf"
)

// keyFileEnvelope represents the JSON structure of a cardano-cli key file.
type keyFileEnvelope struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	CborHex     string `json:"cborHex"`
}

// loadedKey holds the parsed contents of a key file.
type loadedKey struct {
	Type        string
	Description string
	RawCBOR     []byte
	VKey        []byte
	SKey        []byte
	// OpCert fields (only populated for NodeOperationalCertificate type)
	OpCertIssueNumber uint64
	OpCertKesPeriod   uint64
	OpCertSignature   []byte
	OpCertColdVKey    []byte
}

// loadKeyFromFile loads a key from a file path (cardano-cli format).
// Supports VRF, KES, and operational certificates.
// Returns ErrInsecureFileMode if the file has group or other access.
//
// The file is opened first and permissions are checked on the open handle
// (via fstat on Unix) to avoid a TOCTOU race between the permission check
// and the read.
func loadKeyFromFile(path string) (*loadedKey, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open key file %q: %w", path, err)
	}
	defer f.Close()

	if err := checkOpenFilePermissions(f); err != nil {
		return nil, err
	}

	// Limit read to 1 MiB to guard against accidentally pointing at a
	// large file. Valid key files are well under this size.
	const maxKeyFileSize = 1 << 20
	data, err := io.ReadAll(io.LimitReader(f, maxKeyFileSize))
	if err != nil {
		return nil, fmt.Errorf("failed to read key file %q: %w", path, err)
	}
	key, err := parseKeyEnvelope(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key file %q: %w", path, err)
	}
	return key, nil
}

// loadOpCertFromFile loads an operational certificate from a file path.
// Unlike loadKeyFromFile, this does not check file permissions because
// operational certificates contain only public data (cold vkey, KES vkey,
// signature) and do not require protection like secret keys.
func loadOpCertFromFile(path string) (*loadedKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read OpCert file %q: %w", path, err)
	}
	key, err := parseKeyEnvelope(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OpCert file %q: %w", path, err)
	}
	if key.Type != "NodeOperationalCertificate" {
		return nil, fmt.Errorf(
			"expected NodeOperationalCertificate, got %s",
			key.Type,
		)
	}
	return key, nil
}

// parseKeyEnvelope parses a cardano-cli format key file.
func parseKeyEnvelope(fileBytes []byte) (*loadedKey, error) {
	var env keyFileEnvelope
	if err := json.Unmarshal(fileBytes, &env); err != nil {
		return nil, fmt.Errorf("could not parse key file envelope: %w", err)
	}

	cborData, err := hex.DecodeString(env.CborHex)
	if err != nil {
		return nil, fmt.Errorf("could not decode key from hex: %w", err)
	}

	lk := &loadedKey{
		Type:        env.Type,
		Description: env.Description,
		RawCBOR:     cborData,
	}

	// Decode cbor encoded key bytes based on type
	switch env.Type {
	// VRF keys
	case "VrfSigningKey_PraosVRF", "VRFSigningKey_PraosVRF":
		sk, vk, err := decodeVRFSKey(cborData)
		if err != nil {
			return nil, err
		}
		lk.SKey, lk.VKey = sk, vk
		return lk, nil

	// KES keys
	case "KesSigningKey_ed25519_kes_2^6", "KESSigningKey_PraosV2":
		sk, vk, err := decodeKESSKey(cborData)
		if err != nil {
			return nil, err
		}
		lk.SKey, lk.VKey = sk, vk
		return lk, nil

	// Operational Certificate
	case "NodeOperationalCertificate":
		kesVkey, issueNumber, kesPeriod, signature, coldVkey, err := decodeOpCert(
			cborData,
		)
		if err != nil {
			return nil, err
		}
		lk.VKey = kesVkey
		lk.OpCertIssueNumber = issueNumber
		lk.OpCertKesPeriod = kesPeriod
		lk.OpCertSignature = signature
		lk.OpCertColdVKey = coldVkey
		return lk, nil

	default:
		return nil, fmt.Errorf("unknown key type: %s", env.Type)
	}
}

// decodeVRFSKey decodes a VRF signing key from CBOR.
func decodeVRFSKey(skeyBytes []byte) ([]byte, []byte, error) {
	var keyBytes []byte
	if _, err := cbor.Decode(skeyBytes, &keyBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal VRF skey CBOR: %w", err)
	}

	switch len(keyBytes) {
	case vrf.SeedSize:
		// Just the seed - generate public key
		pubKey, _, err := vrf.KeyGen(keyBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to derive VRF public key: %w", err)
		}
		return keyBytes, pubKey, nil
	case vrf.SeedSize + vrf.PublicKeySize:
		// Seed + public key (cardano-cli format)
		// Derive pubkey from seed rather than trusting file contents
		seed := keyBytes[:vrf.SeedSize]
		pubKey, _, err := vrf.KeyGen(seed)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to derive VRF public key: %w", err)
		}
		return seed, pubKey, nil
	default:
		return nil, nil, fmt.Errorf(
			"invalid VRF skey bytes: expected %d or %d, got %d",
			vrf.SeedSize,
			vrf.SeedSize+vrf.PublicKeySize,
			len(keyBytes),
		)
	}
}

// decodeKESSKey decodes a KES signing key from CBOR.
func decodeKESSKey(skeyBytes []byte) ([]byte, []byte, error) {
	var keyBytes []byte
	if _, err := cbor.Decode(skeyBytes, &keyBytes); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal KES skey CBOR: %w", err)
	}
	if len(keyBytes) != kes.CardanoKesSecretKeySize {
		return nil, nil, fmt.Errorf(
			"invalid KES skey bytes: expected %d, got %d",
			kes.CardanoKesSecretKeySize,
			len(keyBytes),
		)
	}
	// Create a SecretKey struct to extract the public key
	sk := &kes.SecretKey{
		Depth:  kes.CardanoKesDepth,
		Period: 0, // Period is not encoded in the key file
		Data:   keyBytes,
	}
	pubKey := kes.PublicKey(sk)
	return keyBytes, pubKey, nil
}

// decodeOpCert decodes an operational certificate from CBOR.
// OpCert CBOR format: [[kes_vkey, issue_number, kes_period, signature], cold_vkey]
func decodeOpCert(
	certBytes []byte,
) ([]byte, uint64, uint64, []byte, []byte, error) {
	var outerData []any
	if _, err := cbor.Decode(certBytes, &outerData); err != nil {
		return nil, 0, 0, nil, nil, fmt.Errorf(
			"failed to unmarshal OpCert CBOR: %w",
			err,
		)
	}
	if len(outerData) != 2 {
		return nil, 0, 0, nil, nil, fmt.Errorf(
			"invalid OpCert: expected 2-element outer array, got %d",
			len(outerData),
		)
	}

	// Extract inner certificate array
	certData, ok := outerData[0].([]any)
	if !ok {
		return nil, 0, 0, nil, nil, errors.New(
			"invalid OpCert: first element is not an array",
		)
	}
	if len(certData) != 4 {
		return nil, 0, 0, nil, nil, fmt.Errorf(
			"invalid OpCert: expected 4-element cert array, got %d",
			len(certData),
		)
	}

	// Extract cold vkey
	coldVkey, ok := outerData[1].([]byte)
	if !ok {
		return nil, 0, 0, nil, nil, errors.New(
			"invalid OpCert: cold_vkey is not bytes",
		)
	}
	if len(coldVkey) != 32 {
		return nil, 0, 0, nil, nil, fmt.Errorf(
			"invalid OpCert: cold_vkey expected 32 bytes, got %d",
			len(coldVkey),
		)
	}

	// Extract KES vkey from inner array
	kesVkey, ok := certData[0].([]byte)
	if !ok {
		return nil, 0, 0, nil, nil, errors.New(
			"invalid OpCert: kes_vkey is not bytes",
		)
	}
	if len(kesVkey) != 32 {
		return nil, 0, 0, nil, nil, fmt.Errorf(
			"invalid OpCert: kes_vkey expected 32 bytes, got %d",
			len(kesVkey),
		)
	}

	// Extract issue number (can be uint64 or int64 depending on CBOR encoding)
	var issueNumber uint64
	switch v := certData[1].(type) {
	case uint64:
		issueNumber = v
	case int64:
		if v < 0 {
			return nil, 0, 0, nil, nil, errors.New(
				"invalid OpCert: issue_number cannot be negative",
			)
		}
		issueNumber = uint64(v) //nolint:gosec // G115: validated non-negative above
	default:
		return nil, 0, 0, nil, nil, fmt.Errorf(
			"invalid OpCert: issue_number has unexpected type %T",
			certData[1],
		)
	}

	// Extract KES period
	var kesPeriod uint64
	switch v := certData[2].(type) {
	case uint64:
		kesPeriod = v
	case int64:
		if v < 0 {
			return nil, 0, 0, nil, nil, errors.New(
				"invalid OpCert: kes_period cannot be negative",
			)
		}
		kesPeriod = uint64(v) //nolint:gosec // G115: validated non-negative above
	default:
		return nil, 0, 0, nil, nil, fmt.Errorf(
			"invalid OpCert: kes_period has unexpected type %T",
			certData[2],
		)
	}

	// Extract cold signature
	signature, ok := certData[3].([]byte)
	if !ok {
		return nil, 0, 0, nil, nil, errors.New(
			"invalid OpCert: cold_signature is not bytes",
		)
	}
	if len(signature) != 64 {
		return nil, 0, 0, nil, nil, fmt.Errorf(
			"invalid OpCert: cold_signature expected 64 bytes, got %d",
			len(signature),
		)
	}

	return kesVkey, issueNumber, kesPeriod, signature, coldVkey, nil
}
