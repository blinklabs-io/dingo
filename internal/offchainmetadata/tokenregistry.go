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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package offchainmetadata

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
)

const (
	// tokenRegistrySubjectMinLen is the hex length of a bare policy ID (28
	// bytes). A CIP-26 subject is the policy ID followed by the hex-encoded
	// asset name, so a policy-only subject is the shortest legal one.
	tokenRegistrySubjectMinLen = 56
	// tokenRegistrySubjectMaxLen is the hex length of a policy ID plus the
	// maximum 32-byte asset name.
	tokenRegistrySubjectMaxLen = tokenRegistrySubjectMinLen + 64
	// tokenRegistryMaxDecimals is CIP-26's own bound on the decimals
	// property, whose schema declares {"minimum": 0, "maximum": 19}. A value
	// outside it is a data error rather than a token denomination, and
	// passing one through would let a bad mapping shift a wallet's displayed
	// balance by an arbitrary number of orders of magnitude.
	tokenRegistryMaxDecimals = 19
)

// tokenRegistryProperty is the CIP-26 property envelope. Every property in a
// mapping is wrapped this way; only "subject" and "policy" are bare values.
// Signatures are captured but not verified here — baseline trust is the
// registry repository's PR gating plus HTTPS transport.
type tokenRegistryProperty struct {
	Value          json.RawMessage `json:"value"`
	SequenceNumber int             `json:"sequenceNumber"`
}

// tokenRegistryMapping mirrors the on-disk mappings/*.json shape. Properties
// are held as raw JSON so that one malformed envelope fails only its own
// property: decoding them into typed fields up front would fail the whole
// document instead.
type tokenRegistryMapping struct {
	Subject     string          `json:"subject"`
	Name        json.RawMessage `json:"name"`
	Ticker      json.RawMessage `json:"ticker"`
	Description json.RawMessage `json:"description"`
	URL         json.RawMessage `json:"url"`
	Logo        json.RawMessage `json:"logo"`
	Decimals    json.RawMessage `json:"decimals"`
}

// ParseTokenRegistryEntry decodes one CIP-26 registry mapping document.
//
// The subject is required and must be a hex string of a policy ID, optionally
// followed by a hex-encoded asset name: lookups build the same string from
// raw on-chain bytes, so a subject that is not hex could never be matched and
// is rejected outright. Subjects are lower-cased for the same reason.
//
// Individual properties are best-effort. A property whose envelope is
// malformed, whose value has the wrong JSON type, whose string value is
// blank, or whose decimals value is out of range is dropped and the remaining
// properties are kept. Only a document that fails to decode as JSON, or that
// carries no usable subject, returns an error.
func ParseTokenRegistryEntry(raw []byte) (*models.TokenRegistryEntry, error) {
	var doc tokenRegistryMapping
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decode token registry mapping: %w", err)
	}
	subject, err := normalizeTokenRegistrySubject(doc.Subject)
	if err != nil {
		return nil, err
	}
	entry := &models.TokenRegistryEntry{
		Subject:     subject,
		Name:        tokenRegistryStringProperty(doc.Name),
		Ticker:      tokenRegistryStringProperty(doc.Ticker),
		Description: tokenRegistryStringProperty(doc.Description),
		URL:         tokenRegistryStringProperty(doc.URL),
		Logo:        tokenRegistryStringProperty(doc.Logo),
		Decimals:    tokenRegistryDecimalsProperty(doc.Decimals),
	}
	return entry, nil
}

// normalizeTokenRegistrySubject validates and lower-cases a mapping subject.
func normalizeTokenRegistrySubject(raw string) (string, error) {
	subject := strings.ToLower(strings.TrimSpace(raw))
	if subject == "" {
		return "", errors.New("token registry mapping has no subject")
	}
	if len(subject) < tokenRegistrySubjectMinLen ||
		len(subject) > tokenRegistrySubjectMaxLen {
		return "", fmt.Errorf(
			"token registry mapping subject is %d characters, want %d to %d",
			len(subject),
			tokenRegistrySubjectMinLen,
			tokenRegistrySubjectMaxLen,
		)
	}
	if _, err := hex.DecodeString(subject); err != nil {
		return "", fmt.Errorf(
			"token registry mapping subject is not hex: %w",
			err,
		)
	}
	return subject, nil
}

// tokenRegistryStringProperty extracts a string-valued CIP-26 property,
// returning "" for anything malformed, absent, or blank.
func tokenRegistryStringProperty(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var prop tokenRegistryProperty
	if err := json.Unmarshal(raw, &prop); err != nil {
		return ""
	}
	var value string
	if err := json.Unmarshal(prop.Value, &value); err != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

// tokenRegistryDecimalsProperty extracts the decimals property, returning nil
// for anything malformed, absent, fractional, or outside
// [0, tokenRegistryMaxDecimals].
func tokenRegistryDecimalsProperty(raw json.RawMessage) *int {
	if len(raw) == 0 {
		return nil
	}
	var prop tokenRegistryProperty
	if err := json.Unmarshal(raw, &prop); err != nil {
		return nil
	}
	// encoding/json treats a null as a no-op for a non-pointer target: it
	// leaves the variable at its zero value and reports no error. Without
	// this guard a declared null would be published as decimals 0, which a
	// wallet would use to render balances unscaled. Absent means absent.
	if string(bytes.TrimSpace(prop.Value)) == "null" {
		return nil
	}
	// Decoding into int rejects both a fractional number and a quoted
	// string, which is what we want: either is a malformed decimals value.
	var value int
	if err := json.Unmarshal(prop.Value, &value); err != nil {
		return nil
	}
	if value < 0 || value > tokenRegistryMaxDecimals {
		return nil
	}
	return &value
}
