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
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/blinklabs-io/dingo/database/models"
)

// poolMetadataMaxBytes is the maximum size, in bytes, of a stake pool
// off-chain metadata document. It mirrors cardano-api's
// validateAndHashStakePoolMetadata
// (Cardano.Api.Certificate.Internal.StakePoolMetadata), which rejects any
// document over 512 bytes before even attempting to decode it.
const poolMetadataMaxBytes = 512

// Field length constraints, mirrored from the same reference validator's
// FromJSON instance for StakePoolMetadata: name <=50 characters, description
// <=255 characters, ticker 3-5 characters. "homepage" is required but the
// reference validator does not bound its length.
const (
	poolMetadataNameMaxLen        = 50
	poolMetadataDescriptionMaxLen = 255
	poolMetadataTickerMinLen      = 3
	poolMetadataTickerMaxLen      = 5
)

// PoolMetadataFields holds the decoded fields of a validated stake pool
// off-chain metadata document.
type PoolMetadataFields struct {
	Name        string
	Description string
	Ticker      string
	Homepage    string
}

// ValidatePoolMetadata decodes and validates raw as Cardano stake-pool
// off-chain metadata. It mirrors cardano-api's
// validateAndHashStakePoolMetadata: the document must be at most 512 bytes,
// and must decode as a JSON object carrying required "name" (<=50
// characters), "description" (<=255 characters), "ticker" (3-5 characters),
// and "homepage" (required, unbounded length) string fields.
//
// A size violation is reported with the
// models.OffchainFetchErrBodyTooLargePrefix prefix; any other validation
// failure (JSON syntax, wrong JSON type, a missing/null required field, or a
// field failing its length constraint) is reported with the
// models.OffchainFetchErrDecodeErrorPrefix prefix. Callers classify the
// failure the same way readLimited's size errors are classified, by
// inspecting the returned error's message prefix.
func ValidatePoolMetadata(raw []byte) (*PoolMetadataFields, error) {
	if len(raw) > poolMetadataMaxBytes {
		return nil, fmt.Errorf(
			"%s %d bytes: stake pool metadata document is %d bytes",
			models.OffchainFetchErrBodyTooLargePrefix,
			poolMetadataMaxBytes,
			len(raw),
		)
	}
	var doc struct {
		Name        *string `json:"name"`
		Description *string `json:"description"`
		Ticker      *string `json:"ticker"`
		Homepage    *string `json:"homepage"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf(
			"%s: %w",
			models.OffchainFetchErrDecodeErrorPrefix,
			err,
		)
	}
	name, err := requirePoolMetadataField(
		"name", doc.Name, 0, poolMetadataNameMaxLen,
	)
	if err != nil {
		return nil, err
	}
	description, err := requirePoolMetadataField(
		"description", doc.Description, 0, poolMetadataDescriptionMaxLen,
	)
	if err != nil {
		return nil, err
	}
	ticker, err := requirePoolMetadataField(
		"ticker",
		doc.Ticker,
		poolMetadataTickerMinLen,
		poolMetadataTickerMaxLen,
	)
	if err != nil {
		return nil, err
	}
	// homepage has no length constraint in the reference validator, only a
	// presence requirement.
	homepage, err := requirePoolMetadataField("homepage", doc.Homepage, 0, 0)
	if err != nil {
		return nil, err
	}
	return &PoolMetadataFields{
		Name:        name,
		Description: description,
		Ticker:      ticker,
		Homepage:    homepage,
	}, nil
}

// requirePoolMetadataField validates one required stake-pool metadata string
// field. A missing key or a JSON null both decode to a nil pointer here,
// matching Aeson's (.:) accessor, which rejects both the same way. Length is
// counted in Unicode code points (utf8.RuneCountInString), matching the
// reference validator's use of Data.Text.length rather than a byte count.
// maxLen <= 0 means no upper bound is enforced (only "homepage" has none).
func requirePoolMetadataField(
	field string,
	value *string,
	minLen int,
	maxLen int,
) (string, error) {
	if value == nil {
		return "", fmt.Errorf(
			"%s: missing required field %q",
			models.OffchainFetchErrDecodeErrorPrefix,
			field,
		)
	}
	n := utf8.RuneCountInString(*value)
	if n < minLen {
		return "", fmt.Errorf(
			"%s: %q must have at least %d characters, but it has %d characters",
			models.OffchainFetchErrDecodeErrorPrefix,
			field,
			minLen,
			n,
		)
	}
	if maxLen > 0 && n > maxLen {
		return "", fmt.Errorf(
			"%s: %q must have at most %d characters, but it has %d characters",
			models.OffchainFetchErrDecodeErrorPrefix,
			field,
			maxLen,
			n,
		)
	}
	return *value, nil
}
