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

package mithril

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// AggregatorSigner is a signer as the aggregator publishes and accepts it.
// Keys, signatures and the operational certificate are in the aggregator's
// hex-encoded JSON wire form.
type AggregatorSigner struct {
	PartyID                  string `json:"party_id"`
	VerificationKey          string `json:"verification_key"`
	VerificationKeySignature string `json:"verification_key_signature,omitempty"`
	OperationalCertificate   string `json:"operational_certificate,omitempty"`
	KESPeriod                uint64 `json:"kes_period"`
}

// EpochSettings holds the aggregator's signer sets for an epoch: the signers
// that sign in it and those that registered to sign in the next one.
type EpochSettings struct {
	Epoch          uint64             `json:"epoch"`
	CurrentSigners []AggregatorSigner `json:"current_signers"`
	NextSigners    []AggregatorSigner `json:"next_signers"`
}

// ProtocolConfiguration holds the aggregator's protocol configuration for an
// epoch.
type ProtocolConfiguration struct {
	ProtocolParameters ProtocolParameters `json:"protocol_parameters"`
}

// RegisteredSigners holds the stake of the signers registered in one epoch.
type RegisteredSigners struct {
	RegisteredAt  uint64                   `json:"registered_at"`
	SigningAt     uint64                   `json:"signing_at"`
	Registrations []StakeDistributionParty `json:"registrations"`
}

// SingleSignatureRegistration is a signer's individual signature for a
// signed entity.
type SingleSignatureRegistration struct {
	EntityType    SignedEntityType `json:"entity_type"`
	PartyID       string           `json:"party_id"`
	Signature     string           `json:"signature"`
	Indexes       []uint64         `json:"indexes"`
	SignedMessage string           `json:"signed_message"`
}

// MithrilStakeDistributionEntityType returns the signed entity type for the
// Mithril stake distribution of an epoch.
func MithrilStakeDistributionEntityType(epoch uint64) SignedEntityType {
	return SignedEntityType{
		raw: json.RawMessage(
			`{"` + signedEntityTypeMithrilStakeDistribution + `":` +
				strconv.FormatUint(epoch, 10) + `}`,
		),
	}
}

// GetEpochSettings retrieves the aggregator's current epoch settings.
// Corresponds to GET /epoch-settings.
func (c *Client) GetEpochSettings(
	ctx context.Context,
) (*EpochSettings, error) {
	var ret EpochSettings
	if err := c.getJSON(ctx, "/epoch-settings", &ret); err != nil {
		return nil, fmt.Errorf("getting epoch settings: %w", err)
	}
	return &ret, nil
}

// GetProtocolConfiguration retrieves the protocol configuration for an
// epoch. Corresponds to GET /protocol-configuration/{epoch}.
func (c *Client) GetProtocolConfiguration(
	ctx context.Context,
	epoch uint64,
) (*ProtocolConfiguration, error) {
	var ret ProtocolConfiguration
	if err := c.getJSON(
		ctx,
		"/protocol-configuration/"+strconv.FormatUint(epoch, 10),
		&ret,
	); err != nil {
		return nil, fmt.Errorf(
			"getting protocol configuration for epoch %d: %w",
			epoch,
			err,
		)
	}
	return &ret, nil
}

// GetRegisteredSigners retrieves the signers, with stake, registered in an
// epoch. Corresponds to GET /signers/registered/{epoch}.
func (c *Client) GetRegisteredSigners(
	ctx context.Context,
	epoch uint64,
) (*RegisteredSigners, error) {
	var ret RegisteredSigners
	if err := c.getJSON(
		ctx,
		"/signers/registered/"+strconv.FormatUint(epoch, 10),
		&ret,
	); err != nil {
		return nil, fmt.Errorf(
			"getting registered signers for epoch %d: %w",
			epoch,
			err,
		)
	}
	return &ret, nil
}

// RegisterSigner registers a signer with the aggregator during epoch.
// Corresponds to POST /register-signer.
func (c *Client) RegisterSigner(
	ctx context.Context,
	epoch uint64,
	signer AggregatorSigner,
) error {
	err := c.postJSON(
		ctx,
		"/register-signer",
		struct {
			Epoch uint64 `json:"epoch"`
			AggregatorSigner
		}{Epoch: epoch, AggregatorSigner: signer},
		http.StatusCreated,
	)
	if err != nil {
		return fmt.Errorf("registering signer: %w", err)
	}
	return nil
}

// RegisterSingleSignature submits a signer's individual signature.
// Corresponds to POST /register-signatures.
func (c *Client) RegisterSingleSignature(
	ctx context.Context,
	registration SingleSignatureRegistration,
) error {
	err := c.postJSON(
		ctx,
		"/register-signatures",
		registration,
		http.StatusCreated,
		http.StatusAccepted,
	)
	if err != nil {
		return fmt.Errorf("registering signature: %w", err)
	}
	return nil
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	body, err := c.doGet(ctx, c.aggregatorURL+path)
	if err != nil {
		return err
	}
	defer body.Close()
	if err := json.NewDecoder(body).Decode(out); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}

func (c *Client) postJSON(
	ctx context.Context,
	path string,
	payload any,
	okStatuses ...int,
) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encoding request: %w", err)
	}
	body, err := c.doRequest(
		ctx,
		http.MethodPost,
		c.aggregatorURL+path,
		raw,
		okStatuses...,
	)
	if err != nil {
		return err
	}
	return body.Close()
}
