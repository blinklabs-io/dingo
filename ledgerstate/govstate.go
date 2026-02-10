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

package ledgerstate

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// ParsedConstitution holds decoded constitution data.
type ParsedConstitution struct {
	AnchorUrl  string
	AnchorHash []byte // 32 bytes
	PolicyHash []byte // 28 bytes, nil if no guardrails script
}

// ParsedCommitteeMember holds a committee member credential
// and expiration.
type ParsedCommitteeMember struct {
	ColdCredential []byte // 28 bytes
	ExpiresEpoch   uint64
}

// ParsedGovProposal holds a decoded governance proposal.
type ParsedGovProposal struct {
	TxHash       []byte // 32 bytes
	ActionIndex  uint32
	ActionType   uint8
	Deposit      uint64
	ReturnAddr   []byte
	AnchorUrl    string
	AnchorHash   []byte
	ProposedIn   uint64
	ExpiresAfter uint64
}

// ParsedGovState holds all decoded governance state components.
type ParsedGovState struct {
	Constitution *ParsedConstitution
	Committee    []ParsedCommitteeMember
	Proposals    []ParsedGovProposal
}

// ParseGovState decodes governance state from raw CBOR.
// Returns nil, nil for pre-Conway eras (eraIndex < 6).
//
// Conway GovState structure:
//
//	[proposals, committee, constitution,
//	 cur_pparams, prev_pparams, future_pparams]
//
// Constitution parsing is required. Committee and proposals
// parsing is best-effort: errors are collected and returned
// alongside any partial results so the caller can log them.
func ParseGovState(
	data cbor.RawMessage,
	eraIndex int,
) (*ParsedGovState, error) {
	if eraIndex < EraConway {
		return nil, nil
	}
	if len(data) == 0 {
		return nil, nil
	}

	fields, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding GovState: %w", err,
		)
	}
	if len(fields) < 3 {
		return nil, fmt.Errorf(
			"GovState has %d elements, expected at least 3",
			len(fields),
		)
	}

	result := &ParsedGovState{}

	// Parse constitution (field 2) — required
	constitution, err := parseConstitution(fields[2])
	if err != nil {
		return nil, fmt.Errorf(
			"parsing constitution: %w", err,
		)
	}
	result.Constitution = constitution

	// Parse committee (field 1) — best-effort
	var warnings []error
	committee, err := parseCommittee(fields[1])
	if err != nil {
		warnings = append(warnings, fmt.Errorf(
			"parsing committee: %w", err,
		))
	}
	result.Committee = committee

	// Parse proposals (field 0) — best-effort
	proposals, err := parseProposals(fields[0])
	if err != nil {
		warnings = append(warnings, fmt.Errorf(
			"parsing proposals: %w", err,
		))
	}
	result.Proposals = proposals

	return result, errors.Join(warnings...)
}

// parseConstitution decodes a Constitution from CBOR.
// Constitution = [anchor, scriptHash]
// anchor = [url_text, hash_bytes]
func parseConstitution(data []byte) (
	*ParsedConstitution, error,
) {
	fields, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding constitution: %w", err,
		)
	}
	if len(fields) < 2 {
		return nil, fmt.Errorf(
			"constitution has %d elements, expected 2",
			len(fields),
		)
	}

	// Decode anchor = [url, hash]
	anchor, err := decodeRawArray(fields[0])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding constitution anchor: %w", err,
		)
	}
	if len(anchor) < 2 {
		return nil, fmt.Errorf(
			"constitution anchor has %d elements, "+
				"expected 2",
			len(anchor),
		)
	}

	c := &ParsedConstitution{}

	var url string
	if _, err := cbor.Decode(
		anchor[0], &url,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding constitution URL: %w", err,
		)
	}
	c.AnchorUrl = url

	var hash []byte
	if _, err := cbor.Decode(
		anchor[1], &hash,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding constitution hash: %w", err,
		)
	}
	c.AnchorHash = hash

	// Decode optional script hash (null or bytes(28))
	var policyHash []byte
	if _, err := cbor.Decode(
		fields[1], &policyHash,
	); err == nil {
		if len(policyHash) == 28 {
			c.PolicyHash = policyHash
		} else if len(policyHash) > 0 {
			return nil, fmt.Errorf(
				"constitution policy hash has %d bytes, "+
					"expected 28",
				len(policyHash),
			)
		}
		// len == 0 means null/empty: no guardrails script
	}

	return c, nil
}

// parseCommittee decodes a Committee from CBOR.
// The committee field uses StrictMaybe encoding:
//   - [0]                     = SNothing (no committee)
//   - [1, [members_map, quorum]] = SJust committee
func parseCommittee(data []byte) (
	[]ParsedCommitteeMember, error,
) {
	outer, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding committee: %w", err,
		)
	}
	if len(outer) == 0 {
		return nil, nil
	}

	// Check the StrictMaybe tag
	var tag uint64
	if _, err := cbor.Decode(outer[0], &tag); err != nil {
		return nil, fmt.Errorf(
			"decoding committee tag: %w", err,
		)
	}
	if tag == 0 {
		return nil, nil
	}
	if tag != 1 {
		return nil, fmt.Errorf(
			"unexpected StrictMaybe committee tag: %d",
			tag,
		)
	}
	if len(outer) < 2 {
		return nil, errors.New(
			"StrictMaybe committee tag 1 but missing body",
		)
	}

	// SJust: decode the actual committee [members_map, quorum]
	fields, err := decodeRawArray(outer[1])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding committee body: %w", err,
		)
	}
	if len(fields) < 2 {
		return nil, fmt.Errorf(
			"committee body has %d elements, expected 2",
			len(fields),
		)
	}

	// Decode the committee map using decodeMapEntries to
	// handle credential array keys.
	entries, err := decodeMapEntries(fields[0])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding committee members map: %w", err,
		)
	}

	members := make(
		[]ParsedCommitteeMember, 0, len(entries),
	)
	var memberErrs []error
	for _, entry := range entries {
		cred, err := parseCredential(entry.KeyRaw)
		if err != nil {
			memberErrs = append(memberErrs, fmt.Errorf(
				"parsing committee credential: %w", err,
			))
			continue
		}

		var expiresEpoch uint64
		if _, err := cbor.Decode(
			entry.ValueRaw, &expiresEpoch,
		); err != nil {
			memberErrs = append(memberErrs, fmt.Errorf(
				"decoding committee expiry: %w", err,
			))
			continue
		}

		members = append(members, ParsedCommitteeMember{
			ColdCredential: cred,
			ExpiresEpoch:   expiresEpoch,
		})
	}

	// Return parsed members even if some failed
	return members, errors.Join(memberErrs...)
}

// parseProposals decodes governance proposals from CBOR.
// The proposals field is a Proposals container:
//
//	[omap_entries, roots, tree, children]
//
// where omap_entries is a sequence of GovActionState entries.
// Each GovActionState = [govActionId, committeeVotes,
// drepVotes, spoVotes, proposalProcedure, proposedIn,
// expiresAfter].
func parseProposals(data []byte) (
	[]ParsedGovProposal, error,
) {
	container, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding proposals container: %w", err,
		)
	}
	if len(container) == 0 {
		return nil, nil
	}

	// The proposals are in the first element of the
	// container (the OMap sequence).
	proposalSeq := container[0]

	// The OMap is encoded as a CBOR array of entries.
	// Each entry may be a GovActionState directly or
	// a [key, value] pair.
	items, err := decodeRawArray(proposalSeq)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding proposals OMap: %w", err,
		)
	}

	proposals := make([]ParsedGovProposal, 0, len(items))
	var propErrs []error
	for _, item := range items {
		prop, err := parseGovActionState(item)
		if err != nil {
			propErrs = append(propErrs, err)
			continue
		}
		if prop != nil {
			proposals = append(proposals, *prop)
		}
	}

	// Return parsed proposals even if some failed
	return proposals, errors.Join(propErrs...)
}

// parseGovActionState decodes a single GovActionState.
func parseGovActionState(
	data []byte,
) (*ParsedGovProposal, error) {
	fields, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding GovActionState: %w", err,
		)
	}
	if len(fields) < 7 {
		return nil, fmt.Errorf(
			"GovActionState has %d elements, "+
				"expected 7",
			len(fields),
		)
	}

	prop := &ParsedGovProposal{}

	// govActionId = [txHash, actionIndex]
	govId, err := decodeRawArray(fields[0])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding govActionId: %w", err,
		)
	}
	if len(govId) < 2 {
		return nil, fmt.Errorf(
			"govActionId has %d elements, expected 2",
			len(govId),
		)
	}
	if _, err := cbor.Decode(
		govId[0], &prop.TxHash,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding govActionId txHash: %w", err,
		)
	}
	var actionIdx uint32
	if _, err := cbor.Decode(
		govId[1], &actionIdx,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding govActionId index: %w", err,
		)
	}
	prop.ActionIndex = actionIdx

	// Skip vote maps (fields 1-3)

	// proposalProcedure = [deposit, returnAddr, govAction, anchor]
	procedure, err := decodeRawArray(fields[4])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding proposalProcedure: %w", err,
		)
	}
	if len(procedure) < 4 {
		return nil, fmt.Errorf(
			"proposalProcedure has %d elements, "+
				"expected 4",
			len(procedure),
		)
	}

	// Deposit
	if _, err := cbor.Decode(
		procedure[0], &prop.Deposit,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding proposal deposit: %w", err,
		)
	}

	// Return address
	if _, err := cbor.Decode(
		procedure[1], &prop.ReturnAddr,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding proposal return address: %w",
			err,
		)
	}

	// govAction = [actionType, ...] - extract actionType
	govAction, err := decodeRawArray(procedure[2])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding govAction: %w", err,
		)
	}
	if len(govAction) == 0 {
		return nil, errors.New(
			"govAction has 0 elements, expected at least 1",
		)
	}
	if _, err := cbor.Decode(
		govAction[0], &prop.ActionType,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding govAction type: %w", err,
		)
	}

	// anchor = [url, hash] — best-effort: proposals are still
	// useful for deposit tracking even without anchor metadata.
	anchorArr, err := decodeRawArray(procedure[3])
	if err == nil && len(anchorArr) >= 2 {
		var url string
		if _, err := cbor.Decode(
			anchorArr[0], &url,
		); err == nil {
			prop.AnchorUrl = url
		}
		var hash []byte
		if _, err := cbor.Decode(
			anchorArr[1], &hash,
		); err == nil {
			prop.AnchorHash = hash
		}
	}

	// proposedIn (epoch)
	if _, err := cbor.Decode(
		fields[5], &prop.ProposedIn,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding proposedIn: %w", err,
		)
	}

	// expiresAfter (epoch)
	if _, err := cbor.Decode(
		fields[6], &prop.ExpiresAfter,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding expiresAfter: %w", err,
		)
	}

	return prop, nil
}
