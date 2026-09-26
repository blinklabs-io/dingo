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

package byronupdate

import (
	"fmt"
	"math/big"
)

// ProposalInvalidProposerError is Registration.InvalidProposer: the proposal
// issuer is not an active delegate.
type ProposalInvalidProposerError struct {
	Proposer KeyHash
}

func (e ProposalInvalidProposerError) Error() string {
	return "update proposal issuer " + e.Proposer.String() + " is not an active delegate"
}

// ProposalInvalidSignatureError is Registration.InvalidSignature.
type ProposalInvalidSignatureError struct {
	Err error
}

func (e ProposalInvalidSignatureError) Error() string {
	return fmt.Sprintf("update proposal signature is invalid: %v", e.Err)
}

func (e ProposalInvalidSignatureError) Unwrap() error {
	return e.Err
}

// ProposalNullUpdateError is Registration.NullUpdateProposal: the proposal
// changes neither the protocol nor the software version.
type ProposalNullUpdateError struct{}

func (ProposalNullUpdateError) Error() string {
	return "update proposal changes neither the protocol nor the software version"
}

// ProposalDuplicateProtocolVersionError is
// Registration.DuplicateProtocolVersion.
type ProposalDuplicateProtocolVersionError struct {
	Version ProtocolVersion
}

func (e ProposalDuplicateProtocolVersionError) Error() string {
	return fmt.Sprintf(
		"an update proposal for protocol version %d.%d.%d is already registered",
		e.Version.Major,
		e.Version.Minor,
		e.Version.Alt,
	)
}

// ProposalInvalidProtocolVersionError is
// Registration.InvalidProtocolVersion: the proposed version cannot follow
// the adopted one.
type ProposalInvalidProtocolVersionError struct {
	Version ProtocolVersion
	Adopted ProtocolVersion
}

func (e ProposalInvalidProtocolVersionError) Error() string {
	return fmt.Sprintf(
		"protocol version %d.%d.%d cannot follow adopted version %d.%d.%d",
		e.Version.Major, e.Version.Minor, e.Version.Alt,
		e.Adopted.Major, e.Adopted.Minor, e.Adopted.Alt,
	)
}

// ProposalTooLargeError is Registration.ProposalTooLarge.
type ProposalTooLargeError struct {
	Size *big.Int
	Max  *big.Int
}

func (e ProposalTooLargeError) Error() string {
	return fmt.Sprintf(
		"update proposal size %s exceeds maxProposalSize %s",
		e.Size, e.Max,
	)
}

// ProposalMaxBlockSizeTooLargeError is Registration.MaxBlockSizeTooLarge: a
// proposal may at most double the block size limit.
type ProposalMaxBlockSizeTooLargeError struct {
	Proposed *big.Int
	Adopted  *big.Int
}

func (e ProposalMaxBlockSizeTooLargeError) Error() string {
	return fmt.Sprintf(
		"proposed maxBlockSize %s exceeds twice the adopted %s",
		e.Proposed, e.Adopted,
	)
}

// ProposalMaxTxSizeTooLargeError is Registration.MaxTxSizeTooLarge: the
// transaction size limit must stay below the block size limit.
type ProposalMaxTxSizeTooLargeError struct {
	MaxTxSize    *big.Int
	MaxBlockSize *big.Int
}

func (e ProposalMaxTxSizeTooLargeError) Error() string {
	return fmt.Sprintf(
		"proposed maxTxSize %s is not below maxBlockSize %s",
		e.MaxTxSize, e.MaxBlockSize,
	)
}

// ProposalInvalidScriptVersionError is Registration.InvalidScriptVersion: the
// script version may only stay or rise by one.
type ProposalInvalidScriptVersionError struct {
	Adopted  uint16
	Proposed uint16
}

func (e ProposalInvalidScriptVersionError) Error() string {
	return fmt.Sprintf(
		"proposed script version %d cannot follow adopted %d",
		e.Proposed, e.Adopted,
	)
}

// ProposalDuplicateSoftwareVersionError is
// Registration.DuplicateSoftwareVersion.
type ProposalDuplicateSoftwareVersionError struct {
	AppName string
	Version uint32
}

func (e ProposalDuplicateSoftwareVersionError) Error() string {
	return fmt.Sprintf(
		"a software update for application %q is already registered",
		e.AppName,
	)
}

// ProposalInvalidApplicationNameError is Registration.SoftwareVersionError.
type ProposalInvalidApplicationNameError struct {
	AppName string
}

func (e ProposalInvalidApplicationNameError) Error() string {
	return fmt.Sprintf(
		"application name %q is longer than %d characters or not ASCII",
		e.AppName, applicationNameMaxLength,
	)
}

// ProposalInvalidSoftwareVersionError is
// Registration.InvalidSoftwareVersion: the version must be the next one for
// the application, or 0 or 1 for a new application.
type ProposalInvalidSoftwareVersionError struct {
	AppName string
	Version uint32
}

func (e ProposalInvalidSoftwareVersionError) Error() string {
	return fmt.Sprintf(
		"software version %d cannot follow the current version of %q",
		e.Version, e.AppName,
	)
}

// VoteProposalNotRegisteredError is Voting.VotingProposalNotRegistered.
type VoteProposalNotRegisteredError struct {
	UpId UpId
}

func (e VoteProposalNotRegisteredError) Error() string {
	return "update vote for unregistered proposal " + e.UpId.String()
}

// VoteVoterNotDelegateError is Voting.VotingVoterNotDelegate.
type VoteVoterNotDelegateError struct {
	Voter KeyHash
}

func (e VoteVoterNotDelegateError) Error() string {
	return "update voter " + e.Voter.String() + " is not an active delegate"
}

// VoteAlreadyCastError is Voting.VotingVoteAlreadyCast.
type VoteAlreadyCastError struct {
	Voter KeyHash
}

func (e VoteAlreadyCastError) Error() string {
	return "genesis key " + e.Voter.String() + " has already voted for this proposal"
}

// VoteInvalidSignatureError is Voting.VotingInvalidSignature.
type VoteInvalidSignatureError struct {
	Err error
}

func (e VoteInvalidSignatureError) Error() string {
	return fmt.Sprintf("update vote signature is invalid: %v", e.Err)
}

func (e VoteInvalidSignatureError) Unwrap() error {
	return e.Err
}

// EndorsementMultipleProposalsError is
// Endorsement.MultipleProposalsForProtocolVersion.
type EndorsementMultipleProposalsError struct {
	Version ProtocolVersion
}

func (e EndorsementMultipleProposalsError) Error() string {
	return fmt.Sprintf(
		"multiple update proposals for protocol version %d.%d.%d",
		e.Version.Major, e.Version.Minor, e.Version.Alt,
	)
}

// TransitionNotAdoptedError is a block leaving Byron in an epoch that no
// stable candidate for the next era's protocol version is adopted in.
type TransitionNotAdoptedError struct {
	Epoch uint64
	Major uint16
}

func (e TransitionNotAdoptedError) Error() string {
	return fmt.Sprintf(
		"byron epoch %d: the next era's first block arrived without a "+
			"stable protocol version %d update adopted in this epoch",
		e.Epoch, e.Major,
	)
}

// TransitionMissedError is a Byron block in or after the epoch a stable
// candidate for the next era's protocol version takes effect.
type TransitionMissedError struct {
	Epoch           uint64
	Major           uint16
	TransitionEpoch uint64
}

func (e TransitionMissedError) Error() string {
	return fmt.Sprintf(
		"byron epoch %d: a Byron block arrived after protocol version %d "+
			"was adopted in epoch %d",
		e.Epoch, e.Major, e.TransitionEpoch,
	)
}
