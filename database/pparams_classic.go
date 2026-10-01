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

package database

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// classicPParamProposers counts the distinct genesis keys with a stored
// proposal for submissionEpoch. At least quorum of them is necessary for any
// single update value to reach quorum, so a count below quorum proves nothing
// will be enacted without decoding a proposal.
func classicPParamProposers(
	rows []models.PParamUpdate,
	submissionEpoch uint64,
) int {
	proposers := make(map[string]struct{})
	for i := range rows {
		if rows[i].Epoch == submissionEpoch {
			proposers[string(rows[i].GenesisHash)] = struct{}{}
		}
	}
	return len(proposers)
}

// classicPParamEnactment is the update chosen for enactment at an epoch
// boundary, with the number of genesis keys that voted for it.
type classicPParamEnactment struct {
	update any
	votes  int
}

type classicPParamVote struct {
	update    any
	decodeErr error
	votes     int
}

// selectClassicPParamUpdate returns the proposed update the Shelley update
// system enacts at the boundary into enactEpoch, or nil when none is enacted.
// It follows the reference votedFuturePParams and updatePpup
// (Cardano.Ledger.Shelley.Rules.Ppup and Rules.Newpp):
//
//   - a proposal counts only for the epoch it targets, so the proposals for
//     the boundary into enactEpoch are those targeting enactEpoch-1;
//   - each genesis key has one vote, its latest proposal for that epoch, in
//     chain order (slot, then insertion order within a slot);
//   - proposals made during the previous epoch, after its slot of no return,
//     carry over into the submission epoch only if every one of them has a
//     protocol version that can follow the submission epoch's parameters;
//     otherwise all of them are discarded;
//   - votes are grouped by update value, not by encoding, and exactly one
//     value must reach quorum: none, or two values that both reach it, enact
//     nothing;
//   - the agreed update is enacted only if the resulting parameters keep
//     maxTxSize + maxBlockHeaderSize below maxBlockBodySize.
//
// submissionEpochStart is the first slot of the submission epoch; proposals
// stored with an earlier slot are the carried-over ones. currentPParams are
// the submission epoch's parameters.
func selectClassicPParamUpdate(
	rows []models.PParamUpdate,
	enactEpoch uint64,
	submissionEpochStart uint64,
	quorum int,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
) (*classicPParamEnactment, error) {
	if enactEpoch == 0 {
		return nil, nil
	}
	submissionEpoch := enactEpoch - 1
	proposals := make([]models.PParamUpdate, 0, len(rows))
	for i := range rows {
		if rows[i].Epoch == submissionEpoch {
			proposals = append(proposals, rows[i])
		}
	}
	if len(proposals) == 0 {
		return nil, nil
	}
	// Newest first, so the first proposal seen for a genesis key is its vote.
	slices.SortStableFunc(proposals, func(a, b models.PParamUpdate) int {
		if c := cmp.Compare(b.AddedSlot, a.AddedSlot); c != 0 {
			return c
		}
		return cmp.Compare(b.ID, a.ID)
	})

	keepCarriedOver, err := classicCarriedOverProposalsFollow(
		proposals,
		submissionEpochStart,
		currentPParams,
		decodeFunc,
	)
	if err != nil {
		return nil, err
	}

	voted := make(map[string]struct{})
	votes := make(map[string]*classicPParamVote)
	for i := range proposals {
		if !keepCarriedOver && proposals[i].AddedSlot < submissionEpochStart {
			continue
		}
		genesis := string(proposals[i].GenesisHash)
		if _, ok := voted[genesis]; ok {
			continue
		}
		voted[genesis] = struct{}{}
		update, decodeErr := decodeFunc(proposals[i].Cbor)
		identity := "raw:" + string(proposals[i].Cbor)
		if decodeErr == nil {
			if encoded, encodeErr := classicPParamUpdateIdentity(
				update,
			); encodeErr == nil {
				identity = "value:" + encoded
			}
		}
		vote, ok := votes[identity]
		if !ok {
			vote = &classicPParamVote{update: update, decodeErr: decodeErr}
			votes[identity] = vote
		}
		vote.votes++
	}

	var agreed *classicPParamVote
	for _, vote := range votes {
		if vote.votes < quorum {
			continue
		}
		if agreed != nil {
			return nil, nil
		}
		agreed = vote
	}
	if agreed == nil {
		return nil, nil
	}
	if agreed.decodeErr != nil {
		return nil, fmt.Errorf("decode pparam update: %w", agreed.decodeErr)
	}
	if !classicUpdateKeepsBlockSizes(currentPParams, agreed.update) {
		return nil, nil
	}
	return &classicPParamEnactment{
		update: agreed.update,
		votes:  agreed.votes,
	}, nil
}

// classicCarriedOverProposalsFollow reports whether the proposals made before
// submissionEpochStart survive the boundary into the submission epoch. The
// reference keeps them only when all of them, each genesis key's latest one,
// have a protocol version that can follow the new parameters.
func classicCarriedOverProposalsFollow(
	newestFirst []models.PParamUpdate,
	submissionEpochStart uint64,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
) (bool, error) {
	seen := make(map[string]struct{})
	for i := range newestFirst {
		if newestFirst[i].AddedSlot >= submissionEpochStart {
			continue
		}
		genesis := string(newestFirst[i].GenesisHash)
		if _, ok := seen[genesis]; ok {
			continue
		}
		seen[genesis] = struct{}{}
		update, err := decodeFunc(newestFirst[i].Cbor)
		if err != nil {
			return false, fmt.Errorf(
				"decode carried-over pparam update: %w",
				err,
			)
		}
		provider, ok := update.(lcommon.ProtocolParameterVersionUpdateProvider)
		if !ok {
			continue
		}
		proposed := provider.ProtocolParameterVersionUpdate()
		if proposed == nil {
			continue
		}
		current, ok := currentPParams.(lcommon.ProtocolParametersProtocolVersionProvider)
		if !ok {
			return false, errors.New(
				"protocol parameters do not expose a protocol version",
			)
		}
		if !classicProtocolVersionCanFollow(
			current.ProtocolParametersProtocolVersion(),
			*proposed,
		) {
			return false, nil
		}
	}
	return true, nil
}

// classicProtocolVersionCanFollow is the reference pvCanFollow: a major
// increment resetting the minor version, or a minor increment.
func classicProtocolVersionCanFollow(
	current lcommon.ProtocolParametersProtocolVersion,
	proposed lcommon.ProtocolParametersProtocolVersion,
) bool {
	majorIncrement := current.Major < math.MaxUint &&
		proposed.Major == current.Major+1 && proposed.Minor == 0
	minorIncrement := proposed.Major == current.Major &&
		current.Minor < math.MaxUint && proposed.Minor == current.Minor+1
	return majorIncrement || minorIncrement
}

// classicPParamUpdateIdentity encodes a decoded update from its field values,
// ignoring the CBOR it was decoded from, so two encodings of the same update
// share an identity. The reference compares PParamsUpdate values, not bytes.
func classicPParamUpdateIdentity(update any) (string, error) {
	value := reflect.ValueOf(update)
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "", errors.New("nil pparam update")
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return "", fmt.Errorf("unsupported pparam update type %T", update)
	}
	copied := reflect.New(value.Type())
	copied.Elem().Set(value)
	encoded, err := cbor.EncodeGeneric(copied.Interface())
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// classicUpdateKeepsBlockSizes applies the reference votedFuturePParams guard
// that the updated parameters keep maxTxSize + maxBlockHeaderSize strictly
// below maxBlockBodySize. It reads the three sizes directly, before the era
// update function mutates anything. Parameter or update types outside the
// Shelley-family update system carry no such guard.
func classicUpdateKeepsBlockSizes(
	currentPParams lcommon.ProtocolParameters,
	update any,
) bool {
	txSize, headerSize, bodySize, ok := classicPParamBlockSizes(currentPParams)
	if !ok {
		return true
	}
	newTx, newHeader, newBody, ok := classicUpdateBlockSizes(update)
	if !ok {
		return true
	}
	if newTx != nil {
		txSize = *newTx
	}
	if newHeader != nil {
		headerSize = *newHeader
	}
	if newBody != nil {
		bodySize = *newBody
	}
	sum := uint64(txSize) + uint64(headerSize)
	return sum >= uint64(txSize) && sum < uint64(bodySize)
}

func classicPParamBlockSizes(
	pp lcommon.ProtocolParameters,
) (uint, uint, uint, bool) {
	switch p := pp.(type) {
	case *shelley.ShelleyProtocolParameters:
		return p.MaxTxSize, p.MaxBlockHeaderSize, p.MaxBlockBodySize, true
	case *mary.MaryProtocolParameters:
		return p.MaxTxSize, p.MaxBlockHeaderSize, p.MaxBlockBodySize, true
	case *alonzo.AlonzoProtocolParameters:
		return p.MaxTxSize, p.MaxBlockHeaderSize, p.MaxBlockBodySize, true
	case *babbage.BabbageProtocolParameters:
		return p.MaxTxSize, p.MaxBlockHeaderSize, p.MaxBlockBodySize, true
	}
	return 0, 0, 0, false
}

func classicUpdateBlockSizes(update any) (*uint, *uint, *uint, bool) {
	switch u := update.(type) {
	case shelley.ShelleyProtocolParameterUpdate:
		return u.MaxTxSize, u.MaxBlockHeaderSize, u.MaxBlockBodySize, true
	case *shelley.ShelleyProtocolParameterUpdate:
		return u.MaxTxSize, u.MaxBlockHeaderSize, u.MaxBlockBodySize, true
	case mary.MaryProtocolParameterUpdate:
		return u.MaxTxSize, u.MaxBlockHeaderSize, u.MaxBlockBodySize, true
	case *mary.MaryProtocolParameterUpdate:
		return u.MaxTxSize, u.MaxBlockHeaderSize, u.MaxBlockBodySize, true
	case alonzo.AlonzoProtocolParameterUpdate:
		return u.MaxTxSize, u.MaxBlockHeaderSize, u.MaxBlockBodySize, true
	case *alonzo.AlonzoProtocolParameterUpdate:
		return u.MaxTxSize, u.MaxBlockHeaderSize, u.MaxBlockBodySize, true
	case babbage.BabbageProtocolParameterUpdate:
		return u.MaxTxSize, u.MaxBlockHeaderSize, u.MaxBlockBodySize, true
	case *babbage.BabbageProtocolParameterUpdate:
		return u.MaxTxSize, u.MaxBlockHeaderSize, u.MaxBlockBodySize, true
	}
	return nil, nil, nil, false
}

// classicSubmissionEpochStart returns the first slot of the epoch whose
// proposals are enacted at the boundary into enactEpoch. The start only tells
// carried-over proposals from proposals made during that epoch, and only a
// protocol version update can make a carried-over proposal illegal, so the
// epoch record is read only when a proposal for the epoch carries one.
// Otherwise it returns 0, which classifies every proposal as current.
func (d *Database) classicSubmissionEpochStart(
	rows []models.PParamUpdate,
	enactEpoch uint64,
	decodeFunc func([]byte) (any, error),
	txn types.Txn,
) (uint64, error) {
	// Epoch 0 has no previous epoch to carry proposals over from.
	if enactEpoch <= 1 {
		return 0, nil
	}
	versionUpdate := false
	for i := range rows {
		if rows[i].Epoch != enactEpoch-1 {
			continue
		}
		update, err := decodeFunc(rows[i].Cbor)
		if err != nil {
			// An undecodable proposal cannot be checked for a version
			// update, so its classification needs the epoch start.
			versionUpdate = true
			break
		}
		provider, ok := update.(lcommon.ProtocolParameterVersionUpdateProvider)
		if ok && provider.ProtocolParameterVersionUpdate() != nil {
			versionUpdate = true
			break
		}
	}
	if !versionUpdate {
		return 0, nil
	}
	epoch, err := d.metadata.GetEpoch(enactEpoch-1, txn)
	if err != nil {
		return 0, fmt.Errorf("get epoch %d: %w", enactEpoch-1, err)
	}
	if epoch == nil {
		return 0, fmt.Errorf(
			"epoch %d not found for its pparam update proposals",
			enactEpoch-1,
		)
	}
	return epoch.StartSlot, nil
}
