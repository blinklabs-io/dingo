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

package eras

import (
	"bytes"
	"slices"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// governanceLevel is one set of certificates and the votes that GOV checks
// after them.
type governanceLevel struct {
	certificates []lcommon.Certificate
	votes        lcommon.VotingProcedures
}

// governanceLevels returns a transaction's governance levels in application
// order: each Dijkstra sub-transaction, then the top-level transaction.
func governanceLevels(tx lcommon.Transaction) []governanceLevel {
	dijkstraTx, ok := tx.(*gdijkstra.DijkstraTransaction)
	if !ok || dijkstraTx == nil {
		return []governanceLevel{{
			certificates: tx.Certificates(),
			votes:        tx.VotingProcedures(),
		}}
	}
	subTxs := dijkstraTx.Body.TxSubTransactions.Items()
	levels := make([]governanceLevel, 0, len(subTxs)+1)
	for idx := range subTxs {
		body := &subTxs[idx].Body
		levels = append(levels, governanceLevel{
			certificates: body.Certificates(),
			votes:        body.VotingProcedures(),
		})
	}
	return append(levels, governanceLevel{
		certificates: dijkstraTx.Certificates(),
		votes:        dijkstraTx.VotingProcedures(),
	})
}

// voterOverlay answers voter existence after the certificates applied so far,
// deferring to the ledger state for anything they did not touch.
type voterOverlay struct {
	ls        lcommon.LedgerState
	committee CommitteeCredentialState
	voting    lcommon.CommitteeVotingState
	dreps     map[committeeCredentialKey]bool
	pools     map[lcommon.PoolKeyHash]struct{}
	// coldHot records, for each cold credential a certificate touched, the
	// hot credential it now authorizes, or nil after a resignation.
	coldHot map[committeeCredentialKey]*committeeCredentialKey

	availabilityKnown bool
	available         bool
}

func newVoterOverlay(
	ls lcommon.LedgerState,
	committee CommitteeCredentialState,
) *voterOverlay {
	voting, _ := ls.(lcommon.CommitteeVotingState)
	return &voterOverlay{
		ls:        ls,
		committee: committee,
		voting:    voting,
		dreps:     make(map[committeeCredentialKey]bool),
		pools:     make(map[lcommon.PoolKeyHash]struct{}),
		coldHot:   make(map[committeeCredentialKey]*committeeCredentialKey),
	}
}

func (o *voterOverlay) applyCertificates(certificates []lcommon.Certificate) {
	for _, certificate := range certificates {
		switch cert := certificate.(type) {
		case *lcommon.RegistrationDrepCertificate:
			o.dreps[committeeCredentialKeyFor(cert.DrepCredential)] = true
		case *lcommon.DeregistrationDrepCertificate:
			o.dreps[committeeCredentialKeyFor(cert.DrepCredential)] = false
		case *lcommon.PoolRegistrationCertificate:
			o.pools[cert.Operator] = struct{}{}
		case *lcommon.AuthCommitteeHotCertificate:
			hot := committeeCredentialKeyFor(cert.HotCredential)
			o.coldHot[committeeCredentialKeyFor(cert.ColdCredential)] = &hot
		case *lcommon.ResignCommitteeColdCertificate:
			o.coldHot[committeeCredentialKeyFor(cert.ColdCredential)] = nil
		}
	}
}

func (o *voterOverlay) validateVoters(votes lcommon.VotingProcedures) error {
	voters := make([]*lcommon.Voter, 0, len(votes))
	for voter := range votes {
		if voter != nil {
			voters = append(voters, voter)
		}
	}
	// Sorted so the reported voter does not depend on map iteration.
	slices.SortFunc(voters, func(a, b *lcommon.Voter) int {
		if a.Type != b.Type {
			return int(a.Type) - int(b.Type)
		}
		return bytes.Compare(a.Hash[:], b.Hash[:])
	})
	for _, voter := range voters {
		known, err := o.voterKnown(voter)
		if err != nil {
			return err
		}
		if !known {
			return conway.UnknownVoterError{Voter: *voter}
		}
	}
	return nil
}

func voterCredential(
	voter *lcommon.Voter,
	scriptType uint8,
) lcommon.Credential {
	credentialType := uint(lcommon.CredentialTypeAddrKeyHash)
	if voter.Type == scriptType {
		credentialType = lcommon.CredentialTypeScriptHash
	}
	return lcommon.Credential{
		CredType:   credentialType,
		Credential: lcommon.Blake2b224(voter.Hash),
	}
}

func (o *voterOverlay) voterKnown(voter *lcommon.Voter) (bool, error) {
	switch voter.Type {
	case lcommon.VoterTypeDRepKeyHash, lcommon.VoterTypeDRepScriptHash:
		credential := voterCredential(voter, lcommon.VoterTypeDRepScriptHash)
		if registered, ok := o.dreps[committeeCredentialKeyFor(credential)]; ok {
			return registered, nil
		}
		registration, err := o.ls.DRepRegistration(credential)
		if err != nil {
			return false, err
		}
		return registration != nil, nil
	case lcommon.VoterTypeStakingPoolKeyHash:
		pool := lcommon.PoolKeyHash(voter.Hash)
		if _, ok := o.pools[pool]; ok {
			return true, nil
		}
		return o.ls.IsPoolRegistered(pool), nil
	case lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		lcommon.VoterTypeConstitutionalCommitteeHotScriptHash:
		hot := voterCredential(
			voter,
			lcommon.VoterTypeConstitutionalCommitteeHotScriptHash,
		)
		known, err := o.committeeHotKnown(hot)
		if err != nil {
			// A failed lookup is never a known voter: fail closed.
			return false, conway.CommitteeMemberLookupError{
				Credential: hot.Credential,
				Err:        err,
			}
		}
		return known, nil
	default:
		return false, nil
	}
}

// committeeHotKnown reports whether any cold credential authorizes hot after
// the applied certificates. When the committee state is not authoritative
// (see ledger.LedgerView.CommitteeStateAvailable), a hot credential that no
// applied certificate authorizes cannot be shown unknown and is accepted.
func (o *voterOverlay) committeeHotKnown(hot lcommon.Credential) (bool, error) {
	hotKey := committeeCredentialKeyFor(hot)
	for _, current := range o.coldHot {
		if current != nil && *current == hotKey {
			return true, nil
		}
	}
	if !o.availabilityKnown {
		available, err := o.committee.CommitteeStateAvailable()
		if err != nil {
			return false, err
		}
		o.available, o.availabilityKnown = available, true
	}
	if !o.available {
		return true, nil
	}
	if o.voting != nil {
		coldCredentials, err := o.voting.CommitteeHotCredentialColdCredentials(
			hot,
		)
		if err != nil {
			return false, err
		}
		for _, cold := range coldCredentials {
			if _, touched := o.coldHot[committeeCredentialKeyFor(cold)]; !touched {
				return true, nil
			}
		}
		return false, nil
	}
	member, err := o.committee.CommitteeHotCredentialMember(hot)
	if err != nil {
		return false, err
	}
	if member == nil || member.Resigned {
		return false, nil
	}
	// A ledger state without CommitteeVotingState names one witness by bare
	// cold hash, so a touched cold credential with that hash, of either
	// type, makes the witness stale.
	for cold := range o.coldHot {
		if cold.credential == member.ColdKey {
			return false, nil
		}
	}
	return true, nil
}
