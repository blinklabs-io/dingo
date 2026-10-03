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

package signer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/mithril"
)

// round brings the signer up to date with the aggregator's current epoch: it
// registers once per epoch and signs the epoch's stake distribution once.
func (s *Signer) round(ctx context.Context) error {
	settings, err := s.cfg.Client.GetEpochSettings(ctx)
	if err != nil {
		return err
	}
	epoch := settings.Epoch
	if !s.registered.is(epoch) {
		if err := s.register(ctx, epoch); err != nil {
			return err
		}
		s.registered.mark(epoch)
	}
	if s.signed.is(epoch) {
		return nil
	}
	done, err := s.signStakeDistribution(ctx, settings)
	if err != nil {
		return err
	}
	if done {
		s.signed.mark(epoch)
	}
	return nil
}

// register submits the signer's STM verification key, bound to the pool by a
// KES signature at the current KES period. A key registered in epoch N signs
// from epoch N+2.
//
// The aggregator records a registration made in epoch N at epoch N+1, opens
// its registration round for that epoch, and rejects one naming any other.
func (s *Signer) register(ctx context.Context, epoch uint64) error {
	slot, err := s.cfg.Slot()
	if err != nil {
		return fmt.Errorf("compute current slot: %w", err)
	}
	if err := s.creds.ValidateKESPeriod(s.cfg.Genesis, slot); err != nil {
		return fmt.Errorf("validate KES period: %w", err)
	}
	period, err := forging.CurrentKESPeriodFromGenesis(s.cfg.Genesis, slot)
	if err != nil {
		return err
	}
	opCert := s.opCert
	if err := s.creds.UpdateKESPeriod(period); err != nil {
		return fmt.Errorf("evolve KES key: %w", err)
	}
	kesSignature, err := s.creds.KESSign(period, s.stmVK.Bytes())
	if err != nil {
		return fmt.Errorf("sign verification key: %w", err)
	}
	encodedKESSignature, err := mithril.EncodeKESSignature(kesSignature)
	if err != nil {
		return err
	}
	encodedOpCert, err := mithril.EncodeOperationalCertificate(
		opCert.KESVKey,
		opCert.IssueNumber,
		opCert.KESPeriod,
		opCert.Signature,
		opCert.ColdVKey,
	)
	if err != nil {
		return err
	}
	encodedVK, err := s.stmVK.Encode()
	if err != nil {
		return err
	}
	if err := s.cfg.Client.RegisterSigner(ctx, epoch+1, mithril.AggregatorSigner{
		PartyID:                  s.partyID,
		VerificationKey:          encodedVK,
		VerificationKeySignature: encodedKESSignature,
		OperationalCertificate:   encodedOpCert,
		KESPeriod:                period - opCert.KESPeriod,
	}); err != nil {
		return err
	}
	s.cfg.Logger.Info(
		"mithril signer registered",
		"component", "mithril-signer",
		"party_id", s.partyID,
		"epoch", epoch,
	)
	return nil
}

// signStakeDistribution signs the Mithril stake distribution of the epoch and
// submits the signature. It reports whether the epoch needs no further
// attempt: the signature was accepted, the signer has nothing to sign this
// epoch, or the aggregator will no longer take it.
func (s *Signer) signStakeDistribution(
	ctx context.Context,
	settings *mithril.EpochSettings,
) (bool, error) {
	epoch := settings.Epoch
	if epoch < 2 {
		return false, fmt.Errorf(
			"epoch %d precedes the first signing epoch",
			epoch,
		)
	}
	// Signers sign two epochs after registering, and the next epoch's
	// signers registered one epoch ago.
	current, err := s.closeRegistration(
		ctx,
		epoch-2,
		epoch,
		settings.CurrentSigners,
	)
	if err != nil {
		return false, err
	}
	if _, _, ok := current.SignerIndex(s.stmVK.VK); !ok {
		s.cfg.Logger.Info(
			"mithril signer not registered for this epoch, nothing to sign",
			"component", "mithril-signer",
			"party_id", s.partyID,
			"epoch", epoch,
		)
		return true, nil
	}
	next, err := s.closeRegistration(
		ctx,
		epoch-1,
		epoch+1,
		settings.NextSigners,
	)
	if err != nil {
		return false, err
	}
	nextAVK, err := next.AggregateVerificationKey()
	if err != nil {
		return false, err
	}
	params, err := s.cfg.Client.GetProtocolConfiguration(ctx, epoch)
	if err != nil {
		return false, err
	}
	nextParams, err := s.cfg.Client.GetProtocolConfiguration(ctx, epoch+1)
	if err != nil {
		return false, err
	}
	message := mithril.ProtocolMessage{MessageParts: map[string]string{
		"next_aggregate_verification_key": nextAVK,
		"next_protocol_parameters":        nextParams.ProtocolParameters.ComputeHash(),
		"current_epoch":                   strconv.FormatUint(epoch, 10),
	}}.ComputeHash()

	// The aggregator verifies the signature over the hash's text form.
	signature, err := s.stmKey.Sign(
		[]byte(message),
		current,
		params.ProtocolParameters,
	)
	if err != nil {
		return false, err
	}
	if len(signature.Indexes) == 0 {
		s.cfg.Logger.Info(
			"mithril signer won no lottery indexes, nothing to submit",
			"component", "mithril-signer",
			"epoch", epoch,
		)
		return true, nil
	}
	encodedSignature, err := signature.Encode()
	if err != nil {
		return false, err
	}
	err = s.cfg.Client.RegisterSingleSignature(
		ctx,
		mithril.SingleSignatureRegistration{
			EntityType:    mithril.MithrilStakeDistributionEntityType(epoch),
			PartyID:       s.partyID,
			Signature:     encodedSignature,
			Indexes:       signature.Indexes,
			SignedMessage: message,
		},
	)
	var statusErr *mithril.HTTPStatusError
	switch {
	case err == nil:
		s.metrics.signatures.Inc()
		s.cfg.Logger.Info(
			"mithril signer submitted signature",
			"component", "mithril-signer",
			"epoch", epoch,
			"indexes", len(signature.Indexes),
		)
		return true, nil
	case errors.As(err, &statusErr) &&
		statusErr.StatusCode == http.StatusNotFound:
		// The aggregator has not opened the round yet.
		return false, nil
	case errors.As(err, &statusErr) &&
		statusErr.StatusCode == http.StatusGone:
		s.cfg.Logger.Warn(
			"mithril signer signature came too late",
			"component", "mithril-signer",
			"epoch", epoch,
		)
		return true, nil
	default:
		return false, err
	}
}

// closeRegistration closes the registration made in registeredAt for signing
// in signingAt over the given signers, whose stake the aggregator publishes
// separately.
func (s *Signer) closeRegistration(
	ctx context.Context,
	registeredAt uint64,
	signingAt uint64,
	signers []mithril.AggregatorSigner,
) (*mithril.STMClosedRegistration, error) {
	registered, err := s.cfg.Client.GetRegisteredSigners(ctx, registeredAt)
	if err != nil {
		return nil, err
	}
	if registered.SigningAt != signingAt {
		return nil, fmt.Errorf(
			"signers registered at epoch %d sign at epoch %d, want %d",
			registeredAt,
			registered.SigningAt,
			signingAt,
		)
	}
	stakes := make(map[string]uint64, len(registered.Registrations))
	for _, registration := range registered.Registrations {
		stakes[registration.PartyID] = registration.Stake
	}
	parties := make([]mithril.MithrilStakeDistributionParty, 0, len(signers))
	for _, signer := range signers {
		stake, ok := stakes[signer.PartyID]
		if !ok {
			return nil, fmt.Errorf(
				"no stake published for signer %s at epoch %d",
				signer.PartyID,
				registeredAt,
			)
		}
		parties = append(parties, mithril.MithrilStakeDistributionParty{
			PartyID:         signer.PartyID,
			Stake:           stake,
			VerificationKey: signer.VerificationKey,
		})
	}
	return mithril.NewSTMClosedRegistration(parties)
}
