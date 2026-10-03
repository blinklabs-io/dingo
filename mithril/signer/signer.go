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

// Package signer runs a Mithril signer for a Cardano stake pool. It reuses the
// pool's KES key and operational certificate, registers an STM verification
// key with the aggregator each epoch and submits individual signatures of the
// Mithril stake distribution.
package signer

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/keystore"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultPollInterval = time.Minute
	defaultMinBackoff   = 5 * time.Second
	defaultMaxBackoff   = 5 * time.Minute
)

// ErrSlotUnavailable is wrapped by Config.Slot when the node cannot yet place
// the wall clock on the chain, for example while the ledger is still catching
// up. Loading defers the KES period check in that case instead of failing.
var ErrSlotUnavailable = errors.New("current slot unavailable")

// Config configures a Signer.
type Config struct {
	// KESKeyPath, OperationalCertPath and ColdVKeyPath are the pool's key
	// material, in cardano-cli format.
	KESKeyPath          string
	OperationalCertPath string
	ColdVKeyPath        string
	// STMKeyPath holds the signer's BLS key. It is created on first use.
	STMKeyPath string
	// Genesis supplies the KES period length and lifetime.
	Genesis *shelley.ShelleyGenesis
	// Client talks to the aggregator.
	Client *mithril.Client
	// Slot returns the wall-clock slot.
	Slot func() (uint64, error)
	// Ledger supplies the highest operational certificate counter seen on
	// chain for the pool.
	Ledger       forging.LedgerView
	Logger       *slog.Logger
	PromRegistry prometheus.Registerer
	// PollInterval is the wait after a successful round; MinBackoff and
	// MaxBackoff bound the exponential wait after a failed one. Zero selects
	// a default.
	PollInterval time.Duration
	MinBackoff   time.Duration
	MaxBackoff   time.Duration
}

// Signer registers with a Mithril aggregator and signs its rounds.
type Signer struct {
	cfg     Config
	creds   *forging.PoolCredentials
	opCert  *forging.OpCert
	stmKey  *mithril.STMSigningKey
	stmVK   *mithril.STMVerificationKey
	partyID string
	metrics *metrics

	progress *roundProgress
}

// New loads the pool's key material and the signer's STM key and checks them:
// the operational certificate must be valid and name the loaded cold and KES
// keys, its KES period must be current, and its counter must not be behind
// the chain's.
func New(cfg Config) (*Signer, error) {
	return newSigner(cfg, nil)
}

func newSigner(cfg Config, progress *roundProgress) (*Signer, error) {
	switch {
	case cfg.Client == nil:
		return nil, errors.New("aggregator client is required")
	case cfg.Genesis == nil:
		return nil, errors.New("shelley genesis is required")
	case cfg.Slot == nil:
		return nil, errors.New("slot source is required")
	case cfg.Ledger == nil:
		return nil, errors.New("ledger view is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = defaultMinBackoff
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = max(defaultMaxBackoff, cfg.MinBackoff)
	}
	if progress == nil {
		progress = &roundProgress{}
	}

	creds := forging.NewPoolCredentials()
	if err := creds.LoadKESFromFiles(
		cfg.KESKeyPath,
		cfg.OperationalCertPath,
	); err != nil {
		return nil, err
	}
	if err := creds.ValidateOpCert(); err != nil {
		return nil, fmt.Errorf("validate operational certificate: %w", err)
	}
	opCert := creds.GetOpCert()
	if opCert == nil {
		return nil, errors.New("operational certificate not loaded")
	}
	coldKey, err := bursa.LoadKeyFromFile(cfg.ColdVKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load cold verification key: %w", err)
	}
	if !bytes.Equal(coldKey.VKey, opCert.ColdVKey) {
		return nil, errors.New(
			"cold verification key does not match the operational certificate",
		)
	}
	stmKey, err := loadOrCreateSTMKey(cfg.STMKeyPath)
	if err != nil {
		return nil, err
	}
	stmVK, err := stmKey.VerificationKey()
	if err != nil {
		return nil, fmt.Errorf("derive STM verification key: %w", err)
	}
	s := &Signer{
		cfg:      cfg,
		creds:    creds,
		opCert:   opCert,
		stmKey:   stmKey,
		stmVK:    stmVK,
		partyID:  creds.GetPoolID().String(),
		metrics:  newMetrics(cfg.PromRegistry),
		progress: progress,
	}
	if err := s.validateCredentials(); err != nil {
		return nil, err
	}
	return s, nil
}

// PartyID returns the signer's identity on the aggregator, its pool ID.
func (s *Signer) PartyID() string {
	return s.partyID
}

// validateCredentials checks the operational certificate against the current
// KES period and the chain's counter. When the node cannot yet place the wall
// clock the period check is deferred to the first registration.
func (s *Signer) validateCredentials() error {
	slot, err := s.cfg.Slot()
	switch {
	case errors.Is(err, ErrSlotUnavailable):
		if _, _, err := s.creds.ValidateAgainstLedger(s.cfg.Ledger); err != nil {
			return fmt.Errorf("validate against ledger: %w", err)
		}
		s.cfg.Logger.Warn(
			"mithril signer: KES period check deferred until the current slot is known",
			"component",
			"mithril-signer",
			"error",
			err,
		)
		return nil
	case err != nil:
		return fmt.Errorf("compute current slot: %w", err)
	}
	if err := s.creds.ValidateKESPeriod(s.cfg.Genesis, slot); err != nil {
		return fmt.Errorf("validate KES period: %w", err)
	}
	if _, _, err := s.creds.ValidateAgainstLedger(s.cfg.Ledger); err != nil {
		return fmt.Errorf("validate against ledger: %w", err)
	}
	return nil
}

// Run registers and signs each epoch until ctx is cancelled. A failed round
// is retried with exponential backoff; Run returns nil on cancellation.
func (s *Signer) Run(ctx context.Context) error {
	var wait time.Duration
	backoff := s.cfg.MinBackoff
	for {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		s.metrics.rounds.Inc()
		err := s.round(ctx)
		// A round that fails because ctx was cancelled is a stop, not an
		// error.
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err == nil {
			backoff = s.cfg.MinBackoff
			wait = s.cfg.PollInterval
			continue
		}
		s.metrics.errors.Inc()
		s.cfg.Logger.Error(
			"mithril signer round failed",
			"component", "mithril-signer",
			"error", err,
			"retry_in", backoff,
		)
		wait = backoff
		backoff = min(backoff*2, s.cfg.MaxBackoff)
	}
}

// epochMark records the last epoch an action completed for.
type epochMark struct {
	epoch uint64
	set   bool
}

type roundProgress struct {
	mu         sync.Mutex
	registered epochMark
	signed     epochMark
}

func (p *roundProgress) isRegistered(epoch uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.registered.is(epoch)
}

func (p *roundProgress) markRegistered(epoch uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.registered.mark(epoch)
}

func (p *roundProgress) isSigned(epoch uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.signed.is(epoch)
}

func (p *roundProgress) markSigned(epoch uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.signed.mark(epoch)
}

func (m *epochMark) is(epoch uint64) bool { return m.set && m.epoch == epoch }

func (m *epochMark) mark(epoch uint64) { m.epoch, m.set = epoch, true }

// loadOrCreateSTMKey reads the hex-encoded STM signing key at path, creating
// it with owner-only permissions when absent.
func loadOrCreateSTMKey(path string) (*mithril.STMSigningKey, error) {
	f, err := os.Open(path) // #nosec G304 -- operator-configured key path
	switch {
	case errors.Is(err, os.ErrNotExist):
		return createSTMKey(path)
	case err != nil:
		return nil, fmt.Errorf("open STM key: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only handle
	if err := keystore.CheckOpenFilePermissions(f); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, 1024))
	if err != nil {
		return nil, fmt.Errorf("read STM key: %w", err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, fmt.Errorf("decode STM key %q: %w", path, err)
	}
	return mithril.STMSigningKeyFromBytes(raw)
}

func createSTMKey(path string) (*mithril.STMSigningKey, error) {
	key, err := mithril.NewSTMSigningKey()
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile( // #nosec G304 -- operator-configured key path
		path,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("create STM key: %w", err)
	}
	_, err = f.WriteString(hex.EncodeToString(key.Bytes()) + "\n")
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("write STM key: %w", err)
	}
	return key, nil
}
