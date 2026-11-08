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

package kesagent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	// AttestedProtocolID is the protocol a bursa KES agent's remote sign
	// listener reports in its Hello.
	AttestedProtocolID = "bursa-attested-kes/1"
	// ProviderMTLS is the attestation provider of an agent authenticated by
	// mutual TLS alone: its attestation_response carries no evidence, and the
	// client authenticates the agent by its pinned server key.
	ProviderMTLS = "mtls"

	// DefaultSessionFreshFor is how long after its last frame an admitted
	// session is reused. The agent closes a sign session that sends no new
	// request for 10 seconds, so a session older than this is redialled
	// rather than risk a request on a connection the agent is about to drop.
	DefaultSessionFreshFor = 8 * time.Second

	attestationNonceSize = 32
	// maxAttestationFrameLen bounds the attestation_response. It carries the
	// operational certificate and, for an enclave provider, its evidence.
	maxAttestationFrameLen = 1 << 20
)

// ErrServerPinMismatch is returned when the remote agent's TLS server key does
// not match the configured pin.
var ErrServerPinMismatch = errors.New(
	"kesagent: remote agent server key does not match the configured pin",
)

// ErrRemoteAdmission is returned when the remote agent's attestation_response
// does not admit the session.
var ErrRemoteAdmission = errors.New("kesagent: remote agent admission failed")

// RemoteConfig configures a RemoteClient.
type RemoteConfig struct {
	// Address is the agent's remote sign listener, host:port.
	Address string
	// ClientCertFile and ClientKeyFile are this node's TLS client
	// certificate and key, which the agent authenticates.
	ClientCertFile string
	ClientKeyFile  string
	// ServerSPKISHA256 is the hex SHA-256 of the agent's TLS server
	// certificate SubjectPublicKeyInfo. It is required: the agent is
	// authenticated by this pin, not by any certificate authority.
	ServerSPKISHA256 string
	// Provider is the attestation provider the agent must report. Only
	// ProviderMTLS is supported; empty means ProviderMTLS.
	Provider string
	// KESVKey and OpCertStartPeriod come from the local operational
	// certificate. The agent must report this KES key at admission, and
	// every signature is verified against it.
	KESVKey           []byte
	OpCertStartPeriod uint64
	// OpCert is the local operational certificate's CBOR. The agent must
	// report this exact certificate at admission; the node has already
	// validated its cold-key signature.
	OpCert []byte
	// HelloTimeout bounds the dial, TLS handshake and admission. Zero uses
	// DefaultHelloTimeout.
	HelloTimeout time.Duration
	// SignTimeout bounds one sign round trip, including a reconnect when no
	// session is open. Zero uses DefaultSignTimeout.
	SignTimeout time.Duration
	// SessionFreshFor is how long after its last frame a session is reused.
	// Zero uses DefaultSessionFreshFor. It must stay below the agent's
	// 10-second bound on the wait for the next request.
	SessionFreshFor time.Duration

	Logger  *slog.Logger
	Metrics *Metrics
}

// RemoteClient signs block headers through a bursa KES agent's remote sign
// listener (AttestedProtocolID) over mutually authenticated TLS 1.3. The
// forger calls Prepare shortly before each leader slot, so the dial, the TLS
// handshake and the admission exchange happen before the slot and Sign finds
// an admitted session; Sign reuses a session while it is fresh and otherwise
// opens one itself. A sign request is sent at most once per Sign call: a
// failure after it may have reached the agent is returned, never resent on a
// new session. It is safe for concurrent use.
type RemoteClient struct {
	cfg       RemoteConfig
	logger    *slog.Logger
	tlsConfig *tls.Config
	pin       []byte

	mu       sync.Mutex
	conn     net.Conn
	lastUsed time.Time
	closed   bool

	backoff       time.Duration
	nextDialAfter time.Time
}

// NewRemoteClient validates cfg and loads the client certificate. It does not
// dial: the first Prepare, CheckReady or Sign does.
func NewRemoteClient(cfg RemoteConfig) (*RemoteClient, error) {
	if cfg.Address == "" {
		return nil, errors.New("kesagent: remote agent address is required")
	}
	if _, _, err := net.SplitHostPort(cfg.Address); err != nil {
		return nil, fmt.Errorf(
			"kesagent: remote agent address %q: %w",
			cfg.Address,
			err,
		)
	}
	pin, err := parseSPKIPin(cfg.ServerSPKISHA256)
	if err != nil {
		return nil, err
	}
	switch cfg.Provider {
	case "":
		cfg.Provider = ProviderMTLS
	case ProviderMTLS:
	default:
		return nil, fmt.Errorf(
			"kesagent: unsupported remote agent provider %q",
			cfg.Provider,
		)
	}
	if len(cfg.KESVKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf(
			"kesagent: remote agent requires a %d-byte KESVKey, got %d",
			ed25519.PublicKeySize,
			len(cfg.KESVKey),
		)
	}
	if len(cfg.OpCert) == 0 {
		return nil, errors.New(
			"kesagent: remote agent requires the local operational certificate",
		)
	}
	if cfg.ClientCertFile == "" || cfg.ClientKeyFile == "" {
		return nil, errors.New(
			"kesagent: remote agent requires a client certificate and key",
		)
	}
	cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
	if err != nil {
		return nil, fmt.Errorf("kesagent: load client certificate: %w", err)
	}
	if cfg.HelloTimeout <= 0 {
		cfg.HelloTimeout = DefaultHelloTimeout
	}
	if cfg.SignTimeout <= 0 {
		cfg.SignTimeout = DefaultSignTimeout
	}
	if cfg.SessionFreshFor <= 0 {
		cfg.SessionFreshFor = DefaultSessionFreshFor
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &RemoteClient{
		cfg:    cfg,
		logger: cfg.Logger,
		pin:    pin,
	}
	c.tlsConfig = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		// The agent is authenticated by its pinned server key in
		// VerifyConnection, not by a certificate authority or host name.
		InsecureSkipVerify: true, //nolint:gosec // replaced by the SPKI pin
		VerifyConnection:   c.verifyServerPin,
	}
	return c, nil
}

// parseSPKIPin decodes a hex SHA-256 SubjectPublicKeyInfo pin.
func parseSPKIPin(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, errors.New(
			"kesagent: remote agent requires a server SPKI SHA-256 pin",
		)
	}
	pin, err := hex.DecodeString(s)
	if err != nil || len(pin) != sha256.Size {
		return nil, fmt.Errorf(
			"kesagent: server SPKI pin must be %d hex characters",
			2*sha256.Size,
		)
	}
	return pin, nil
}

// SPKIFingerprint is the hex SHA-256 of a certificate's
// SubjectPublicKeyInfo, the form ServerSPKISHA256 takes.
func SPKIFingerprint(rawSubjectPublicKeyInfo []byte) string {
	sum := sha256.Sum256(rawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

func (c *RemoteClient) verifyServerPin(cs tls.ConnectionState) error {
	if cs.Version != tls.VersionTLS13 {
		return errors.New("kesagent: remote agent did not negotiate TLS 1.3")
	}
	if len(cs.PeerCertificates) == 0 {
		return ErrServerPinMismatch
	}
	sum := sha256.Sum256(cs.PeerCertificates[0].RawSubjectPublicKeyInfo)
	if subtle.ConstantTimeCompare(sum[:], c.pin) != 1 {
		return ErrServerPinMismatch
	}
	return nil
}

// Prepare opens and admits a session ahead of a leader slot, unless a fresh
// one is already open, so that Sign does not pay for the dial, handshake and
// admission inside its timeout. A failure here is reported early, before the
// slot; Sign retries the connection itself.
func (c *RemoteClient) Prepare() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(
		context.Background(),
		c.cfg.HelloTimeout,
	)
	defer cancel()
	return c.freshSessionLocked(ctx)
}

// CheckReady verifies that the agent is reachable and admits this node. It
// keeps the admitted session for a following Sign and never requests a
// signature.
func (c *RemoteClient) CheckReady() error {
	return c.Prepare()
}

// freshSessionLocked leaves an admitted session open that was last used
// within SessionFreshFor, dropping and redialling an older one. Caller holds
// c.mu.
func (c *RemoteClient) freshSessionLocked(ctx context.Context) error {
	if c.closed {
		return ErrClosed
	}
	if c.conn != nil && time.Since(c.lastUsed) >= c.cfg.SessionFreshFor {
		c.dropLocked()
	}
	return c.connectLocked(ctx)
}

// connectLocked dials, completes the pinned TLS 1.3 handshake and the
// admission exchange when no session is open. Caller holds c.mu.
func (c *RemoteClient) connectLocked(ctx context.Context) error {
	if c.closed {
		return ErrClosed
	}
	if c.conn != nil {
		return nil
	}
	if !c.nextDialAfter.IsZero() && time.Now().Before(c.nextDialAfter) {
		return fmt.Errorf(
			"kesagent: remote agent reconnect backoff active until %s",
			c.nextDialAfter.Format(time.RFC3339),
		)
	}
	conn, err := c.admit(ctx)
	if err != nil {
		c.recordDialFailureLocked()
		return err
	}
	c.conn = conn
	c.lastUsed = time.Now()
	c.backoff = 0
	c.nextDialAfter = time.Time{}
	return nil
}

// admit opens one admitted session: TLS 1.3 with the pinned server key, the
// agent's Hello, and an attestation exchange bound to a fresh nonce.
func (c *RemoteClient) admit(ctx context.Context) (net.Conn, error) {
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{},
		Config:    c.tlsConfig,
	}
	raw, err := dialer.DialContext(ctx, "tcp", c.cfg.Address)
	if err != nil {
		return nil, fmt.Errorf(
			"kesagent: dial remote agent %s: %w",
			c.cfg.Address,
			err,
		)
	}
	conn := raw
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()
	deadline := time.Now().Add(c.cfg.HelloTimeout)
	if d, has := ctx.Deadline(); has && d.Before(deadline) {
		deadline = d
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("kesagent: set admission deadline: %w", err)
	}
	var hello Hello
	if err := readFrame(conn, MaxSignFrameLen, &hello); err != nil {
		return nil, fmt.Errorf("kesagent: read remote agent hello: %w", err)
	}
	if hello.Protocol != AttestedProtocolID {
		return nil, fmt.Errorf("%w: got %q, want %q",
			ErrWrongProtocol, hello.Protocol, AttestedProtocolID)
	}
	if hello.Mode != ModeSign {
		return nil, fmt.Errorf("%w: got %q, want %q",
			ErrWrongMode, hello.Mode, ModeSign)
	}
	nonce := make([]byte, attestationNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("kesagent: admission nonce: %w", err)
	}
	if err := writeFrame(conn, MaxSignFrameLen, attestationRequest{
		Type:     "attestation_request",
		Protocol: AttestedProtocolID,
		Nonce:    nonce,
	}); err != nil {
		return nil, fmt.Errorf("kesagent: send attestation request: %w", err)
	}
	var resp attestationResponse
	if err := readFrame(conn, maxAttestationFrameLen, &resp); err != nil {
		return nil, fmt.Errorf("kesagent: read attestation response: %w", err)
	}
	if err := c.checkAdmission(nonce, resp); err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("kesagent: clear admission deadline: %w", err)
	}
	ok = true
	return conn, nil
}

// attestationRequest and attestationResponse mirror the attested listener's
// admission frames in blinklabs-io/bursa internal/kesagent.
type attestationRequest struct {
	Type     string `json:"type"`
	Protocol string `json:"protocol"`
	Nonce    []byte `json:"nonce"`
}

type attestationResponse struct {
	Type        string `json:"type"`
	Protocol    string `json:"protocol"`
	Nonce       []byte `json:"nonce"`
	Provider    string `json:"provider"`
	KESVKey     []byte `json:"kes_vkey,omitempty"`
	OpCert      []byte `json:"opcert,omitempty"`
	Period      uint64 `json:"period,omitempty"`
	Attestation []byte `json:"attestation,omitempty"`
	Error       string `json:"error,omitempty"`
}

// checkAdmission accepts an mtls attestation_response for this node's KES
// key: the echoed nonce, protocol and provider match, the evidence is absent
// (mtls carries none, and evidence would mean another provider), the agent's
// operational certificate is byte-equal to the local one, and its active KES
// key is that certificate's.
func (c *RemoteClient) checkAdmission(
	nonce []byte,
	resp attestationResponse,
) error {
	switch {
	case resp.Error != "":
		return fmt.Errorf(
			"%w: agent reported %q",
			ErrRemoteAdmission,
			resp.Error,
		)
	case resp.Type != "attestation_response":
		return fmt.Errorf("%w: frame type %q", ErrRemoteAdmission, resp.Type)
	case resp.Protocol != AttestedProtocolID:
		return fmt.Errorf("%w: protocol %q", ErrRemoteAdmission, resp.Protocol)
	case subtle.ConstantTimeCompare(resp.Nonce, nonce) != 1:
		return fmt.Errorf("%w: nonce does not match", ErrRemoteAdmission)
	case resp.Provider != c.cfg.Provider:
		return fmt.Errorf("%w: provider %q, want %q",
			ErrRemoteAdmission, resp.Provider, c.cfg.Provider)
	case len(resp.Attestation) != 0:
		return fmt.Errorf("%w: unexpected evidence for provider %q",
			ErrRemoteAdmission, c.cfg.Provider)
	case !bytes.Equal(resp.OpCert, c.cfg.OpCert):
		return fmt.Errorf(
			"%w: agent's operational certificate is not the local one",
			ErrRemoteAdmission,
		)
	case !bytes.Equal(resp.KESVKey, c.cfg.KESVKey):
		return fmt.Errorf(
			"%w: agent's KES key is not the local operational certificate's",
			ErrRemoteAdmission,
		)
	case resp.Period < c.cfg.OpCertStartPeriod:
		return fmt.Errorf(
			"%w: agent period %d precedes the opcert start period %d",
			ErrRemoteAdmission, resp.Period, c.cfg.OpCertStartPeriod,
		)
	}
	return nil
}

// Sign implements ledger/forging.RemoteKESSigner over the remote session. It
// reuses the open session, or opens one within SignTimeout, sends the request
// once and verifies the signature against the local operational
// certificate's KES key. A transport or verification failure drops the
// session, and the next Prepare or Sign redials; an agent refusal keeps it.
func (c *RemoteClient) Sign(
	period uint64,
	message []byte,
) (_ []byte, err error) {
	if len(message) > MaxSignFrameLen/2 {
		return nil, fmt.Errorf(
			"kesagent: sign message is %d bytes, too large for a sign request",
			len(message),
		)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	defer func() {
		if err != nil {
			c.cfg.Metrics.incSignFailure()
		}
	}()
	ctx, cancel := context.WithTimeout(
		context.Background(),
		c.cfg.SignTimeout,
	)
	defer cancel()
	start := time.Now()
	if err := c.freshSessionLocked(ctx); err != nil {
		return nil, err
	}
	deadline, _ := ctx.Deadline()
	if err := c.conn.SetDeadline(deadline); err != nil {
		c.dropLocked()
		return nil, fmt.Errorf("kesagent: set sign deadline: %w", err)
	}
	req := SignRequest{Type: "sign_request", Period: period, Message: message}
	if err := writeFrame(c.conn, MaxSignFrameLen, req); err != nil {
		c.dropLocked()
		return nil, fmt.Errorf("kesagent: send sign request: %w", err)
	}
	var resp SignResponse
	if err := readFrame(c.conn, MaxSignFrameLen, &resp); err != nil {
		c.dropLocked()
		return nil, fmt.Errorf("kesagent: read sign response: %w", err)
	}
	c.lastUsed = time.Now()
	if err := validateSignResponse(
		c.cfg.KESVKey,
		c.cfg.OpCertStartPeriod,
		period,
		message,
		resp,
	); err != nil {
		// A refusal (resp.Error) leaves the session usable; anything else
		// means the agent cannot be trusted on this session.
		if resp.Error == "" {
			c.dropLocked()
		}
		return nil, err
	}
	c.cfg.Metrics.observeSignLatency(time.Since(start))
	c.cfg.Metrics.incSignSuccess()
	return resp.Signature, nil
}

// Close closes the session. It is safe to call more than once.
func (c *RemoteClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.dropLocked()
	return nil
}

func (c *RemoteClient) dropLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *RemoteClient) recordDialFailureLocked() {
	if c.backoff == 0 {
		c.backoff = minReconnectBackoff
	} else {
		c.backoff = min(c.backoff*2, maxReconnectBackoff)
	}
	c.nextDialAfter = time.Now().Add(c.backoff)
}
