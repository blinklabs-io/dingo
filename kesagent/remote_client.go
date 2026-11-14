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

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/bursa/kesagent/wire"
)

const (
	// AttestedProtocolID is the protocol a bursa KES agent's remote sign
	// listener reports in its Hello.
	AttestedProtocolID = wire.AttestedProtocolID
	// ProviderNone is the attestation provider of an agent that presents no
	// evidence: its attestation_response carries none. A provider names the
	// evidence, not the transport; how the link itself is authenticated (a
	// pinned server key, a client certificate, or nothing over plain TCP) is
	// transport configuration.
	ProviderNone = wire.NoEvidenceProvider

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
	// certificate and key, which the agent can authenticate. They are
	// optional and set as a pair; without them no client certificate is
	// sent, and the agent decides whether to accept the session.
	ClientCertFile string
	ClientKeyFile  string
	// ServerSPKISHA256 is the optional hex SHA-256 of the agent's TLS server
	// certificate SubjectPublicKeyInfo. When set, the agent is authenticated
	// by this pin instead of a certificate authority or host name. When
	// empty, the agent's certificate is verified with standard x509 against
	// the system roots for the address's host name; on Unix, SSL_CERT_FILE
	// and SSL_CERT_DIR point those roots at a private CA.
	ServerSPKISHA256 string
	// Plaintext runs the same frames over plain TCP instead of TLS, for
	// setups where something else secures the link, such as a service mesh
	// sidecar. It cannot be combined with a pin or a client certificate,
	// which need TLS.
	Plaintext bool
	// Profile names the agent profile this node signs for, sent in the
	// admission request. An agent that serves several pools on one listener
	// binds the session to it; empty sends none, for an agent with one
	// profile.
	Profile string
	// Provider is the attestation provider the agent must report. Only
	// ProviderNone is supported; empty means ProviderNone.
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
	// OnNewerOpCert, when set, is called with the agent's operational
	// certificate when admission finds one for the same cold key with a
	// higher issue counter than OpCert: the agent's key was rotated and this
	// node's copy of the certificate is behind. Admission still fails, since
	// every signature is verified against OpCert; the caller decides whether
	// to validate the newer certificate and reload with it. It runs on the
	// admitting goroutine and must not block.
	OnNewerOpCert func(opCert []byte)
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

	// onConnected, when set, receives this client's session state in place
	// of Metrics' connected gauge. A FailoverClient sets it, so a standby
	// agent's session does not overwrite the active agent's state.
	onConnected func(connected bool)
}

// RemoteClient signs block headers through a bursa KES agent's remote sign
// listener (AttestedProtocolID) over TLS 1.3, with the agent authenticated by
// a pinned server key or by x509 verification and an optional client
// certificate, or over plain TCP. The
// forger calls Prepare shortly before each leader slot, so the dial, the TLS
// handshake and the admission exchange happen before the slot and Sign finds
// an admitted session; Sign reuses a session while it is fresh and otherwise
// opens one itself. A Sign that gets no signature back sends the identical
// body once more, never a different one, and never after a final refusal.
// It is safe for concurrent use.
type RemoteClient struct {
	cfg    RemoteConfig
	logger *slog.Logger
	// tlsConfig is nil over plain TCP; pin is nil without a server pin.
	tlsConfig *tls.Config
	pin       []byte

	// turn is held by the one caller using the connection: a sign, or a
	// dial and admission. It is a channel so a sign can wait for it within
	// its own deadline. mu guards the fields below and is held only briefly,
	// never across network I/O.
	turn chan struct{}
	// localOpCert is cfg.OpCert decoded, to recognise a newer certificate
	// for the same pool at admission; nil when it does not decode.
	localOpCert *bursa.DecodedOpCert

	mu       sync.Mutex
	conn     net.Conn
	lastUsed time.Time
	closed   bool
	// lastErr is the result of the last dial and admission, which
	// CheckReady reports; lastCheckDial is when CheckReady last started one.
	lastErr       error
	lastCheckDial time.Time

	backoff       time.Duration
	nextDialAfter time.Time
}

// checkReadyDialInterval bounds how often CheckReady, which a readiness probe
// and a metrics scrape call, starts a background dial.
const checkReadyDialInterval = 30 * time.Second

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
	pin, err := remoteTransportPin(cfg)
	if err != nil {
		return nil, err
	}
	switch cfg.Provider {
	case "":
		cfg.Provider = ProviderNone
	case ProviderNone:
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
	if (cfg.ClientCertFile == "") != (cfg.ClientKeyFile == "") {
		return nil, errors.New(
			"kesagent: remote agent client certificate and key must be set together",
		)
	}
	var certs []tls.Certificate
	if cfg.ClientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(
			cfg.ClientCertFile,
			cfg.ClientKeyFile,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"kesagent: load client certificate: %w",
				err,
			)
		}
		certs = []tls.Certificate{cert}
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
		turn:   make(chan struct{}, 1),
	}
	if dec, err := bursa.DecodeOpCert(cfg.OpCert); err == nil {
		c.localOpCert = dec
	}
	cfg.Logger.Info(
		"KES agent link: "+remoteTransportName(cfg, pin),
		"component", "kesagent",
		"address", cfg.Address,
		"client_certificate", len(certs) != 0,
	)
	if !cfg.Plaintext {
		c.tlsConfig = &tls.Config{
			MinVersion:       tls.VersionTLS13,
			Certificates:     certs,
			VerifyConnection: c.verifyServerPin,
		}
		if pin != nil {
			// The agent is authenticated by its pinned server key in
			// VerifyConnection, not by a certificate authority or host
			// name. Without a pin the standard x509 verification runs.
			c.tlsConfig.InsecureSkipVerify = true //nolint:gosec // replaced by the SPKI pin
		}
	}
	return c, nil
}

// remoteTransportName describes the link's transport for the startup log.
func remoteTransportName(cfg RemoteConfig, pin []byte) string {
	switch {
	case cfg.Plaintext:
		return "plain TCP"
	case pin == nil:
		return "TLS, verified by CA"
	default:
		return "TLS, pinned server key"
	}
}

// remoteTransportPin returns the server pin, nil when there is none. It
// refuses only combinations that cannot work: a pin or a client certificate
// over plain TCP, where there is no TLS to carry them.
func remoteTransportPin(cfg RemoteConfig) ([]byte, error) {
	pinned := strings.TrimSpace(cfg.ServerSPKISHA256) != ""
	if cfg.Plaintext {
		if pinned || cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" {
			return nil, errors.New(
				"kesagent: a server pin and a client certificate need TLS; " +
					"a plain TCP remote agent link cannot carry them",
			)
		}
		return nil, nil
	}
	if !pinned {
		return nil, nil
	}
	return parseSPKIPin(cfg.ServerSPKISHA256)
}

// parseSPKIPin decodes a hex SHA-256 SubjectPublicKeyInfo pin.
func parseSPKIPin(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
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
	if c.pin == nil {
		return nil
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
// slot; Sign retries the connection itself. Prepare honours the reconnect
// backoff; Sign does not.
func (c *RemoteClient) Prepare() error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		c.cfg.HelloTimeout,
	)
	defer cancel()
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer c.release()
	return c.ensureSession(ctx, true)
}

// CheckReady reports whether the agent was reachable and admitted this node
// on the last attempt, without waiting on the network: a readiness probe and
// a metrics scrape call it. When no fresh session is open it starts a
// background Prepare, at most once per checkReadyDialInterval, so the answer
// stays current between leader slots. It never requests a signature.
func (c *RemoteClient) CheckReady() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.freshLocked() {
		return nil
	}
	if time.Since(c.lastCheckDial) >= checkReadyDialInterval {
		c.lastCheckDial = time.Now()
		go func() { _ = c.Prepare() }()
	}
	return c.lastErr
}

func (c *RemoteClient) acquire(ctx context.Context) error {
	select {
	case c.turn <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf(
			"kesagent: remote agent connection busy: %w",
			ctx.Err(),
		)
	}
}

func (c *RemoteClient) release() { <-c.turn }

// freshLocked reports whether an admitted session is open and was last used
// within SessionFreshFor. Caller holds c.mu.
func (c *RemoteClient) freshLocked() bool {
	return c.conn != nil && time.Since(c.lastUsed) < c.cfg.SessionFreshFor
}

// ensureSession leaves a fresh admitted session open, dropping an older one
// and dialling a new one. The caller holds the turn; the dial runs without
// c.mu, so Close and CheckReady never wait on the network. respectBackoff
// skips the dial while the reconnect backoff is active.
func (c *RemoteClient) ensureSession(
	ctx context.Context,
	respectBackoff bool,
) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.freshLocked() {
		c.mu.Unlock()
		return nil
	}
	c.dropLocked()
	if respectBackoff && !c.nextDialAfter.IsZero() &&
		time.Now().Before(c.nextDialAfter) {
		until := c.nextDialAfter
		c.mu.Unlock()
		return fmt.Errorf(
			"kesagent: remote agent reconnect backoff active until %s",
			until.Format(time.RFC3339),
		)
	}
	c.mu.Unlock()

	conn, err := c.admit(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.recordDialFailureLocked()
		c.lastErr = err
		return err
	}
	if c.closed {
		_ = conn.Close()
		return ErrClosed
	}
	c.conn = conn
	c.setConnected(true)
	c.lastUsed = time.Now()
	c.lastErr = nil
	c.backoff = 0
	c.nextDialAfter = time.Time{}
	return nil
}

// admit opens one admitted session: the transport (TLS 1.3, by default with
// the pinned server key, or plain TCP), the agent's Hello, and an attestation
// exchange bound to a fresh nonce.
func (c *RemoteClient) admit(ctx context.Context) (net.Conn, error) {
	var raw net.Conn
	var err error
	if c.tlsConfig == nil {
		raw, err = (&net.Dialer{}).DialContext(ctx, "tcp", c.cfg.Address)
	} else {
		raw, err = (&tls.Dialer{
			NetDialer: &net.Dialer{},
			Config:    c.tlsConfig,
		}).DialContext(ctx, "tcp", c.cfg.Address)
	}
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
		Profile:  c.cfg.Profile,
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

// attestationRequest and attestationResponse are the attested listener's
// admission frames, from bursa's kesagent/wire.
type (
	attestationRequest  = wire.AttestationRequest
	attestationResponse = wire.AttestationResponse
)

// checkAdmission accepts a "none" attestation_response for this node's KES
// key, whatever the transport: the echoed nonce, protocol and provider match,
// the evidence is absent ("none" carries none, and evidence would mean
// another provider), the agent's
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
		c.offerNewerOpCert(resp.OpCert)
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

// offerNewerOpCert passes the agent's certificate to OnNewerOpCert when it is
// for the same cold key as the local one and carries a higher issue counter.
// Anything else -- another pool's certificate, an older or equal counter, or
// bytes that do not decode -- is only an admission failure. The caller
// validates the certificate fully before using it; this filter only keeps a
// mismatch that cannot be a rotation from reaching it.
func (c *RemoteClient) offerNewerOpCert(agentOpCert []byte) {
	if c.cfg.OnNewerOpCert == nil || c.localOpCert == nil {
		return
	}
	dec, err := bursa.DecodeOpCert(agentOpCert)
	if err != nil ||
		!bytes.Equal(dec.ColdVKey, c.localOpCert.ColdVKey) ||
		dec.IssueNumber <= c.localOpCert.IssueNumber {
		return
	}
	c.cfg.OnNewerOpCert(append([]byte(nil), agentOpCert...))
}

// maxSignSends bounds how often one Sign sends its request: once, and once
// more with the identical body after a lost reply or an agent error that is
// not a final refusal.
const maxSignSends = 2

// finalSignRefusals are the agent's reasons, as blinklabs-io/bursa words them
// in internal/kesagent, for refusing a request that the identical body cannot
// get past on a resend: a second header for a slot it already signed, an
// older slot, a period its key has passed, a period below its floor, an
// exhausted key, and a message that is not a header for its key. The wire
// protocol carries no error code, so the reason text is all a client has.
var finalSignRefusals = []string{
	"a different header was already signed for this slot",
	"header slot below the last signed slot",
	"requested KES period is in the past for the active key",
	"KES period rollback refused",
	"active KES key has exhausted its evolutions",
	"sign request is not a matching Praos header body",
}

// opCertMismatchRefusals are the agent reasons, as blinklabs-io/bursa words
// them in internal/kesagent, for a header that carries another operational
// certificate than the one the agent has installed: its key was rotated after
// this session was admitted. They are final refusals, and the session is
// dropped too, so the next Prepare or Sign re-admits and offers the agent's
// newer certificate to OnNewerOpCert.
var opCertMismatchRefusals = []string{
	"operational certificate names a different KES key",
	"operational certificate counter/period do not match the installed one",
	"operational certificate cold signature is not the installed one",
}

// isOpCertMismatchRefusal reports whether an agent's refusal reason says the
// header's operational certificate is not the one the agent has installed.
func isOpCertMismatchRefusal(reason string) bool {
	for _, mismatch := range opCertMismatchRefusals {
		if strings.Contains(reason, mismatch) {
			return true
		}
	}
	return false
}

// isFinalSignRefusal reports whether an agent's refusal reason is one a
// resend of the same body cannot change.
func isFinalSignRefusal(reason string) bool {
	for _, final := range finalSignRefusals {
		if strings.Contains(reason, final) {
			return true
		}
	}
	return false
}

// Sign implements ledger/forging.RemoteKESSigner over the remote session. It
// reuses the open session, or opens one within SignTimeout, sends the request
// and verifies the signature against the local operational certificate's KES
// key. A transport or verification failure drops the session, and the next
// Prepare or Sign redials; an agent refusal keeps it, except one saying the
// header's operational certificate is not the agent's installed one, which
// drops it so the redial can offer the agent's newer certificate.
//
// A request that got no signature back, because the reply was lost with the
// session or the agent returned an error that is not a final refusal, is
// sent once more with the identical body while SignTimeout allows: the agent
// may already have recorded the slot, and it signs a recorded slot again
// only for the same body. A final refusal, such as a different header
// already signed for the slot, is returned at once, and so is a signature
// that fails verification.
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
	defer func() {
		if err != nil {
			c.cfg.Metrics.incSignFailure()
		}
	}()
	// The deadline covers the wait for the connection too: a sign at a
	// leader slot must not outlast SignTimeout behind a slow Prepare.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		c.cfg.SignTimeout,
	)
	defer cancel()
	start := time.Now()
	if err := c.acquire(ctx); err != nil {
		return nil, err
	}
	defer c.release()
	req := SignRequest{Type: "sign_request", Period: period, Message: message}
	for sends := 1; ; sends++ {
		sig, resend, err := c.signOnce(ctx, req)
		if err == nil {
			c.cfg.Metrics.observeSignLatency(time.Since(start))
			c.cfg.Metrics.incSignSuccess()
			return sig, nil
		}
		if !resend || sends >= maxSignSends || ctx.Err() != nil {
			return nil, err
		}
		c.logger.Warn(
			"KES agent returned no signature, resending the identical header body",
			"component", "kesagent",
			"period", period,
			"error", err,
		)
	}
}

// signOnce sends req on the session, opening one if needed, and verifies the
// reply. resend reports whether the identical request may be sent again: the
// reply was lost, or the agent answered with an error that is not a final
// refusal. The caller holds the turn.
func (c *RemoteClient) signOnce(
	ctx context.Context,
	req SignRequest,
) (_ []byte, resend bool, _ error) {
	// A sign ignores the reconnect backoff: a leader slot is worth a dial.
	if err := c.ensureSession(ctx, false); err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return nil, false, ErrClosed
	}
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		c.dropConn(conn)
		return nil, true, fmt.Errorf("kesagent: set sign deadline: %w", err)
	}
	if err := writeFrame(conn, MaxSignFrameLen, req); err != nil {
		c.dropConn(conn)
		return nil, true, fmt.Errorf("kesagent: send sign request: %w", err)
	}
	var resp SignResponse
	if err := readFrame(conn, MaxSignFrameLen, &resp); err != nil {
		c.dropConn(conn)
		return nil, true, fmt.Errorf("kesagent: read sign response: %w", err)
	}
	c.mu.Lock()
	if c.conn == conn {
		c.lastUsed = time.Now()
	}
	c.mu.Unlock()
	if err := validateSignResponse(
		c.cfg.KESVKey,
		c.cfg.OpCertStartPeriod,
		req.Period,
		req.Message,
		resp,
	); err != nil {
		// A well-formed refusal leaves the session usable, unless it says
		// the agent now holds another operational certificate; anything
		// else, including an error on a frame of the wrong type or period,
		// means the agent cannot be trusted on this session.
		refusal := resp.Error != "" && resp.Type == "sign_response" &&
			resp.Period == req.Period
		if !refusal || isOpCertMismatchRefusal(resp.Error) {
			c.dropConn(conn)
		}
		return nil, refusal && !isFinalSignRefusal(resp.Error), err
	}
	return resp.Signature, false, nil
}

// Close closes the session. It is safe to call more than once.
func (c *RemoteClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.dropLocked()
	return nil
}

// dropConn closes conn if it is still the open session.
func (c *RemoteClient) dropConn(conn net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		c.dropLocked()
	}
}

func (c *RemoteClient) dropLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.setConnected(false)
	}
}

func (c *RemoteClient) setConnected(connected bool) {
	if c.cfg.onConnected != nil {
		c.cfg.onConnected(connected)
		return
	}
	c.cfg.Metrics.setConnected(connected)
}

func (c *RemoteClient) recordDialFailureLocked() {
	if c.backoff == 0 {
		c.backoff = minReconnectBackoff
	} else {
		c.backoff = min(c.backoff*2, maxReconnectBackoff)
	}
	c.nextDialAfter = time.Now().Add(c.backoff)
	c.cfg.Metrics.incReconnectFailures()
}
