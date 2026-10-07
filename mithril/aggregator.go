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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	aggregatorStateKey        = "aggregator.json"
	certificateVersion        = "0.1.0"
	maxAggregatorRequestBytes = 1 << 20
	stmVerificationKeyBytes   = 96
	minOperatorTokenBytes     = 32
)

var (
	partyIDPattern     = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	networkNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
)

// AggregatorConfig configures an Aggregator.
type AggregatorConfig struct {
	// Network is the Cardano network name written into certificates; only
	// snapshots of this network are certified.
	Network string
	// Epoch is the epoch signers register for. The genesis certificate is
	// issued at Epoch-1, so Epoch must be at least 1.
	Epoch uint64
	// Parameters are the STM protocol parameters for the signer set.
	Parameters ProtocolParameters
	// GenesisSigningKey signs the genesis certificate that roots the chain.
	GenesisSigningKey ed25519.PrivateKey
	// Store holds the snapshots to certify and receives the certificates.
	Store ArtifactStore
	// OperatorToken authorizes signer registration and registration closure.
	// Use at least 32 unpredictable bytes and transport it over TLS.
	OperatorToken string
}

// Aggregator collects signer registrations and individual STM signatures over
// stored snapshots, and publishes a certificate for a snapshot once the
// signatures cover the quorum. An operator explicitly closes the epoch's
// signer set before signing starts; closure derives the aggregate verification
// key and issues the genesis certificate.
type Aggregator struct {
	cfg               AggregatorConfig
	operatorTokenKey  [sha256.Size]byte
	operatorTokenHash [sha256.Size]byte

	mu     sync.Mutex
	regs   map[string]stmRegistration
	sealed *sealedRegistration
	state  aggregatorState
	open   *openMessage
	now    func() time.Time
}

type persistedSigner struct {
	PartyID           string `json:"party_id"`
	VerificationKey   string `json:"verification_key"`
	ProofOfPossession string `json:"proof_of_possession"`
	Stake             uint64 `json:"stake"`
}

// aggregatorState is what survives a restart: the closed signer set and the
// head of the certificate chain.
type aggregatorState struct {
	Epoch               uint64             `json:"epoch"`
	Parameters          ProtocolParameters `json:"parameters"`
	Signers             []persistedSigner  `json:"signers"`
	GenesisHash         string             `json:"genesis_hash"`
	GenesisCreatedAt    string             `json:"genesis_created_at"`
	LastCertificateHash string             `json:"last_certificate_hash"`
}

type sealedRegistration struct {
	ordered    []stmRegistration
	entries    []stmClosedRegistrationEntry
	tree       *stmMerkleTree
	avk        *stmAggregateVerificationKey
	avkEncoded string
	partyIndex map[string]int
}

type openMessage struct {
	snapshot      CardanoDatabaseSnapshot
	protocol      ProtocolMessage
	signedMessage string
	initiatedAt   time.Time
	sigs          map[string]stmSingleSignature
}

// NewAggregator validates cfg and restores the signer set and chain head a
// previous run closed, if any.
func NewAggregator(
	ctx context.Context,
	cfg AggregatorConfig,
) (*Aggregator, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("aggregator requires an artifact store")
	case !networkNamePattern.MatchString(cfg.Network):
		return nil, fmt.Errorf("invalid network name %q", cfg.Network)
	case cfg.Epoch == 0:
		return nil, errors.New("aggregator epoch must be at least 1")
	case len(cfg.GenesisSigningKey) != ed25519.PrivateKeySize:
		return nil, errors.New("aggregator requires a genesis signing key")
	case cfg.Parameters.K == 0 || cfg.Parameters.M == 0 ||
		cfg.Parameters.K > cfg.Parameters.M:
		return nil, errors.New(
			"aggregator parameters k and m must be positive and k must not exceed m",
		)
	case !(cfg.Parameters.PhiF > 0 && cfg.Parameters.PhiF <= 1):
		return nil, fmt.Errorf(
			"aggregator parameter phi_f=%v must be in (0, 1]",
			cfg.Parameters.PhiF,
		)
	case len(cfg.OperatorToken) < minOperatorTokenBytes:
		return nil, fmt.Errorf(
			"aggregator operator token must be at least %d bytes",
			minOperatorTokenBytes,
		)
	}
	var tokenKey [sha256.Size]byte
	if _, err := rand.Read(tokenKey[:]); err != nil {
		return nil, fmt.Errorf("generating operator token key: %w", err)
	}
	tokenHash := operatorTokenHash(tokenKey[:], cfg.OperatorToken)
	cfg.OperatorToken = ""
	a := &Aggregator{
		cfg:               cfg,
		operatorTokenKey:  tokenKey,
		operatorTokenHash: tokenHash,
		regs:              make(map[string]stmRegistration),
		now:               time.Now,
	}
	found, err := getJSON(ctx, cfg.Store, aggregatorStateKey, &a.state)
	if err != nil {
		return nil, err
	}
	if !found {
		return a, nil
	}
	if a.state.Epoch != cfg.Epoch || a.state.Parameters != cfg.Parameters {
		return nil, fmt.Errorf(
			"aggregator epoch %d parameters %+v differ from the closed "+
				"registration (epoch %d, parameters %+v)",
			cfg.Epoch, cfg.Parameters, a.state.Epoch, a.state.Parameters,
		)
	}
	regs := make([]stmRegistration, 0, len(a.state.Signers))
	for _, p := range a.state.Signers {
		vk, err := hex.DecodeString(p.VerificationKey)
		if err != nil {
			return nil, fmt.Errorf("decoding stored verification key: %w", err)
		}
		pop, err := hex.DecodeString(p.ProofOfPossession)
		if err != nil {
			return nil, fmt.Errorf(
				"decoding stored proof of possession: %w", err,
			)
		}
		regs = append(regs, stmRegistration{
			PartyID: p.PartyID, VerificationKey: vk,
			ProofOfPossession: pop, Stake: p.Stake,
		})
	}
	a.sealed = closeRegistrations(regs)
	return a, nil
}

func closeRegistrations(regs []stmRegistration) *sealedRegistration {
	ordered, entries, total := closeSTMRegistration(regs)
	tree := newSTMMerkleTree(entries)
	index := make(map[string]int, len(ordered))
	for i, r := range ordered {
		index[r.PartyID] = i
	}
	return &sealedRegistration{
		ordered: ordered,
		entries: entries,
		tree:    tree,
		avk: &stmAggregateVerificationKey{
			MTCommitment: stmMerkleTreeBatchCommitment{
				Root: tree.root(), NrLeaves: len(entries),
			},
			TotalStake: total,
		},
		avkEncoded: encodeSTMAggregateVerificationKey(
			tree.root(), len(entries), total,
		),
		partyIndex: index,
	}
}

func (a *Aggregator) registerRoutes(
	mux *http.ServeMux,
	publicRequest func(http.HandlerFunc) http.HandlerFunc,
) {
	mux.HandleFunc("POST /register-signer", a.handleRegisterSigner)
	mux.HandleFunc("POST /close-registrations", a.handleCloseRegistrations)
	mux.HandleFunc(
		"GET /certificate-pending", publicRequest(a.handlePending),
	)
	mux.HandleFunc(
		"POST /register-signatures", publicRequest(a.handleRegisterSignatures),
	)
	mux.HandleFunc(
		"GET /artifact/mithril-stake-distributions",
		publicRequest(a.handleDistributions),
	)
	mux.HandleFunc(
		"GET /artifact/mithril-stake-distribution/{hash}",
		publicRequest(a.handleDistribution),
	)
}

// apiError carries the HTTP status a failed request maps to.
type apiError struct {
	status int
	msg    string
}

func (e *apiError) Error() string { return e.msg }

func badRequest(format string, args ...any) *apiError {
	return &apiError{http.StatusBadRequest, fmt.Sprintf(format, args...)}
}

func conflict(msg string) *apiError {
	return &apiError{http.StatusConflict, msg}
}

func writeAPIError(w http.ResponseWriter, err error) {
	if ae, ok := errors.AsType[*apiError](err); ok {
		http.Error(w, ae.msg, ae.status)
		return
	}
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxAggregatorRequestBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return badRequest("invalid request body: %v", err)
	}
	return nil
}

// getJSON decodes the object at key into v, reporting false when it does not
// exist.
func getJSON(
	ctx context.Context,
	store ArtifactStore,
	key string,
	v any,
) (bool, error) {
	r, err := store.Open(ctx, key)
	if errors.Is(err, ErrArtifactNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer r.Close()
	if err := json.NewDecoder(io.LimitReader(r, 1<<20)).Decode(v); err != nil {
		return false, fmt.Errorf("decoding %s: %w", key, err)
	}
	return true, nil
}

func putJSON(
	ctx context.Context,
	store ArtifactStore,
	key string,
	v any,
) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encoding %s: %w", key, err)
	}
	return store.Put(ctx, key, bytes.NewReader(data))
}

type registerSignerRequest struct {
	Epoch   uint64 `json:"epoch"`
	PartyID string `json:"party_id"`
	// VerificationKey is the hex of the 96-byte BLS verification key followed
	// by its 96-byte proof of possession.
	VerificationKey string `json:"verification_key"`
	Stake           uint64 `json:"stake"`
}

func (a *Aggregator) handleRegisterSigner(
	w http.ResponseWriter,
	r *http.Request,
) {
	if !a.authorizeOperator(w, r) {
		return
	}
	var req registerSignerRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	if err := a.registerSigner(req); err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (a *Aggregator) handleCloseRegistrations(
	w http.ResponseWriter,
	r *http.Request,
) {
	if !a.authorizeOperator(w, r) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.closeRegistrations(r.Context()); err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Aggregator) authorizeOperator(
	w http.ResponseWriter,
	r *http.Request,
) bool {
	header := r.Header.Get("Authorization")
	scheme, token, ok := strings.Cut(header, " ")
	if ok && strings.EqualFold(scheme, "Bearer") && token != "" {
		candidate := operatorTokenHash(a.operatorTokenKey[:], token)
		if hmac.Equal(candidate[:], a.operatorTokenHash[:]) {
			return true
		}
	}
	w.Header().Set("WWW-Authenticate", "Bearer")
	http.Error(w, "operator authorization required", http.StatusUnauthorized)
	return false
}

func operatorTokenHash(key []byte, token string) [sha256.Size]byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(token))
	var ret [sha256.Size]byte
	copy(ret[:], h.Sum(nil))
	return ret
}

func (a *Aggregator) registerSigner(req registerSignerRequest) error {
	if req.Epoch != a.cfg.Epoch {
		return badRequest(
			"registration is for epoch %d, not %d", req.Epoch, a.cfg.Epoch,
		)
	}
	if !partyIDPattern.MatchString(req.PartyID) {
		return badRequest("invalid party_id")
	}
	if req.Stake == 0 {
		return badRequest("stake must be positive")
	}
	raw, err := hex.DecodeString(req.VerificationKey)
	if err != nil || len(raw) != 2*stmVerificationKeyBytes {
		return badRequest(
			"verification_key must be %d hex-encoded bytes",
			2*stmVerificationKeyBytes,
		)
	}
	vk, pop := raw[:stmVerificationKeyBytes], raw[stmVerificationKeyBytes:]
	if err := verifySTMProofOfPossession(vk, pop); err != nil {
		return badRequest("invalid verification key: %v", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sealed != nil {
		return conflict("registration is closed")
	}
	if current, ok := a.regs[req.PartyID]; ok &&
		(!bytes.Equal(current.VerificationKey, vk) ||
			!bytes.Equal(current.ProofOfPossession, pop)) {
		return conflict("party_id is already bound to another verification key")
	}
	var others uint64
	for id, r := range a.regs {
		if id != req.PartyID {
			others += r.Stake
		}
	}
	if others > math.MaxUint64-req.Stake {
		return badRequest("total stake overflows")
	}
	a.regs[req.PartyID] = stmRegistration{
		PartyID:           req.PartyID,
		VerificationKey:   vk,
		ProofOfPossession: pop,
		Stake:             req.Stake,
	}
	return nil
}

type pendingSigner struct {
	PartyID         string `json:"party_id"`
	VerificationKey string `json:"verification_key"`
	Stake           uint64 `json:"stake"`
}

// pendingCertificate is what a signer needs to sign the open message: the
// message, the commitment to the closed signer set, and the set itself in
// signer-index order.
type pendingCertificate struct {
	Epoch                    uint64             `json:"epoch"`
	SignedEntityType         json.RawMessage    `json:"signed_entity_type"`
	ProtocolParameters       ProtocolParameters `json:"protocol_parameters"`
	SignedMessage            string             `json:"signed_message"`
	AggregateVerificationKey string             `json:"aggregate_verification_key"`
	Signers                  []pendingSigner    `json:"signers"`
}

func (a *Aggregator) handlePending(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	open, err := a.ensureOpen(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if open == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	entity, err := newSignedEntityType(
		signedEntityTypeCardanoDatabase, open.snapshot.Beacon,
	)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	signers := make([]pendingSigner, len(a.sealed.ordered))
	for i, s := range a.sealed.ordered {
		signers[i] = pendingSigner{
			PartyID:         s.PartyID,
			VerificationKey: hex.EncodeToString(s.VerificationKey),
			Stake:           s.Stake,
		}
	}
	writeJSON(w, pendingCertificate{
		Epoch:                    a.cfg.Epoch,
		SignedEntityType:         entity.Raw(),
		ProtocolParameters:       a.cfg.Parameters,
		SignedMessage:            open.signedMessage,
		AggregateVerificationKey: a.sealed.avkEncoded,
		Signers:                  signers,
	})
}

// ensureOpen returns the message to sign after registration has been closed.
// It returns nil when registration remains open or no snapshot awaits a
// certificate. The caller holds a.mu.
func (a *Aggregator) ensureOpen(ctx context.Context) (*openMessage, error) {
	if a.open != nil {
		// Retention may have pruned the open snapshot. Certifying it would
		// spend a certificate on a snapshot publishing then unlists, so
		// signing moves on to the next snapshot instead.
		_, err := readSnapshot(ctx, a.cfg.Store, a.open.snapshot.Hash)
		switch {
		case err == nil:
			return a.open, nil
		case !errors.Is(err, ErrArtifactNotFound):
			return nil, err
		}
		a.open = nil
	}
	snapshot, err := a.oldestUncertified(ctx)
	if err != nil || snapshot == nil {
		return nil, err
	}
	if a.sealed == nil {
		return nil, nil
	}
	a.open = a.newOpenMessage(*snapshot)
	return a.open, nil
}

func (a *Aggregator) closeRegistrations(ctx context.Context) error {
	if a.sealed != nil {
		return nil
	}
	if len(a.regs) == 0 {
		return conflict("no signers are registered")
	}
	snapshot, err := a.oldestUncertified(ctx)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return conflict("no snapshot is awaiting a certificate")
	}
	if err := a.seal(ctx); err != nil {
		return err
	}
	a.open = a.newOpenMessage(*snapshot)
	return nil
}

func (a *Aggregator) newOpenMessage(
	snapshot CardanoDatabaseSnapshot,
) *openMessage {
	protocol := a.protocolMessage(a.cfg.Epoch, snapshot.MerkleRoot)
	return &openMessage{
		snapshot:      snapshot,
		protocol:      protocol,
		signedMessage: protocol.ComputeHash(),
		initiatedAt:   a.now().UTC(),
		sigs:          make(map[string]stmSingleSignature),
	}
}

func (a *Aggregator) oldestUncertified(
	ctx context.Context,
) (*CardanoDatabaseSnapshot, error) {
	list, err := ListSnapshots(ctx, a.cfg.Store)
	if err != nil {
		return nil, err
	}
	for _, item := range slices.Backward(list) {
		if item.CertificateHash == "" && item.Network == a.cfg.Network {
			return &item, nil
		}
	}
	return nil, nil
}

// protocolMessage builds the message a certificate at epoch signs. The
// next-epoch key and parameters are the closed set's own: the same signers
// carry on, so the chain verifies across the genesis boundary and within the
// epoch. An empty merkleRoot builds the genesis message.
func (a *Aggregator) protocolMessage(
	epoch uint64,
	merkleRoot string,
) ProtocolMessage {
	parts := map[string]string{
		"current_epoch":                   strconv.FormatUint(epoch, 10),
		"next_aggregate_verification_key": a.sealed.avkEncoded,
		"next_protocol_parameters":        a.cfg.Parameters.ComputeHash(),
	}
	if merkleRoot != "" {
		parts["cardano_database_merkle_root"] = merkleRoot
	}
	return ProtocolMessage{MessageParts: parts}
}

// seal closes registration, persists the signer set and issues the genesis
// certificate at the previous epoch, whose next-epoch key is the closed set's.
func (a *Aggregator) seal(ctx context.Context) error {
	regs := make([]stmRegistration, 0, len(a.regs))
	for _, r := range a.regs {
		regs = append(regs, r)
	}
	a.sealed = closeRegistrations(regs)
	genesis, err := a.newGenesisCertificate()
	if err != nil {
		a.sealed = nil
		return err
	}
	if err := putJSON(ctx, a.cfg.Store, certificateKey(genesis.Hash), genesis); err != nil {
		a.sealed = nil
		return err
	}
	signers := make([]persistedSigner, len(a.sealed.ordered))
	for i, s := range a.sealed.ordered {
		signers[i] = persistedSigner{
			PartyID:           s.PartyID,
			VerificationKey:   hex.EncodeToString(s.VerificationKey),
			ProofOfPossession: hex.EncodeToString(s.ProofOfPossession),
			Stake:             s.Stake,
		}
	}
	state := aggregatorState{
		Epoch:               a.cfg.Epoch,
		Parameters:          a.cfg.Parameters,
		Signers:             signers,
		GenesisHash:         genesis.Hash,
		GenesisCreatedAt:    genesis.Metadata.SealedAt,
		LastCertificateHash: genesis.Hash,
	}
	if err := putJSON(ctx, a.cfg.Store, aggregatorStateKey, state); err != nil {
		a.sealed = nil
		return err
	}
	a.state = state
	a.regs = nil
	return nil
}

func certificateKey(hash string) string {
	return path.Join(certificatesPrefix, hash+".json")
}

func (a *Aggregator) metadata(initiated, sealed time.Time) CertificateMetadata {
	signers := make([]StakeDistributionParty, len(a.sealed.ordered))
	for i, s := range a.sealed.ordered {
		signers[i] = StakeDistributionParty{PartyID: s.PartyID, Stake: s.Stake}
	}
	return CertificateMetadata{
		Network:     a.cfg.Network,
		Version:     certificateVersion,
		Parameters:  a.cfg.Parameters,
		InitiatedAt: initiated.UTC().Format(time.RFC3339Nano),
		SealedAt:    sealed.UTC().Format(time.RFC3339Nano),
		Signers:     signers,
	}
}

func newSignedEntityType(kind string, payload any) (SignedEntityType, error) {
	raw, err := json.Marshal(map[string]any{kind: payload})
	if err != nil {
		return SignedEntityType{}, fmt.Errorf(
			"encoding signed entity type: %w", err,
		)
	}
	var out SignedEntityType
	if err := out.UnmarshalJSON(raw); err != nil {
		return SignedEntityType{}, err
	}
	return out, nil
}

func (a *Aggregator) newGenesisCertificate() (*Certificate, error) {
	epoch := a.cfg.Epoch - 1
	entity, err := newSignedEntityType(
		signedEntityTypeMithrilStakeDistribution, epoch,
	)
	if err != nil {
		return nil, err
	}
	protocol := a.protocolMessage(epoch, "")
	now := a.now()
	cert := &Certificate{
		Epoch:                    epoch,
		SignedEntityType:         entity,
		Metadata:                 a.metadata(now, now),
		ProtocolMessage:          protocol,
		SignedMessage:            protocol.ComputeHash(),
		AggregateVerificationKey: a.sealed.avkEncoded,
	}
	cert.GenesisSignature = hex.EncodeToString(
		ed25519.Sign(a.cfg.GenesisSigningKey, []byte(cert.SignedMessage)),
	)
	if cert.Hash, err = cert.ComputeHash(); err != nil {
		return nil, fmt.Errorf("hashing genesis certificate: %w", err)
	}
	return cert, nil
}

type registerSignatureRequest struct {
	PartyID       string `json:"party_id"`
	SignedMessage string `json:"signed_message"`
	// Signature is the hex of the signer's single signature: the won lottery
	// indices, the 48-byte BLS signature and the signer index.
	Signature string `json:"signature"`
}

func (a *Aggregator) handleRegisterSignatures(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req registerSignatureRequest
	if err := decodeJSONBody(w, r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	if err := a.registerSignature(r.Context(), req); err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (a *Aggregator) registerSignature(
	ctx context.Context,
	req registerSignatureRequest,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	open, err := a.ensureOpen(ctx)
	if err != nil {
		return err
	}
	if open == nil {
		return conflict("no message is open for signing")
	}
	if req.SignedMessage != open.signedMessage {
		return conflict("signed_message is not the open message")
	}
	index, ok := a.sealed.partyIndex[req.PartyID]
	if !ok {
		return badRequest("party %q is not a registered signer", req.PartyID)
	}
	raw, err := hex.DecodeString(req.Signature)
	if err != nil {
		return badRequest("signature is not hex")
	}
	sig, err := parseSTMSingleSignatureBytes(raw)
	if err != nil || len(encodeSTMSingleSignature(*sig)) != len(raw) {
		return badRequest("malformed signature")
	}
	position := uint64(index) //nolint:gosec // index is a slice position
	if sig.SignerIndex != position {
		return badRequest(
			"signature is for signer index %d, not %d", sig.SignerIndex, index,
		)
	}
	if err := verifySTMSingleSignature(
		[]byte(open.signedMessage), a.sealed.avk, a.cfg.Parameters,
		a.sealed.entries[index], sig,
	); err != nil {
		return badRequest("invalid signature: %v", err)
	}
	open.sigs[req.PartyID] = *sig
	return a.tryCertify(ctx, open)
}

// tryCertify publishes a certificate if the collected signatures cover the
// quorum, and does nothing otherwise. The caller holds a.mu.
func (a *Aggregator) tryCertify(
	ctx context.Context,
	open *openMessage,
) error {
	sigs := make([]stmSingleSignature, 0, len(open.sigs))
	for _, s := range open.sigs {
		sigs = append(sigs, s)
	}
	selected, err := selectSTMSignatures(sigs, a.cfg.Parameters.K)
	if err != nil {
		// Too few indices so far; more signatures may still arrive.
		return nil //nolint:nilerr
	}
	withParty := make([]stmSingleSignatureWithRegisteredParty, len(selected))
	indices := make([]int, len(selected))
	for i, s := range selected {
		withParty[i] = stmSingleSignatureWithRegisteredParty{
			Sig: s, RegParty: a.sealed.entries[s.SignerIndex],
		}
		position := int(s.SignerIndex) //nolint:gosec // checked on receipt
		indices[i] = position
	}
	proof, err := a.sealed.tree.batchPath(indices)
	if err != nil {
		return err
	}
	multiSignature := hex.EncodeToString(
		encodeSTMAggregateSignature(withParty, proof),
	)
	// The certificate is only published once it verifies as a client will
	// verify it, so a defect here fails closed instead of poisoning the chain.
	if err := verifySTMSignature(
		[]byte(open.signedMessage), a.sealed.avkEncoded, multiSignature,
		a.cfg.Parameters,
	); err != nil {
		return fmt.Errorf("aggregated signature does not verify: %w", err)
	}
	entity, err := newSignedEntityType(
		signedEntityTypeCardanoDatabase, open.snapshot.Beacon,
	)
	if err != nil {
		return err
	}
	cert := &Certificate{
		PreviousHash:             a.state.LastCertificateHash,
		Epoch:                    a.cfg.Epoch,
		SignedEntityType:         entity,
		Metadata:                 a.metadata(open.initiatedAt, a.now()),
		ProtocolMessage:          open.protocol,
		SignedMessage:            open.signedMessage,
		AggregateVerificationKey: a.sealed.avkEncoded,
		MultiSignature:           multiSignature,
	}
	if cert.Hash, err = cert.ComputeHash(); err != nil {
		return fmt.Errorf("hashing certificate: %w", err)
	}
	if err := putJSON(ctx, a.cfg.Store, certificateKey(cert.Hash), cert); err != nil {
		return err
	}
	// The chain head moves before the snapshot is marked certified: a crash
	// between the two leaves the snapshot uncertified, so it is certified
	// again on a valid chain, rather than leaving the head behind a snapshot
	// that claims a certificate the next one would not chain to.
	state := a.state
	state.LastCertificateHash = cert.Hash
	if err := putJSON(ctx, a.cfg.Store, aggregatorStateKey, state); err != nil {
		return err
	}
	a.state = state
	snapshot := open.snapshot
	snapshot.CertificateHash = cert.Hash
	err = publishSnapshot(ctx, a.cfg.Store, &snapshot)
	if errors.Is(err, ErrSnapshotArchivesMissing) {
		// Retention removed the snapshot after ensureOpen read it. The
		// certificate stays in the chain and signing moves on.
		a.open = nil
		return conflict("the open snapshot was removed before certification")
	}
	if err != nil {
		return err
	}
	a.open = nil
	return nil
}

// distribution returns the Mithril stake distribution the genesis certificate
// certifies, or nil before registration closes. The hash is the genesis
// certificate's: the distribution has no other identity here.
func (a *Aggregator) distribution() (*MithrilStakeDistribution, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sealed == nil {
		return nil, ""
	}
	signers := make([]MithrilStakeDistributionParty, len(a.sealed.ordered))
	for i, s := range a.sealed.ordered {
		signers[i] = MithrilStakeDistributionParty{
			PartyID:         s.PartyID,
			Stake:           s.Stake,
			VerificationKey: hex.EncodeToString(s.VerificationKey),
		}
	}
	return &MithrilStakeDistribution{
		Hash:            a.state.GenesisHash,
		CertificateHash: a.state.GenesisHash,
		Epoch:           a.cfg.Epoch - 1,
		Signers:         signers,
	}, a.state.GenesisCreatedAt
}

func (a *Aggregator) handleDistributions(
	w http.ResponseWriter,
	_ *http.Request,
) {
	items := []MithrilStakeDistributionListItem{}
	if dist, createdAt := a.distribution(); dist != nil {
		items = append(items, MithrilStakeDistributionListItem{
			Hash:            dist.Hash,
			CertificateHash: dist.CertificateHash,
			CreatedAt:       createdAt,
			Epoch:           dist.Epoch,
		})
	}
	writeJSON(w, items)
}

func (a *Aggregator) handleDistribution(
	w http.ResponseWriter,
	r *http.Request,
) {
	dist, _ := a.distribution()
	if dist == nil || dist.Hash != r.PathValue("hash") {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, dist)
}
