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
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
)

// referenceRecord is one answer recorded from the Rust Mithril crates; see
// testdata/reference/NOTICE for provenance.
type referenceRecord struct {
	File string         `json:"file"`
	I    int            `json:"i"`
	Note string         `json:"note"`
	Req  map[string]any `json:"req"`
	Want struct {
		OK  bool            `json:"ok"`
		Out json.RawMessage `json:"out"`
		Err string          `json:"err"`
	} `json:"want"`
}

func (r referenceRecord) id() string {
	return fmt.Sprintf("%s#%d", r.File, r.I)
}

func (r referenceRecord) str(k string) string { return fmt.Sprint(r.Req[k]) }

func (r referenceRecord) u64(k string) uint64 {
	v, err := strconv.ParseUint(r.str(k), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("%s: %s: %v", r.id(), k, err))
	}
	return v
}

// phi decodes phi_f, recorded as its IEEE-754 bits in hex so that NaN and
// signed zero survive the round trip.
func (r referenceRecord) phi() float64 {
	b, err := strconv.ParseUint(r.str("phi"), 16, 64)
	if err != nil {
		panic(fmt.Sprintf("%s: phi: %v", r.id(), err))
	}
	return math.Float64frombits(b)
}

func (r referenceRecord) bytes(k string) []byte {
	b, err := hex.DecodeString(r.str(k))
	if err != nil {
		panic(fmt.Sprintf("%s: %s: %v", r.id(), k, err))
	}
	return b
}

// wantOut is the recorded output where the answer carries one this test
// compares: the certificate hash, or the protocol parameters hash.
func (r referenceRecord) wantOut() string {
	if len(r.Want.Out) == 0 {
		return ""
	}
	var hashes struct {
		Hash string `json:"hash"`
	}
	if json.Unmarshal(r.Want.Out, &hashes) == nil && hashes.Hash != "" {
		return hashes.Hash
	}
	var s string
	if json.Unmarshal(r.Want.Out, &s) == nil {
		return s
	}
	var b bool
	if json.Unmarshal(r.Want.Out, &b) == nil {
		return strconv.FormatBool(b)
	}
	return ""
}

var errReferenceNotCompared = errors.New("not compared")

// referenceDivergences are the records where dingo deliberately answers
// differently from the recorded Rust code. The test fails if one of them
// starts agreeing, so the list cannot go stale.
var referenceDivergences = map[string]string{
	// Rules Mithril's main branch added after release 2642.0, which dingo
	// enforces: a genesis certificate's AVK and parameters must be the ones
	// its signed message names for the next epoch, and a lottery index must
	// be below m.
	"chain.jsonl#11":  "genesis AVK bound to the signed next AVK",
	"chain.jsonl#12":  "genesis parameters bound to the signed next parameters",
	"chain.jsonl#13":  "certificate after genesis must be in the next epoch",
	"proofs.jsonl#34": "lottery index must be below m",
	// phi_f outside the 8.24 fixed-point range: Rust saturates or panics.
	// Dingo still hashes it, and STM verification rejects any phi_f outside
	// (0, 1], so no certificate carrying one verifies.
	"hashes.jsonl#0":  "phi_f outside 8.24 range",
	"hashes.jsonl#1":  "phi_f outside 8.24 range",
	"hashes.jsonl#4":  "phi_f outside 8.24 range",
	"hashes.jsonl#31": "phi_f infinite: Rust panics",
	"hashes.jsonl#35": "phi_f infinite: Rust panics",
	// An AVK in the binary bytes-hex codec: Rust re-encodes it to JSON-hex
	// before hashing; dingo hashes the text it was given. Aggregators emit
	// JSON-hex, and the hash still binds the text the signatures cover.
	"certs.jsonl#58": "bytes-hex AVK hashed as given",
	"certs.jsonl#61": "upper-case hex multi-signature hashed as given",
	// A multi-signature that does not decode: Rust rejects it when it
	// converts the message, dingo when STM verification decodes it.
	"certs.jsonl#47": "multi-signature decoded at verification",
	"certs.jsonl#51": "multi-signature decoded at verification",
	// A batch path with sibling values left over: Rust ignores them, dingo
	// rejects the path. Mithril never produces one, and the extra values
	// would otherwise be unbounded work the proof does not use.
	"proofs.jsonl#9":   "unused batch path values",
	"proofs.jsonl#42":  "unused batch path values",
	"proofs.jsonl#108": "unused batch path values",
}

// runReference evaluates one recorded request against dingo and returns
// whether dingo accepts it and, where the record has one, its output.
func runReference(r referenceRecord) (bool, string, error) {
	switch r.str("op") {
	case "cert_hash":
		var c Certificate
		if err := json.Unmarshal([]byte(r.str("cert")), &c); err != nil {
			return false, "", err
		}
		h, err := c.ComputeHash()
		return err == nil, h, err
	case "cert_verify":
		err := verifyReferenceCertificate(r)
		return err == nil, "", err
	case "proof_verify":
		params := ProtocolParameters{
			K:    r.u64("k"),
			M:    r.u64("m"),
			PhiF: r.phi(),
		}
		// Certificates carry the AVK and multi-signature as hex of JSON.
		err := verifySTMSignature(
			r.bytes("msg"),
			hex.EncodeToString([]byte(r.str("avk"))),
			hex.EncodeToString([]byte(r.str("proof"))),
			params,
			newCertificateChainBudget(),
		)
		return err == nil, "", err
	case "avk_json":
		_, err := parseSTMAggregateVerificationKey(
			hex.EncodeToString([]byte(r.str("s"))),
		)
		return err == nil, "", err
	case "proof_json":
		_, err := parseSTMAggregateSignature(
			hex.EncodeToString([]byte(r.str("s"))),
		)
		return err == nil, "", err
	case "params_hash":
		p := ProtocolParameters{K: r.u64("k"), M: r.u64("m"), PhiF: r.phi()}
		return true, p.ComputeHash(), nil
	case "lottery":
		// Rust's lottery also rejects zero total stake and phi_f outside
		// (0, 1]. Dingo checks those once per proof in
		// verifySTMConcatenationProof, which proof_verify covers, so only
		// the outcome of a lottery Rust ran is compared here.
		if !r.Want.OK {
			return false, "", errReferenceNotCompared
		}
		var ev [64]byte
		copy(ev[:], r.bytes("ev"))
		won := stmIsLotteryWon(r.phi(), ev, r.u64("stake"), r.u64("total"))
		return true, strconv.FormatBool(won), nil
	case "vk_decode":
		_, err := decodeSTMVerificationKey(r.bytes("b"))
		return err == nil, "", err
	case "sig_decode":
		_, err := decodeSTMSignature(r.bytes("b"))
		return err == nil, "", err
	}
	return false, "", errReferenceNotCompared
}

// verifyReferenceCertificate applies the checks the chain walk applies to
// one certificate and its link to the previous one, or to a genesis
// certificate and the genesis key. A nil error accepts.
func verifyReferenceCertificate(r referenceRecord) error {
	var c Certificate
	if err := json.Unmarshal([]byte(r.str("cert")), &c); err != nil {
		return err
	}
	if err := checkCertificateContent(&c); err != nil {
		return err
	}
	if key, ok := r.Req["genesis_key"]; ok {
		return VerifyGenesisCertificateSignature(&c, fmt.Sprint(key))
	}
	var prev Certificate
	if err := json.Unmarshal([]byte(r.str("prev")), &prev); err != nil {
		return err
	}
	if c.IsGenesis() {
		return errors.New("standard certificate carries a genesis signature")
	}
	if _, err := c.SignedEntityType.Kind(); err != nil {
		return err
	}
	if err := verifySTMCertificate(&c, newCertificateChainBudget()); err != nil {
		return err
	}
	return checkCertificateLink(&c, &prev)
}

func readReferenceCorpus(t *testing.T) []referenceRecord {
	t.Helper()
	f, err := os.Open("testdata/reference/corpus.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(nil, 16<<20)
	var recs []referenceRecord
	for sc.Scan() {
		// UseNumber keeps u64 arguments above 2^53 exact.
		dec := json.NewDecoder(bytes.NewReader(sc.Bytes()))
		dec.UseNumber()
		var rec referenceRecord
		if err := dec.Decode(&rec); err != nil {
			t.Fatal(err)
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return recs
}

// TestReferenceCorpus replays answers recorded from mithril-stm 0.13.0 and
// mithril-common 0.8.3 against dingo's verifier. Every record must agree,
// except the deliberate divergences listed in referenceDivergences.
func TestReferenceCorpus(t *testing.T) {
	t.Parallel()
	recs := readReferenceCorpus(t)
	compared := 0
	for _, rec := range recs {
		ok, out, err := runReference(rec)
		if errors.Is(err, errReferenceNotCompared) {
			continue
		}
		compared++
		agree := ok == rec.Want.OK
		if agree && ok && out != "" && rec.wantOut() != out {
			agree = false
			err = fmt.Errorf("output %s, reference %s", out, rec.wantOut())
		}
		reason, diverges := referenceDivergences[rec.id()]
		switch {
		case diverges && agree:
			t.Errorf(
				"%s (%s): listed as a divergence (%s) but now agrees",
				rec.id(), rec.Note, reason,
			)
		case !diverges && !agree:
			t.Errorf(
				"%s (%s): reference ok=%v err=%s; dingo ok=%v: %v",
				rec.id(), rec.Note, rec.Want.OK, rec.Want.Err, ok, err,
			)
		}
	}
	if compared < 500 {
		t.Fatalf("compared only %d of %d records", compared, len(recs))
	}
}

// TestLiveCardanoBlocksTransactionsCertificate verifies a certificate the
// preprod aggregator published for a CardanoBlocksTransactions beacon: its
// protocol message carries the block-number offset part, hashed between
// latest_block_number and cardano_stake_distribution_epoch, and its signed
// entity type is a three-element array hashed after the entity type id.
func TestLiveCardanoBlocksTransactionsCertificate(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(
		"testdata/reference/preprod-cardano-blocks-transactions.json",
	)
	if err != nil {
		t.Fatal(err)
	}
	var cert Certificate
	if err := json.Unmarshal(raw, &cert); err != nil {
		t.Fatal(err)
	}
	if err := checkCertificateContent(&cert); err != nil {
		t.Fatal(err)
	}
	err = verifySTMCertificate(&cert, newCertificateChainBudget())
	if err != nil {
		t.Fatal(err)
	}
}
