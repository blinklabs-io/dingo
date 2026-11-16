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
	"strings"
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

var (
	errReferenceNotCompared = errors.New("not compared")
	errReferenceUnsupported = errors.New("operation has no handler")
)

// referenceUncompared lists the corpus operations this test does not
// replay, with the reason. Any other operation without a handler fails.
var referenceUncompared = map[string]string{
	"gen": "key generation and signing draw from Rust's seeded ChaCha " +
		"stream, which dingo does not reproduce",
	"aggregate":        "builds keys and signatures with gen's RNG",
	"verify_aggregate": "builds keys and signatures with gen's RNG",
	"ln1m": "Rust's f64 ln(1 - phi_f); dingo decides the lottery by an " +
		"exact rational Taylor bound with no ln, and lottery compares " +
		"the outcome",
}

// referenceDivergence is a record where dingo deliberately answers
// differently from the recorded Rust code, and the answer dingo gives: its
// output when it accepts, or a substring of its error when it rejects.
type referenceDivergence struct {
	reason string
	ok     bool
	detail string
}

// referenceDivergences are the records where dingo deliberately answers
// differently from the recorded Rust code. The test fails if one of them
// starts agreeing, so the list cannot go stale.
var referenceDivergences = map[string]referenceDivergence{
	// Rules Mithril's main branch added after release 2642.0, which dingo
	// enforces: a genesis certificate's AVK and parameters must be the ones
	// its signed message names for the next epoch, and a lottery index must
	// be below m.
	"chain.jsonl#11": {
		"genesis AVK bound to the signed next AVK", false,
		"genesis aggregate verification key does not match signed protocol message",
	},
	"chain.jsonl#12": {
		"genesis parameters bound to the signed next parameters", false,
		"genesis protocol parameters do not match signed protocol message",
	},
	"chain.jsonl#13": {
		"certificate after genesis must be in the next epoch", false,
		"certificate chain epoch 10 follows genesis epoch 10",
	},
	"proofs.jsonl#34": {
		"lottery index must be below m", false,
		"lottery index 12 exceeds parameter m=12",
	},
	// A phi_f that is not finite: Rust panics, dingo hashes it as zero, and
	// STM verification rejects any phi_f outside (0, 1], so no certificate
	// carrying one verifies.
	"hashes.jsonl#31": {
		"phi_f infinite: Rust panics", true,
		"48034c734f1b3b5e5761ac238b54dde7b2e100f0ac6a30ff9c8d70dd6dec3746",
	},
	"hashes.jsonl#35": {
		"phi_f infinite: Rust panics", true,
		"87ba08efa8128d45471a2acf22e62b79337a12f54164865547bd333509326f82",
	},
	// An AVK in the binary bytes-hex codec: Rust re-encodes it to JSON-hex
	// before hashing; dingo hashes the text it was given. Aggregators emit
	// JSON-hex, and the hash still binds the text the signatures cover.
	"certs.jsonl#58": {
		"bytes-hex AVK hashed as given", true,
		"18216e03c1c65ffd2e7bfd17d5841d6a8d2c90e2a163cd6d95327964815deae2",
	},
	"certs.jsonl#61": {
		"upper-case hex multi-signature hashed as given", true,
		"436b4026c721ee1e5a85b574f37dccea6a7c38b1ab979cfb8e0059ea76677157",
	},
	// A multi-signature that does not decode: Rust rejects it when it
	// converts the message, dingo when STM verification decodes it.
	"certs.jsonl#47": {
		"multi-signature decoded at verification", true,
		"02f1dfa2259961adea5c0339c047c18fc8252859d307654e032dcde2d8fdcb0d",
	},
	"certs.jsonl#51": {
		"multi-signature decoded at verification", true,
		"d5806fce250020ebc8c8865522be9872da778510df8f72c7db310883756c00b4",
	},
	// A batch path with sibling values left over: Rust ignores them, dingo
	// rejects the path. Mithril never produces one, and the extra values
	// would otherwise be unbounded work the proof does not use.
	"proofs.jsonl#9": {
		"unused batch path values", false,
		"verifying Merkle batch proof: merkle batch proof has 1 unused sibling values",
	},
	"proofs.jsonl#42": {
		"unused batch path values", false,
		"verifying Merkle batch proof: merkle batch proof has 1 unused sibling values",
	},
	"proofs.jsonl#108": {
		"unused batch path values", false,
		"verifying Merkle batch proof: merkle batch proof has 1 unused sibling values",
	},
	"merkle.jsonl#36": {
		"unused batch path values", false,
		"merkle batch proof has 1 unused sibling values",
	},
	// serde_json parses a float without its float_roundtrip feature, which
	// can land an ulp or two from the nearest double; Go's ParseFloat rounds
	// correctly. Each record here has 17 or more significant digits.
	"numbers.jsonl#11": {
		"serde_json best-effort float parse", true,
		"000fffffffffffff",
	},
	"numbers.jsonl#22": {
		"serde_json best-effort float parse", true,
		"3ddba7188217d8c4",
	},
	"numbers.jsonl#24": {
		"serde_json best-effort float parse", true,
		"3facb284693f1a4d",
	},
	"numbers.jsonl#27": {
		"serde_json best-effort float parse", true,
		"3fb8c36545337aff",
	},
	"numbers.jsonl#29": {
		"serde_json best-effort float parse", true,
		"3fb3fdf329e589a4",
	},
	"numbers.jsonl#30": {
		"serde_json best-effort float parse", true,
		"3f9b5cf2678b2a8b",
	},
	"numbers.jsonl#38": {
		"serde_json best-effort float parse", true,
		"3fab1f871fbce25a",
	},
	"numbers.jsonl#42": {
		"serde_json best-effort float parse", true,
		"3fa98d06c123b859",
	},
	"numbers.jsonl#44": {
		"serde_json best-effort float parse", true,
		"3fbf4838b155a08a",
	},
	"numbers.jsonl#47": {
		"serde_json best-effort float parse", true,
		"3fa64004fd1a2fe8",
	},
	"numbers.jsonl#52": {
		"serde_json best-effort float parse", true,
		"3fbf2967a21581b4",
	},
}

func (d referenceDivergence) matches(ok bool, out string, err error) bool {
	if ok != d.ok {
		return false
	}
	if ok {
		return out == d.detail
	}
	return err != nil && strings.Contains(err.Error(), d.detail)
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
	case "ev":
		ev := stmDenseMapping(r.bytes("sigma"), r.bytes("msgp"), r.u64("index"))
		return true, hex.EncodeToString(ev[:]), nil
	case "ed25519_verify":
		ok := verifyEd25519Strict(r.bytes("vk"), r.bytes("msg"), r.bytes("sig"))
		return ok, "", nil
	case "parse_f64":
		// phi_f as a certificate's parameters decode it.
		clean, err := serdeJSON([]byte(r.str("s")), &serdeField{kind: serdeF64})
		if err != nil {
			return false, "", err
		}
		var f float64
		if err := json.Unmarshal(clean, &f); err != nil {
			return false, "", err
		}
		return true, fmt.Sprintf("%016x", math.Float64bits(f)), nil
	case "batch_path":
		err := verifyReferenceBatchPath(r)
		return err == nil, "", err
	}
	return false, "", errReferenceUnsupported
}

// verifyReferenceBatchPath checks a Merkle batch path for leaves against a
// commitment, as STM verification checks the signers of a proof.
func verifyReferenceBatchPath(r referenceRecord) error {
	nrLeaves, err := strconv.Atoi(r.str("nr_leaves"))
	if err != nil {
		return err
	}
	avk := stmAggregateVerificationKey{
		MTCommitment: stmMerkleTreeBatchCommitment{
			Root:     r.bytes("root"),
			NrLeaves: nrLeaves,
		},
	}
	var proof stmMerkleBatchPath
	for _, v := range r.Req["values"].([]any) {
		b, err := hex.DecodeString(fmt.Sprint(v))
		if err != nil {
			return err
		}
		proof.Values = append(proof.Values, b)
	}
	for _, v := range r.Req["indices"].([]any) {
		i, err := strconv.Atoi(fmt.Sprint(v))
		if err != nil {
			return err
		}
		proof.Indices = append(proof.Indices, i)
	}
	var leaves []stmClosedRegistrationEntry
	for _, v := range r.Req["leaves"].([]any) {
		pair := v.([]any)
		vk, err := hex.DecodeString(fmt.Sprint(pair[0]))
		if err != nil {
			return err
		}
		stake, err := strconv.ParseUint(fmt.Sprint(pair[1]), 10, 64)
		if err != nil {
			return err
		}
		leaves = append(leaves, stmClosedRegistrationEntry{
			VerificationKey: vk,
			Stake:           stake,
		})
	}
	return stmVerifyLeavesMembershipFromBatchPath(&avk, leaves, &proof)
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
		if errors.Is(err, errReferenceUnsupported) {
			if _, listed := referenceUncompared[rec.str("op")]; !listed {
				t.Errorf(
					"%s: operation %q has no handler",
					rec.id(),
					rec.str("op"),
				)
			}
			continue
		}
		if errors.Is(err, errReferenceNotCompared) {
			continue
		}
		compared++
		agree := ok == rec.Want.OK
		if agree && ok && out != "" && rec.wantOut() != out {
			agree = false
			err = fmt.Errorf("output %s, reference %s", out, rec.wantOut())
		}
		div, diverges := referenceDivergences[rec.id()]
		switch {
		case diverges && agree:
			t.Errorf(
				"%s (%s): listed as a divergence (%s) but now agrees",
				rec.id(), rec.Note, div.reason,
			)
		case diverges && !div.matches(ok, out, err):
			t.Errorf(
				"%s (%s): listed as a divergence (%s) with dingo ok=%v %q; "+
					"dingo ok=%v out=%q: %v",
				rec.id(),
				rec.Note,
				div.reason,
				div.ok,
				div.detail,
				ok,
				out,
				err,
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
