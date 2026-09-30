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

package eras_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
)

// Raw-CBOR regressions for the native-script decode domain. Blocks decode
// through ledger.NewBlockFromCbor (chainsync, blockfetch, load) and
// transactions through safedecode.Transaction (mempool, txsubmission), so a
// script the reference decoder refuses must never reach ValidateTx*.

type nsEra struct {
	name      string
	blockType uint
	txType    uint
	// maxCtor is the highest native-script constructor the era decodes.
	maxCtor uint
}

var (
	nsShelley = nsEra{"shelley", ledger.BlockTypeShelley, ledger.TxTypeShelley, 3}
	nsAllegra = nsEra{"allegra", ledger.BlockTypeAllegra, ledger.TxTypeAllegra, 5}
	nsMary    = nsEra{"mary", ledger.BlockTypeMary, ledger.TxTypeMary, 5}
	nsAlonzo  = nsEra{"alonzo", ledger.BlockTypeAlonzo, ledger.TxTypeAlonzo, 5}
	nsBabbage = nsEra{"babbage", ledger.BlockTypeBabbage, ledger.TxTypeBabbage, 5}
	nsConway  = nsEra{"conway", ledger.BlockTypeConway, ledger.TxTypeConway, 5}

	nsPostShelley = []nsEra{nsAllegra, nsMary, nsAlonzo, nsBabbage, nsConway}
	nsAllEras     = append([]nsEra{nsShelley}, nsPostShelley...)
)

func nsKeyHash(seed byte) []byte {
	return bytes.Repeat([]byte{seed}, 28)
}

func nsSig(hash []byte) []any        { return []any{uint64(0), hash} }
func nsAll(s ...any) []any           { return []any{uint64(1), s} }
func nsAny(s ...any) []any           { return []any{uint64(2), s} }
func nsNofK(n int64, s ...any) []any { return []any{uint64(3), n, s} }
func nsBefore(slot uint64) []any     { return []any{uint64(4), slot} }
func nsAfter(slot uint64) []any      { return []any{uint64(5), slot} }
func nsGuard(hash []byte) []any      { return []any{uint64(6), []any{uint64(0), hash}} }

func nsMustEncode(t *testing.T, v any) []byte {
	t.Helper()
	data, err := cbor.Encode(v)
	require.NoError(t, err)
	return data
}

func nsBody(era nsEra) map[uint64]any {
	var amount any = uint64(2_000_000)
	body := map[uint64]any{
		0: []any{[]any{bytes.Repeat([]byte{0xAA}, 32), uint64(0)}},
		1: []any{[]any{append([]byte{0x61}, nsKeyHash(0x11)...), amount}},
		2: uint64(200_000),
	}
	if era.blockType == ledger.BlockTypeShelley {
		body[3] = uint64(1000)
	}
	return body
}

// nsWitnessSet holds only native-script witnesses (key 1).
func nsWitnessSet(scripts ...any) map[uint64]any {
	return map[uint64]any{1: scripts}
}

func nsTxCbor(t *testing.T, era nsEra, body map[uint64]any, ws map[uint64]any) []byte {
	t.Helper()
	if era.blockType >= ledger.BlockTypeAlonzo {
		return nsMustEncode(t, []any{body, ws, true, nil})
	}
	return nsMustEncode(t, []any{body, ws, nil})
}

func nsHeader(era nsEra, bodySize uint64, bodyHash []byte) []any {
	vrf := []any{bytes.Repeat([]byte{1}, 64), bytes.Repeat([]byte{2}, 80)}
	var body []any
	if era.blockType >= ledger.BlockTypeBabbage {
		body = []any{
			uint64(1), uint64(100), bytes.Repeat([]byte{3}, 32),
			bytes.Repeat([]byte{4}, 32), bytes.Repeat([]byte{5}, 32),
			vrf, bodySize, bodyHash,
			[]any{bytes.Repeat([]byte{7}, 32), uint64(0), uint64(0), bytes.Repeat([]byte{8}, 64)},
			[]any{uint64(9), uint64(0)},
		}
	} else {
		body = []any{
			uint64(1), uint64(100), bytes.Repeat([]byte{3}, 32),
			bytes.Repeat([]byte{4}, 32), bytes.Repeat([]byte{5}, 32),
			vrf, vrf, bodySize, bodyHash,
			bytes.Repeat([]byte{7}, 32), uint64(0), uint64(0), bytes.Repeat([]byte{8}, 64),
			uint64(2), uint64(0),
		}
	}
	return []any{body, bytes.Repeat([]byte{9}, 448)}
}

// nsBlockCbor assembles a block whose header carries the real body hash and
// size, because block decode verifies the hash before the witness sets are
// inspected.
func nsBlockCbor(t *testing.T, era nsEra, body map[uint64]any, ws map[uint64]any) []byte {
	t.Helper()
	parts := []any{[]any{body}, []any{ws}, map[uint64]any{}}
	if era.blockType >= ledger.BlockTypeAlonzo {
		parts = append(parts, []any{})
	}
	var hashes []byte
	var size uint64
	raws := make([]any, 0, len(parts)+1)
	raws = append(raws, nil)
	for _, part := range parts {
		raw := nsMustEncode(t, part)
		sum := blake2b.Sum256(raw)
		hashes = append(hashes, sum[:]...)
		size += uint64(len(raw))
		raws = append(raws, cbor.RawMessage(raw))
	}
	bodyHash := blake2b.Sum256(hashes)
	raws[0] = nsHeader(era, size, bodyHash[:])
	return nsMustEncode(t, raws)
}

// nsRefScriptBody carries the script as a Babbage/Conway reference script.
func nsRefScriptBody(t *testing.T, era nsEra, script any) map[uint64]any {
	t.Helper()
	body := nsBody(era)
	scriptRef := nsMustEncode(t, []any{uint64(0), script})
	body[1] = []any{map[uint64]any{
		0: append([]byte{0x61}, nsKeyHash(0x11)...),
		1: uint64(2_000_000),
		3: cbor.Tag{Number: 24, Content: scriptRef},
	}}
	return body
}

type nsCase struct {
	name string
	era  nsEra
	// script is placed in the witness set, or in an output reference script
	// when ref is set.
	script any
	ref    bool
}

func (c nsCase) parts(t *testing.T) (map[uint64]any, map[uint64]any) {
	t.Helper()
	if c.ref {
		return nsRefScriptBody(t, c.era, c.script), map[uint64]any{}
	}
	return nsBody(c.era), nsWitnessSet(c.script)
}

func (c nsCase) tx(t *testing.T) ([]byte, error) {
	t.Helper()
	body, ws := c.parts(t)
	_, err := safedecode.Transaction(c.era.txType, nsTxCbor(t, c.era, body, ws))
	return nil, err
}

func (c nsCase) block(t *testing.T) error {
	t.Helper()
	body, ws := c.parts(t)
	_, err := ledger.NewBlockFromCbor(c.era.blockType, nsBlockCbor(t, c.era, body, ws))
	return err
}

// requireRejectedAtDecode asserts both production decode paths refuse the
// case with an error containing want.
func (c nsCase) requireRejectedAtDecode(t *testing.T, want string) {
	t.Helper()
	_, txErr := c.tx(t)
	require.Error(t, txErr, "transaction decode must reject")
	require.ErrorContains(t, txErr, want)
	blockErr := c.block(t)
	require.Error(t, blockErr, "block decode must reject")
	require.ErrorContains(t, blockErr, want)
}

func (c nsCase) requireAcceptedAtDecode(t *testing.T) {
	t.Helper()
	_, txErr := c.tx(t)
	require.NoError(t, txErr, "transaction decode must accept")
	require.NoError(t, c.block(t), "block decode must accept")
}

const nsCtorErr = "is not supported in this era"

// dingo #4550
func TestNativeScriptConstructorDomainShelleyRejectsAllegraTimelocks(t *testing.T) {
	t.Parallel()
	key := nsKeyHash(0x22)
	scripts := map[string]any{
		// slot 0 is satisfied by every Shelley transaction
		"invalid_before slot 0": nsBefore(0),
		// TTL 1000 is at or below the bound
		"invalid_hereafter compatible ttl": nsAfter(5000),
		"nested in all-of":                 nsAll(nsSig(key), nsBefore(0)),
		"nested two deep":                  nsAll(nsAny(nsSig(key), nsAfter(5000))),
		"constructor 6":                    nsGuard(key),
	}
	for name, script := range scripts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			nsCase{era: nsShelley, script: script}.requireRejectedAtDecode(t, nsCtorErr)
		})
	}
}

// dingo #4550
func TestNativeScriptConstructorDomainPostShelleyRejectsGuardUnderAnyOf(t *testing.T) {
	t.Parallel()
	key := nsKeyHash(0x22)
	script := nsAny(nsSig(key), nsGuard(key))
	for _, era := range nsPostShelley {
		t.Run(era.name+"/witness", func(t *testing.T) {
			t.Parallel()
			nsCase{era: era, script: script}.requireRejectedAtDecode(t, nsCtorErr)
		})
	}
	for _, era := range []nsEra{nsBabbage, nsConway} {
		t.Run(era.name+"/reference script", func(t *testing.T) {
			t.Parallel()
			nsCase{era: era, script: script, ref: true}.requireRejectedAtDecode(t, nsCtorErr)
		})
	}
}

// dingo #4550: positive controls.
func TestNativeScriptConstructorDomainAcceptsInEraConstructors(t *testing.T) {
	t.Parallel()
	key := nsKeyHash(0x22)
	for _, era := range nsPostShelley {
		for name, script := range map[string]any{
			"invalid_before":    nsBefore(0),
			"invalid_hereafter": nsAfter(5000),
			"nested timelocks":  nsAll(nsSig(key), nsBefore(1), nsAfter(5000)),
			"signature only":    nsSig(key),
			"any of signatures": nsAny(nsSig(key), nsSig(nsKeyHash(0x33))),
			"three-arg nofk":    nsNofK(1, nsSig(key)),
		} {
			t.Run(era.name+"/"+name, func(t *testing.T) {
				t.Parallel()
				nsCase{era: era, script: script}.requireAcceptedAtDecode(t)
			})
		}
	}
	for _, era := range nsAllEras {
		t.Run(era.name+"/shelley constructors", func(t *testing.T) {
			t.Parallel()
			nsCase{era: era, script: nsAll(nsSig(key), nsNofK(1, nsSig(key)))}.requireAcceptedAtDecode(t)
		})
	}
	for _, era := range []nsEra{nsBabbage, nsConway} {
		t.Run(era.name+"/reference script timelock", func(t *testing.T) {
			t.Parallel()
			nsCase{era: era, script: nsAll(nsSig(key), nsBefore(1)), ref: true}.requireAcceptedAtDecode(t)
		})
	}
}

// dingo #4552
func TestNativeScriptSignatureHashWidthRejectedAtDecode(t *testing.T) {
	t.Parallel()
	const widthErr = "invalid native script key hash"
	// H || 0x42: the trailing byte is silently dropped by a truncating decode.
	overlong := append(nsKeyHash(0x22), 0x42)
	// A 27-byte hash zero-pads to prefix || 0x00, which a key whose hash ends
	// in a zero byte satisfies.
	underlong := nsKeyHash(0x22)[:27]
	for _, era := range nsAllEras {
		for name, hash := range map[string][]byte{
			"29 bytes": overlong,
			"27 bytes": underlong,
			"empty":    {},
		} {
			t.Run(era.name+"/witness/"+name, func(t *testing.T) {
				t.Parallel()
				nsCase{era: era, script: nsSig(hash)}.requireRejectedAtDecode(t, widthErr)
			})
			t.Run(era.name+"/nested witness/"+name, func(t *testing.T) {
				t.Parallel()
				nsCase{era: era, script: nsAll(nsSig(nsKeyHash(0x22)), nsSig(hash))}.
					requireRejectedAtDecode(t, widthErr)
			})
		}
		t.Run(era.name+"/canonical 28 bytes", func(t *testing.T) {
			t.Parallel()
			nsCase{era: era, script: nsSig(nsKeyHash(0x22))}.requireAcceptedAtDecode(t)
		})
	}
	for _, era := range []nsEra{nsBabbage, nsConway} {
		for name, hash := range map[string][]byte{"29 bytes": overlong, "27 bytes": underlong} {
			t.Run(era.name+"/reference script/"+name, func(t *testing.T) {
				t.Parallel()
				nsCase{era: era, script: nsSig(hash), ref: true}.requireRejectedAtDecode(t, widthErr)
			})
		}
		t.Run(era.name+"/reference script/canonical", func(t *testing.T) {
			t.Parallel()
			nsCase{era: era, script: nsSig(nsKeyHash(0x22)), ref: true}.requireAcceptedAtDecode(t)
		})
	}
}

// nsEvaluate runs the Shelley native-script rule over every transaction
// decoded from the block and from the standalone transaction.
func nsEvaluate(t *testing.T, c nsCase, slot uint64) []error {
	t.Helper()
	body, ws := c.parts(t)
	tx, err := safedecode.Transaction(c.era.txType, nsTxCbor(t, c.era, body, ws))
	require.NoError(t, err)
	block, err := ledger.NewBlockFromCbor(c.era.blockType, nsBlockCbor(t, c.era, body, ws))
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 1)
	return []error{
		shelley.UtxoValidateNativeScripts(tx, slot, nil, nil),
		shelley.UtxoValidateNativeScripts(block.Transactions()[0], slot, nil, nil),
	}
}

// dingo #4553
func TestShelleyNegativeAndZeroNofKAreSatisfied(t *testing.T) {
	t.Parallel()
	key := nsKeyHash(0x22)
	scripts := map[string]any{
		"negative, no children": nsNofK(-1),
		"negative with child":   nsNofK(-1, nsSig(key)),
		"zero, no children":     nsNofK(0),
		"zero with child":       nsNofK(0, nsSig(key)),
		"negative nested":       nsAll(nsNofK(-5, nsSig(key)), nsAny(nsNofK(-1))),
	}
	for name, script := range scripts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, err := range nsEvaluate(t, nsCase{era: nsShelley, script: script}, 10) {
				require.NoError(t, err, "no witness key is present, yet the script must be satisfied")
			}
		})
	}
}

// dingo #4553: an unmet positive threshold is a script failure, not a decode
// failure.
func TestShelleyUnmetNofKIsScriptFailureNotDecodeFailure(t *testing.T) {
	t.Parallel()
	key := nsKeyHash(0x22)
	for name, script := range map[string]any{
		"1-of-1 unmet":         nsNofK(1, nsSig(key)),
		"2-of-2 unmet":         nsNofK(2, nsSig(key), nsSig(nsKeyHash(0x33))),
		"nested unmet":         nsAll(nsNofK(-1), nsNofK(1, nsSig(key))),
		"positive no children": nsNofK(1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, err := range nsEvaluate(t, nsCase{era: nsShelley, script: script}, 10) {
				var failed shelley.NativeScriptFailedError
				require.True(t, errors.As(err, &failed), "want NativeScriptFailedError, got %v", err)
			}
		})
	}
}

// dingo #4553: Allegra and later decode is unchanged.
func TestNegativeNofKAcceptedFromAllegra(t *testing.T) {
	t.Parallel()
	for _, era := range nsPostShelley {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			nsCase{era: era, script: nsNofK(-1, nsSig(nsKeyHash(0x22)))}.requireAcceptedAtDecode(t)
		})
	}
}

// dingo #4550: Dijkstra still decodes constructor 6. The witness-set field
// layout differs from the earlier eras, so it is checked on the transaction
// path only.
func TestNativeScriptConstructorDomainDijkstraAcceptsGuard(t *testing.T) {
	t.Parallel()
	key := nsKeyHash(0x22)
	body := nsBody(nsConway)
	ws := nsWitnessSet(nsAny(nsSig(key), nsGuard(key)))
	data := nsMustEncode(t, []any{body, ws, true, nil})
	_, err := safedecode.Transaction(ledger.TxTypeDijkstra, data)
	require.NoError(t, err)
}
