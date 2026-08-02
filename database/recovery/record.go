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

package recovery

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// recordMagic prefixes every framed record so a reader can tell a truncated
// tail apart from a stream that never held records at all.
var recordMagic = [4]byte{'D', 'W', 'A', 'L'}

// recordVersion is the on-disk framing version. Bump it only for changes that
// an older reader cannot skip; readers reject frames they do not understand
// rather than guessing at their payload.
const recordVersion uint8 = 1

// maxPayloadBytes caps a single decoded payload. A journal record describes one
// commit intent, so anything approaching this is a corrupt length field rather
// than a legitimate record, and without the cap a garbage length would drive a
// multi-gigabyte allocation during replay.
const maxPayloadBytes = 1 << 20

// maxHashBytes caps the hash length prefix inside a payload for the same
// reason. Block hashes are 32 bytes; the limit only has to exclude nonsense.
const maxHashBytes = 64

// castagnoli is the CRC table used for record checksums. It detects the torn
// final write an unclean shutdown leaves behind, which is the failure this
// package exists to survive.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrCorruptRecord reports a record whose framing or checksum did not
	// validate. Replay treats it as the end of usable data.
	ErrCorruptRecord = errors.New("corrupt recovery record")
	// ErrShortRecord reports a record that ended before its framing said it
	// would, the normal shape of a crash during append.
	ErrShortRecord = errors.New("truncated recovery record")
	// ErrUnknownVersion reports a framing version this build cannot decode.
	ErrUnknownVersion = errors.New("unknown recovery record version")
)

// RecordType identifies what a journal record asserts.
type RecordType uint8

const (
	// RecordTypeBegin declares the intent of a cross-store commit that is
	// about to touch the stores.
	RecordTypeBegin RecordType = 1
	// RecordTypeCommit marks the matching begin as fully applied to both
	// stores.
	RecordTypeCommit RecordType = 2
	// RecordTypeAbort marks the matching begin as rolled back before either
	// store committed.
	RecordTypeAbort RecordType = 3
	// RecordTypeCheckpoint records a merkle-rooted state summary inline in
	// the journal, alongside the copy in the checkpoint store.
	RecordTypeCheckpoint RecordType = 4
)

// String renders a record type for logs.
func (t RecordType) String() string {
	switch t {
	case RecordTypeBegin:
		return "begin"
	case RecordTypeCommit:
		return "commit"
	case RecordTypeAbort:
		return "abort"
	case RecordTypeCheckpoint:
		return "checkpoint"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

// valid reports whether the type is one this build knows how to decode.
func (t RecordType) valid() bool {
	switch t {
	case RecordTypeBegin,
		RecordTypeCommit,
		RecordTypeAbort,
		RecordTypeCheckpoint:
		return true
	default:
		return false
	}
}

// IntentKind describes the logical operation a cross-store commit performs.
type IntentKind uint8

const (
	// IntentUnknown is a commit that did not describe itself. Recovery can
	// still use its commit timestamp as a fence, it just cannot name the
	// point that was in flight.
	IntentUnknown IntentKind = 0
	// IntentBlockAdd extends the chain to the recorded point.
	IntentBlockAdd IntentKind = 1
	// IntentRollback rewinds state to the recorded point.
	IntentRollback IntentKind = 2
)

// String renders an intent kind for logs.
func (k IntentKind) String() string {
	switch k {
	case IntentBlockAdd:
		return "block_add"
	case IntentRollback:
		return "rollback"
	case IntentUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(k))
	}
}

// Point identifies a position on the chain.
type Point struct {
	Hash []byte
	Slot uint64
}

// Equal reports whether two points name the same block.
func (p Point) Equal(other Point) bool {
	return p.Slot == other.Slot && bytes.Equal(p.Hash, other.Hash)
}

// IsZero reports whether the point is unset.
func (p Point) IsZero() bool {
	return p.Slot == 0 && len(p.Hash) == 0
}

// Intent describes what a cross-store commit is about to do. Recovery reads it
// to report, and repair, the exact operation a crash interrupted.
type Intent struct {
	Hash        []byte
	Slot        uint64
	BlockNumber uint64
	Kind        IntentKind
}

// Point returns the chain position the intent targets.
func (i Intent) Point() Point {
	return Point{Slot: i.Slot, Hash: i.Hash}
}

// Record is one decoded journal entry.
type Record struct {
	// Checkpoint is set only on RecordTypeCheckpoint records.
	Checkpoint *Checkpoint
	// Intent is meaningful only on RecordTypeBegin records.
	Intent Intent
	// Seq numbers the commit this record belongs to. Begin, and the commit
	// or abort that resolves it, share a Seq.
	Seq uint64
	// CommitTimestamp is the cross-store fence the commit writes into both
	// stores. It is meaningful only on RecordTypeBegin records.
	CommitTimestamp int64
	Type            RecordType
}

// encoder builds a payload with fixed-width, big-endian fields so records sort
// and compare the same on every platform.
//
// It carries its own error rather than returning one per field, so a caller
// writes a record as a straight run of field calls and checks once at the end.
type encoder struct {
	buf []byte
	err error
}

func (e *encoder) uint64(v uint64) {
	e.buf = binary.BigEndian.AppendUint64(e.buf, v)
}

// int64 stores a signed value in the unsigned wire field. The conversion is a
// deliberate two's-complement round trip, undone by decoder.int64.
func (e *encoder) int64(v int64) {
	e.uint64(uint64(v)) //nolint:gosec // round-trips via decoder.int64
}

func (e *encoder) uint8(v uint8) {
	e.buf = append(e.buf, v)
}

// bytesField writes a length-prefixed byte slice, refusing anything the
// single-byte length prefix cannot describe.
func (e *encoder) bytesField(v []byte) {
	if e.err != nil {
		return
	}
	if len(v) > maxHashBytes {
		e.err = fmt.Errorf(
			"byte field length %d exceeds %d",
			len(v),
			maxHashBytes,
		)
		return
	}
	e.uint8(uint8(len(v))) //nolint:gosec // bounded by maxHashBytes above
	e.buf = append(e.buf, v...)
}

// decoder reads the fields an encoder wrote, reporting ErrCorruptRecord as soon
// as the payload runs out rather than reading past the end.
type decoder struct {
	buf []byte
	err error
}

func (d *decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if len(d.buf) < n {
		d.err = fmt.Errorf("%w: payload shorter than expected", ErrCorruptRecord)
		return nil
	}
	out := d.buf[:n]
	d.buf = d.buf[n:]
	return out
}

func (d *decoder) uint64() uint64 {
	b := d.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// int64 undoes the two's-complement round trip encoder.int64 performs.
func (d *decoder) int64() int64 {
	return int64(d.uint64()) //nolint:gosec // inverse of encoder.int64
}

func (d *decoder) uint8() uint8 {
	b := d.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}

// bytesField reads a length-prefixed byte slice into a fresh allocation so the
// result does not alias the read buffer the caller may reuse.
func (d *decoder) bytesField() []byte {
	n := int(d.uint8())
	if d.err != nil {
		return nil
	}
	if n > maxHashBytes {
		d.err = fmt.Errorf(
			"%w: hash field length %d exceeds %d",
			ErrCorruptRecord,
			n,
			maxHashBytes,
		)
		return nil
	}
	if n == 0 {
		return nil
	}
	b := d.take(n)
	if b == nil {
		return nil
	}
	return bytes.Clone(b)
}

// done reports a trailing-bytes error so a payload that decodes but does not
// account for all its bytes is treated as corrupt rather than silently
// truncated.
func (d *decoder) done() error {
	if d.err != nil {
		return d.err
	}
	if len(d.buf) != 0 {
		return fmt.Errorf(
			"%w: %d unread payload bytes",
			ErrCorruptRecord,
			len(d.buf),
		)
	}
	return nil
}

// encodePayload renders a record's type-specific payload.
func encodePayload(r Record) ([]byte, error) {
	e := &encoder{}
	switch r.Type {
	case RecordTypeBegin:
		e.uint64(r.Seq)
		e.int64(r.CommitTimestamp)
		e.uint8(uint8(r.Intent.Kind))
		e.uint64(r.Intent.Slot)
		e.uint64(r.Intent.BlockNumber)
		e.bytesField(r.Intent.Hash)
	case RecordTypeCommit, RecordTypeAbort:
		e.uint64(r.Seq)
	case RecordTypeCheckpoint:
		if r.Checkpoint == nil {
			return nil, errors.New("checkpoint record has no checkpoint")
		}
		payload, err := encodeCheckpoint(*r.Checkpoint)
		if err != nil {
			return nil, err
		}
		e.buf = payload
	default:
		return nil, fmt.Errorf("cannot encode record type %s", r.Type)
	}
	if e.err != nil {
		return nil, e.err
	}
	return e.buf, nil
}

// decodePayload parses a record's type-specific payload.
func decodePayload(t RecordType, payload []byte) (Record, error) {
	r := Record{Type: t}
	d := &decoder{buf: payload}
	switch t {
	case RecordTypeBegin:
		r.Seq = d.uint64()
		r.CommitTimestamp = d.int64()
		r.Intent.Kind = IntentKind(d.uint8())
		r.Intent.Slot = d.uint64()
		r.Intent.BlockNumber = d.uint64()
		r.Intent.Hash = d.bytesField()
	case RecordTypeCommit, RecordTypeAbort:
		r.Seq = d.uint64()
	case RecordTypeCheckpoint:
		cp, err := decodeCheckpoint(payload)
		if err != nil {
			return Record{}, err
		}
		if err := cp.Verify(); err != nil {
			return Record{}, fmt.Errorf("invalid checkpoint: %w", err)
		}
		r.Checkpoint = &cp
		r.Seq = cp.Seq
		return r, nil
	default:
		return Record{}, fmt.Errorf(
			"%w: cannot decode record type %s",
			ErrCorruptRecord,
			t,
		)
	}
	if err := d.done(); err != nil {
		return Record{}, err
	}
	return r, nil
}

// appendFrame appends the framed encoding of a record to dst.
//
// Frame layout, all integers big-endian:
//
//	magic[4] | version | type | payloadLen uint32 | payload | crc32c uint32
//
// The checksum covers version, type, length and payload, so a torn write is
// caught whether it lost payload bytes or corrupted the header.
func appendFrame(dst []byte, r Record) ([]byte, error) {
	payload, err := encodePayload(r)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxPayloadBytes {
		return nil, fmt.Errorf(
			"record payload %d bytes exceeds %d",
			len(payload),
			maxPayloadBytes,
		)
	}
	header := make([]byte, 0, 6)
	header = append(header, recordVersion, uint8(r.Type))
	//nolint:gosec // bounded by maxPayloadBytes above
	header = binary.BigEndian.AppendUint32(header, uint32(len(payload)))
	crc := crc32.Checksum(header, castagnoli)
	crc = crc32.Update(crc, castagnoli, payload)
	dst = append(dst, recordMagic[:]...)
	dst = append(dst, header...)
	dst = append(dst, payload...)
	return binary.BigEndian.AppendUint32(dst, crc), nil
}

// readFrame reads one framed record from r.
//
// It returns io.EOF at a clean end of stream, and ErrShortRecord or
// ErrCorruptRecord for a partial or damaged tail. Callers replaying a journal
// treat all three as "no more usable records" and stop; the difference only
// matters for what they log.
func readFrame(r io.Reader) (Record, error) {
	var head [10]byte
	n, err := io.ReadFull(r, head[:])
	switch {
	case errors.Is(err, io.EOF) && n == 0:
		return Record{}, io.EOF
	case err != nil:
		return Record{}, fmt.Errorf(
			"%w: header: %w",
			ErrShortRecord,
			err,
		)
	}
	if !bytes.Equal(head[:4], recordMagic[:]) {
		return Record{}, fmt.Errorf("%w: bad magic", ErrCorruptRecord)
	}
	version := head[4]
	if version != recordVersion {
		return Record{}, fmt.Errorf(
			"%w: version %d",
			ErrUnknownVersion,
			version,
		)
	}
	recType := RecordType(head[5])
	payloadLen := binary.BigEndian.Uint32(head[6:10])
	if payloadLen > maxPayloadBytes {
		return Record{}, fmt.Errorf(
			"%w: payload length %d exceeds %d",
			ErrCorruptRecord,
			payloadLen,
			maxPayloadBytes,
		)
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Record{}, fmt.Errorf(
			"%w: payload: %w",
			ErrShortRecord,
			err,
		)
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return Record{}, fmt.Errorf(
			"%w: checksum: %w",
			ErrShortRecord,
			err,
		)
	}
	want := binary.BigEndian.Uint32(crcBuf[:])
	got := crc32.Checksum(head[4:10], castagnoli)
	got = crc32.Update(got, castagnoli, payload)
	if got != want {
		return Record{}, fmt.Errorf(
			"%w: checksum %08x does not match recorded %08x",
			ErrCorruptRecord,
			got,
			want,
		)
	}
	// The checksum is verified before the type is, so a record whose type
	// byte was corrupted in transit is reported as corruption. Reaching here
	// with an unknown type means the writer was a newer build.
	if !recType.valid() {
		return Record{}, fmt.Errorf(
			"%w: record type %d",
			ErrUnknownVersion,
			uint8(recType),
		)
	}
	return decodePayload(recType, payload)
}
