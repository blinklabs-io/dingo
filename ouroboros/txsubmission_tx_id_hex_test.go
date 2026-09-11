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

package ouroboros

import (
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/stretchr/testify/require"
)

// doubleHex returns the value a %x verb produces for an operand that
// implements fmt.Stringer with a hex String method: the hex encoding of the
// hex string, twice the intended length.
func doubleHex(t *testing.T, hexId string) string {
	t.Helper()
	encoded := hex.EncodeToString([]byte(hexId))
	require.Len(t, encoded, 2*len(hexId))
	return encoded
}

// txsubmissionLoggedMessages returns the "msg" field of every JSON log record
// in buf whose message starts with prefix.
func txsubmissionLoggedMessages(
	t *testing.T,
	buf string,
	prefix string,
) []string {
	t.Helper()
	var ret []string
	for _, line := range strings.Split(buf, "\n") {
		if line == "" {
			continue
		}
		var record struct {
			Msg string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if strings.HasPrefix(record.Msg, prefix) {
			ret = append(ret, record.Msg)
		}
	}
	return ret
}

// TestTxSubmissionServerInitRejectionLogsSingleHexTxId is the regression test
// for the mempool-rejection log line carrying a double-hex-encoded
// transaction id.
//
// tx.Hash() is an lcommon.Blake2b256, which implements fmt.Stringer with a hex
// String method, and fmt routes the x verb through String for such operands.
// Formatting the hash value itself with %x therefore hex-encoded its hex
// string, producing a 128-character id in the message prefix that matches no
// real transaction hash -- defeating the obvious use of the line, which is to
// grep for a transaction id seen on the wire or on chain. The 64-character id
// must appear in the message itself, not only inside the wrapped validation
// error further along the line.
func TestTxSubmissionServerInitRejectionLogsSingleHexTxId(t *testing.T) {
	fixtures := txsubmissionTestFixtures(t)
	rejected := fixtures[0]
	require.Len(t, rejected.hash, 64)

	logBuf := &lockedBuffer{}
	logger := slog.New(
		slog.NewJSONHandler(
			logBuf,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		logger: logger,
		validatorA: txsubmissionSelectiveRejectingValidator{
			rejectedHash: rejected.hash,
		},
	})
	defer h.close(t)

	addTxSubmissionTestFixtures(t, h.mB, rejected)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	const prefix = "failed to add tx "
	var messages []string
	require.Eventually(
		t,
		func() bool {
			messages = txsubmissionLoggedMessages(t, logBuf.String(), prefix)
			return len(messages) > 0
		},
		5*time.Second,
		10*time.Millisecond,
		"expected the mempool rejection to be logged",
	)

	for _, msg := range messages {
		require.True(
			t,
			strings.HasPrefix(msg, prefix+rejected.hash+" to mempool: "),
			"rejection message must name the transaction by its 64-character id, got %q",
			msg,
		)
		require.NotContains(
			t,
			msg,
			doubleHex(t, rejected.hash),
			"transaction id must not be hex-encoded twice",
		)
	}
}

// TestValidateTxsubmissionReplyMismatchReportsSingleHexTxId covers the second
// %x-on-a-Stringer site in this file: the reply hash/order mismatch error also
// formatted the Blake2b256 value directly, so the id an operator would search
// for was double-hex encoded.
func TestValidateTxsubmissionReplyMismatchReportsSingleHexTxId(t *testing.T) {
	fixtures := txsubmissionTestFixtures(t)
	requested := []txsubmission.TxIdAndSize{{
		TxId: fixtures[0].txId,
		Size: uint32(len(fixtures[0].body)), // #nosec G115 -- test fixture
	}}
	returned := []txsubmission.TxBody{{
		EraId:  fixtures[1].txId.EraId,
		TxBody: fixtures[1].body,
	}}

	_, err := validateTxsubmissionReply(requested, returned)
	require.Error(t, err)
	require.Contains(t, err.Error(), "received "+fixtures[1].hash)
	require.NotContains(t, err.Error(), doubleHex(t, fixtures[1].hash))
}
