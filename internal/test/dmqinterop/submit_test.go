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

//go:build dingo_dmq_interop

package dmqinterop

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagesubmission"
	"github.com/stretchr/testify/require"
)

const (
	verdictTimeout = 30 * time.Second
	// verdictPrefix marks the verdict line in the output of TestSubmitHelper,
	// which shares its output with the test framework's own lines.
	verdictPrefix = "DMQ_SUBMIT_VERDICT: "
	// submitEnvSocket, submitEnvMagic and submitEnvFile name the variables
	// that turn TestSubmitHelper from a skipped test into the submission step.
	submitEnvSocket = "DMQ_INTEROP_SUBMIT_SOCKET"
	submitEnvMagic  = "DMQ_INTEROP_SUBMIT_MAGIC"
	submitEnvFile   = "DMQ_INTEROP_SUBMIT_FILE"
)

// submitMessage submits msg to a DMQ local socket and returns the verdict:
// "ACCEPTED", "REJECTED: <reason>", or "TIMEOUT: no verdict".
func submitMessage(
	socket string,
	magic uint32,
	msg ocommon.DmqMessage,
) (string, error) {
	verdict := make(chan string, 1)
	conn, err := ouroboros.NewConnection(
		ouroboros.WithDMQ(true),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithLocalMessageSubmissionConfig(
			localmessagesubmission.NewConfig(
				localmessagesubmission.WithAuthenticator(
					ocommon.NewNoOpAuthenticator(nil),
				),
				localmessagesubmission.WithTTLValidator(
					ocommon.NewNoOpTTLValidator(nil),
				),
				localmessagesubmission.WithAcceptMessageFunc(
					func(localmessagesubmission.CallbackContext) {
						verdict <- "ACCEPTED"
					},
				),
				localmessagesubmission.WithRejectMessageFunc(
					func(
						_ localmessagesubmission.CallbackContext,
						reason ocommon.RejectReason,
					) {
						verdict <- fmt.Sprintf("REJECTED: %#v", reason)
					},
				),
			),
		),
	)
	if err != nil {
		return "", err
	}
	if err := conn.Dial("unix", socket); err != nil {
		return "", err
	}
	defer conn.Close()
	err = conn.LocalMessageSubmission().Client.SubmitMessage(&msg)
	if err != nil {
		return "", err
	}
	select {
	case v := <-verdict:
		return v, nil
	case <-time.After(verdictTimeout):
		return "TIMEOUT: no verdict", nil
	}
}

// TestSubmitHelper is the submission step the container runs. dmq-node's
// socket exists only inside its container, so run.sh builds this package's
// test binary for Linux and the tests run that binary there with the
// submitEnv* variables set. It is skipped in an ordinary run.
func TestSubmitHelper(t *testing.T) {
	t.Parallel()
	socket := os.Getenv(submitEnvSocket)
	if socket == "" {
		t.Skip(submitEnvSocket + " is not set; this is run inside dmq-node's container")
	}
	magic, err := strconv.ParseUint(os.Getenv(submitEnvMagic), 10, 32)
	require.NoError(t, err)
	raw, err := os.ReadFile(filepath.Clean(os.Getenv(submitEnvFile)))
	require.NoError(t, err)
	var msg ocommon.DmqMessage
	_, err = cbor.Decode(raw, &msg)
	require.NoError(t, err)
	// #nosec G115 -- ParseUint above bounds it to 32 bits
	verdict, err := submitMessage(socket, uint32(magic), msg)
	require.NoError(t, err)
	fmt.Println(verdictPrefix + verdict)
}

// verdictFrom returns the verdict TestSubmitHelper printed in out, or the
// whole output when it printed none.
func verdictFrom(out []byte) string {
	for line := range strings.SplitSeq(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, verdictPrefix); ok {
			return strings.TrimSpace(v)
		}
	}
	return strings.TrimSpace(string(out))
}

// serveSubmissions accepts one connection on a Unix socket and runs a DMQ
// local-message-submission server on it that answers every message with
// reject, or accepts it when reject is nil. It returns the socket path.
func serveSubmissions(
	t *testing.T,
	reject ocommon.RejectReason,
) string {
	t.Helper()
	// A short directory keeps the path under the Unix socket length limit.
	dir, err := os.MkdirTemp("", "dmqs")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", socket)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		conn, err := ouroboros.NewConnection(
			ouroboros.WithConnection(nc),
			ouroboros.WithServer(true),
			ouroboros.WithDMQ(true),
			ouroboros.WithNetworkMagic(dmqNetworkMagic),
			ouroboros.WithLocalMessageSubmissionConfig(
				localmessagesubmission.NewConfig(
					localmessagesubmission.WithAuthenticator(
						ocommon.NewNoOpAuthenticator(nil),
					),
					localmessagesubmission.WithTTLValidator(
						ocommon.NewNoOpTTLValidator(nil),
					),
					localmessagesubmission.WithSubmitMessageFunc(
						func(
							localmessagesubmission.CallbackContext,
							*ocommon.DmqMessage,
						) ocommon.RejectReason {
							return reject
						},
					),
				),
			),
		)
		if err != nil {
			return
		}
		<-conn.ErrorChan()
	}()
	return socket
}

func TestSubmitMessageVerdicts(t *testing.T) {
	t.Parallel()
	msg := ocommon.DmqMessage{
		Payload: ocommon.DmqMessagePayload{
			MessageBody: bytes.Repeat([]byte("x"), 100),
			KESPeriod:   1,
			// #nosec G115 -- test fixture timestamp
			ExpiresAt: uint32(time.Now().Add(10 * time.Minute).Unix()),
		},
		KESSignature: make([]byte, 448),
		OperationalCertificate: ocommon.OperationalCertificate{
			KESVerificationKey: make([]byte, 32),
			IssueNumber:        1,
			KESPeriod:          1,
			ColdSignature:      make([]byte, 64),
		},
		ColdVerificationKey: make([]byte, 32),
	}
	require.NoError(t, msg.SetComputedMessageID())

	t.Run("accepted", func(t *testing.T) {
		t.Parallel()
		socket := serveSubmissions(t, nil)
		verdict, err := submitMessage(socket, dmqNetworkMagic, msg)
		require.NoError(t, err)
		require.Equal(t, "ACCEPTED", verdict)
	})
	t.Run("rejected", func(t *testing.T) {
		t.Parallel()
		socket := serveSubmissions(
			t, ocommon.InvalidReason{Message: "bad signature"},
		)
		verdict, err := submitMessage(socket, dmqNetworkMagic, msg)
		require.NoError(t, err)
		require.Contains(t, verdict, "REJECTED")
		require.Contains(t, verdict, "bad signature")
	})
}
