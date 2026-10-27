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
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagenotification"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagesubmission"
	"github.com/stretchr/testify/require"
)

// These tests run a real Dingo binary with dmq.enabled, which run.sh starts
// natively in node mode, beside a real dmq-node. Unlike the others they use no
// in-process stack for the Dingo side: the node dials dmq-node from its
// topology, accepts dmq-node's connection on its peer port, and authenticates
// every message against its own ledger.

type nodeEnvironment struct {
	// socket is the node's DMQ local socket.
	socket string
	// submit is the dmq-submit helper built for this machine.
	submit string
	// metrics is the node's Prometheus endpoint.
	metrics string
}

func loadNodeEnvironment(t *testing.T) nodeEnvironment {
	t.Helper()
	node := nodeEnvironment{
		socket:  os.Getenv("DMQ_INTEROP_NODE_SOCKET"),
		submit:  os.Getenv("DMQ_INTEROP_NODE_SUBMIT"),
		metrics: os.Getenv("DMQ_INTEROP_NODE_METRICS"),
	}
	if node.socket == "" || node.submit == "" || node.metrics == "" {
		t.Skip("DMQ_INTEROP_NODE_* is not set; run run.sh with " +
			"DMQ_INTEROP_NODE_MODE=1")
	}
	return node
}

// metricSum is the sum of a metric's series whose line contains match, or
// zero when there are none. match may be empty.
func (n nodeEnvironment) metricSum(
	t *testing.T,
	name, match string,
) float64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.metrics, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var sum float64
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, name+"{") &&
			!strings.HasPrefix(line, name+" ") {
			continue
		}
		if match != "" && !strings.Contains(line, match) {
			continue
		}
		fields := strings.Fields(line)
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		require.NoError(t, err, line)
		sum += value
	}
	require.NoError(t, scanner.Err())
	return sum
}

// submitToLocalSocket submits a message to the node's local socket with the
// helper and returns its verdict.
func (n nodeEnvironment) submitToLocalSocket(
	t *testing.T,
	msg ocommon.DmqMessage,
	name string,
) string {
	t.Helper()
	raw, err := cbor.Encode(msg)
	require.NoError(t, err)
	file := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(file, raw, 0o600))
	out, err := exec.Command(
		n.submit, n.socket, fmt.Sprint(dmqNetworkMagic), file,
	).CombinedOutput()
	if err != nil {
		t.Logf("dmq-submit: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// consumerHolds connects to the node's local socket as a notification
// consumer and reports whether the node holds the message, which is what a
// consumer such as a Mithril aggregator would see.
func (n nodeEnvironment) consumerHolds(
	t *testing.T,
	id []byte,
	within time.Duration,
) bool {
	t.Helper()
	replies := make(chan []ocommon.DmqMessage, 64)
	conn, err := ouroboros.NewConnection(
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
			),
		),
		ouroboros.WithLocalMessageNotificationConfig(
			localmessagenotification.NewConfig(
				localmessagenotification.WithAuthenticator(
					ocommon.NewNoOpAuthenticator(nil),
				),
				localmessagenotification.WithTTLValidator(
					ocommon.NewNoOpTTLValidator(nil),
				),
				localmessagenotification.WithReplyMessagesFunc(
					func(
						_ localmessagenotification.CallbackContext,
						msgs []ocommon.DmqMessage,
						_ bool,
					) {
						replies <- msgs
					},
				),
			),
		),
	)
	require.NoError(t, err)
	require.NoError(t, conn.Dial("unix", n.socket))
	defer conn.Close()

	client := conn.LocalMessageNotification().Client
	deadline := time.After(within)
	poll := time.NewTicker(500 * time.Millisecond)
	defer poll.Stop()
	for {
		if err := client.RequestMessagesNonBlocking(); err != nil {
			t.Logf("request messages: %v", err)
			return false
		}
		select {
		case msgs := <-replies:
			for _, msg := range msgs {
				if string(msg.ID()) == string(id) {
					return true
				}
			}
		case <-deadline:
			return false
		case <-poll.C:
		}
	}
}

// retryUntil runs attempt until it reports success or the environment's
// timeout passes. It exists because the node answers "chain state
// unavailable", which is not a refusal of the message, until its own ledger
// has the stake dmq-node already accepts.
func retryUntil(
	t *testing.T,
	env environment,
	what string,
	attempt func(n int) bool,
) {
	t.Helper()
	deadline := time.Now().Add(env.timeout)
	for n := 0; time.Now().Before(deadline); n++ {
		if attempt(n) {
			return
		}
	}
	require.FailNow(t, "gave up: "+what)
}

// A Dingo node with DMQ enabled and a real dmq-node exchange messages through
// the node's real wiring: a message dmq-node accepted is authenticated by the
// node's ledger and reaches a consumer on its local socket, and a message the
// node accepted on its local socket is accepted by dmq-node from the node.
func TestInteropNodeExchangesMessagesWithDmqNode(t *testing.T) {
	env := loadEnvironment(t)
	node := loadNodeEnvironment(t)

	// The node's manager dials dmq-node from its topology, which its dial
	// counter shows, and dmq-node dials the node's peer port.
	retryUntil(t, env, "the node dials dmq-node", func(int) bool {
		dialed := node.metricSum(
			t, "dingo_dmq_peer_dials_total", `result="success"`,
		)
		if dialed >= 1 && node.metricSum(t, "dingo_dmq_peers", "") >= 1 {
			return true
		}
		time.Sleep(2 * time.Second)
		return false
	})

	primeNode(t, env)

	// dmq-node to the node. Each attempt signs a message of its own, because
	// the node refuses, without ending the connection, a message it cannot
	// yet check against stake, and does not ask for it again.
	var received ocommon.DmqMessage
	retryUntil(t, env, "the node receives a message from dmq-node",
		func(n int) bool {
			msg := submitUntilAccepted(t, env, fmt.Sprintf("d-to-node-%d", n))
			if node.consumerHolds(t, msg.ID(), 90*time.Second) {
				received = msg
				return true
			}
			t.Logf("attempt %d: the node does not hold the message yet", n)
			return false
		})
	require.NotEmpty(t, received.ID())
	require.Positive(
		t, node.metricSum(t, "dingo_dmq_messages_received_total", ""),
	)

	// The node to dmq-node: submitted to the node's own socket, which
	// authenticates it against the node's ledger, then pulled by dmq-node.
	// dmq-node allows a pool one message a minute.
	time.Sleep(minSigDelay + 2*time.Second)
	var sent ocommon.DmqMessage
	retryUntil(t, env, "the node accepts a message on its local socket",
		func(n int) bool {
			msg := signedMessage(t, env, fmt.Sprintf("node-to-d-%d", n))
			verdict := node.submitToLocalSocket(t, msg, "node-to-d.cbor")
			if verdict == "ACCEPTED" {
				sent = msg
				return true
			}
			t.Logf("attempt %d: the node says %q", n, verdict)
			time.Sleep(15 * time.Second)
			return false
		})

	observer := newStack(t)
	dialNode(t, env, observer)
	waitFor(t, env.timeout, "dmq-node holds the node's message",
		func() bool { return observer.Mempool().Has(sent.ID()) })
}
