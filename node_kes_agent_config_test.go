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

package dingo

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/stretchr/testify/require"
)

// writeKesAgentFrame/readKesAgentFrame speak the bursa KES agent wire format
// (4-byte big-endian length prefix + JSON payload) directly, independent of
// the kesagent package's own unexported framing helpers -- exactly what any
// other implementation of the protocol (this fake agent included) has to do.
func writeKesAgentFrame(t testing.TB, conn net.Conn, v any) {
	t.Helper()
	payload, err := json.Marshal(v)
	require.NoError(t, err)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	_, err = conn.Write(hdr[:])
	require.NoError(t, err)
	_, err = conn.Write(payload)
	require.NoError(t, err)
}

func readKesAgentFrame(conn net.Conn, v any) error {
	var hdr [4]byte
	if _, err := readFullFrame(conn, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	buf := make([]byte, n)
	if _, err := readFullFrame(conn, buf); err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

func readFullFrame(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// devnetOpCertCBOR extracts the raw CBOR bytes backing the devnet opcert
// text envelope, which is what KeyPush.OpCert carries directly on the wire.
func devnetOpCertCBOR(t testing.TB) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(devnetKeysDir, "opcert.cert"))
	require.NoError(t, err)
	var envelope struct {
		CborHex string `json:"cborHex"`
	}
	require.NoError(t, json.Unmarshal(data, &envelope))
	raw, err := hex.DecodeString(envelope.CborHex)
	require.NoError(t, err)
	return raw
}

func newTestNodeForBPWithAgent(
	t *testing.T,
	vrf, opcert string,
	mode string,
	socketPath string,
	cardanoCfg *cardano.CardanoNodeConfig,
) *Node {
	t.Helper()
	n := newTestNodeForBP(t, true, vrf, "", opcert, cardanoCfg)
	n.config.shelleyKESKey = ""
	n.config.shelleyKESAgentSocket = socketPath
	n.config.shelleyKESAgentMode = mode
	return n
}

// TestValidateBlockProducerStartup_KESAgentServeKeyMode proves
// validateBlockProducerStartupAtSlot installs credentials sourced entirely
// from a fake bursa KES agent in serve-key mode, using the real wire
// protocol against real devnet KES key material.
func TestValidateBlockProducerStartup_KESAgentServeKeyMode(t *testing.T) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)
	kesKeyData, err := bursa.LoadKeyFromFile(
		filepath.Join(devnetKeysDir, "kes.skey"),
	)
	require.NoError(t, err)
	opCertCBOR := devnetOpCertCBOR(t)

	sockPath := filepath.Join(t.TempDir(), "kes-agent.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		writeKesAgentFrame(t, conn, kesagent.Hello{
			Protocol: kesagent.ProtocolID,
			Mode:     kesagent.ModeServeKey,
		})
		writeKesAgentFrame(t, conn, kesagent.KeyPush{
			Type:       "key_push",
			Period:     0,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: kesKeyData.SKey,
			KESVKey:    kesKeyData.VKey,
			OpCert:     opCertCBOR,
		})
		// Keep the connection open for the background Run loop until the
		// test closes the client.
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		opcert,
		kesagent.ModeServeKey,
		sockPath,
		cardanoCfg,
	)
	t.Cleanup(n.closeKESAgentClient)

	creds, err := n.validateBlockProducerStartupAtSlot(0)
	require.NoError(t, err)
	require.True(t, creds.IsLoaded())
	require.NotNil(t, n.kesAgentClient)
}

// TestValidateBlockProducerStartup_KESAgentSignMode proves
// validateBlockProducerStartupAtSlot installs sign-mode credentials backed by
// a fake agent that signs real requests with the real devnet KES key,
// exercising the client's response verification against genuine signatures.
func TestValidateBlockProducerStartup_KESAgentSignMode(t *testing.T) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)
	kesKeyData, err := bursa.LoadKeyFromFile(
		filepath.Join(devnetKeysDir, "kes.skey"),
	)
	require.NoError(t, err)

	sockPath := filepath.Join(t.TempDir(), "kes-agent.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		writeKesAgentFrame(t, conn, kesagent.Hello{
			Protocol: kesagent.ProtocolID,
			Mode:     kesagent.ModeSign,
		})
		var req kesagent.SignRequest
		if err := readKesAgentFrame(conn, &req); err != nil {
			return
		}
		sk := &kes.SecretKey{
			Depth:  kes.CardanoKesDepth,
			Period: req.Period, // devnet opcert KESPeriod is 0
			Data:   append([]byte(nil), kesKeyData.SKey...),
		}
		sig, signErr := kes.Sign(sk, req.Period, req.Message)
		if signErr != nil {
			return
		}
		writeKesAgentFrame(t, conn, kesagent.SignResponse{
			Type:      "sign_response",
			Period:    req.Period,
			Signature: sig,
		})
	}()

	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		opcert,
		kesagent.ModeSign,
		sockPath,
		cardanoCfg,
	)
	t.Cleanup(n.closeKESAgentClient)

	creds, err := n.validateBlockProducerStartupAtSlot(0)
	require.NoError(t, err)
	require.True(t, creds.IsLoaded())
	require.NotNil(t, n.kesAgentClient)
}
