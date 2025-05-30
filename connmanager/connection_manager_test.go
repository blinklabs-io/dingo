// Copyright 2025 Blink Labs Software
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

package connmanager_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/keepalive"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"go.uber.org/goleak"
)

/*
func TestConnectionManagerTagString(t *testing.T) {
	testDefs := map[connmanager.ConnectionManagerTag]string{
		connmanager.ConnectionManagerTagHostP2PLedger: "HostP2PLedger",
		connmanager.ConnectionManagerTagHostP2PGossip: "HostP2PGossip",
		connmanager.ConnectionManagerTagRoleInitiator: "RoleInitiator",
		connmanager.ConnectionManagerTagRoleResponder: "RoleResponder",
		connmanager.ConnectionManagerTagNone:          "Unknown",
		connmanager.ConnectionManagerTag(9999):        "Unknown",
	}
	for k, v := range testDefs {
		if k.String() != v {
			t.Fatalf(
				"did not get expected string for ID %d: got %s, expected %s",
				k,
				k.String(),
				v,
			)
		}
	}
}
*/

func TestConnectionManagerConnError(t *testing.T) {
	defer goleak.VerifyNone(t)
	var expectedConnId ouroboros.ConnectionId
	expectedErr := io.EOF
	doneChan := make(chan any)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			ConnClosedFunc: func(connId ouroboros.ConnectionId, err error) {
				if err != nil {
					if connId != expectedConnId {
						t.Fatalf(
							"did not receive error from expected connection: got %d, wanted %d",
							connId,
							expectedConnId,
						)
					}
					if !errors.Is(err, expectedErr) {
						t.Fatalf(
							"did not receive expected error: got: %s, expected: %s",
							err,
							expectedErr,
						)
					}
					close(doneChan)
				}
			},
		},
	)
	testIdx := 2
	var connIds []ouroboros.ConnectionId
	for i := range 3 {
		mockConversation := ouroboros_mock.ConversationKeepAlive
		if i == testIdx {
			mockConversation = ouroboros_mock.ConversationKeepAliveClose
		}
		mockConn := ouroboros_mock.NewConnection(
			ouroboros_mock.ProtocolRoleClient,
			mockConversation,
		)
		oConn, err := ouroboros.New(
			ouroboros.WithConnection(mockConn),
			ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
			ouroboros.WithNodeToNode(true),
			ouroboros.WithKeepAlive(true),
			ouroboros.WithKeepAliveConfig(
				keepalive.NewConfig(
					keepalive.WithCookie(ouroboros_mock.MockKeepAliveCookie),
					keepalive.WithPeriod(2*time.Second),
					keepalive.WithTimeout(1*time.Second),
				),
			),
		)
		if err != nil {
			t.Fatalf("unexpected error when creating Ouroboros object: %s", err)
		}
		if i == testIdx {
			expectedConnId = oConn.Id()
		}
		connManager.AddConnection(oConn)
		connIds = append(connIds, oConn.Id())
	}
	select {
	case <-doneChan:
		// Shutdown other connections
		for _, connId := range connIds {
			if connId != expectedConnId {
				tmpConn := connManager.GetConnectionById(connId)
				tmpConn.Close()
			}
		}
		// TODO: actually wait for shutdown
		time.Sleep(5 * time.Second)
		return
	case <-time.After(10 * time.Second):
		t.Fatalf("did not receive error within timeout")
	}
}

func TestConnectionManagerConnClosed(t *testing.T) {
	defer goleak.VerifyNone(t)
	var expectedConnId ouroboros.ConnectionId
	doneChan := make(chan any)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			ConnClosedFunc: func(connId ouroboros.ConnectionId, err error) {
				if connId != expectedConnId {
					t.Fatalf(
						"did not receive closed signal from expected connection: got %d, wanted %d",
						connId,
						expectedConnId,
					)
				}
				if err != nil {
					t.Fatalf("received unexpected error: %s", err)
				}
				close(doneChan)
			},
		},
	)
	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleClient,
		[]ouroboros_mock.ConversationEntry{
			ouroboros_mock.ConversationEntryHandshakeRequestGeneric,
			ouroboros_mock.ConversationEntryHandshakeNtNResponse,
		},
	)
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(mockConn),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(false),
	)
	if err != nil {
		t.Fatalf("unexpected error when creating Ouroboros object: %s", err)
	}
	expectedConnId = oConn.Id()
	connManager.AddConnection(oConn)
	time.AfterFunc(
		1*time.Second,
		func() {
			oConn.Close()
		},
	)
	select {
	case <-doneChan:
		// TODO: actually wait for shutdown
		time.Sleep(5 * time.Second)
		return
	case <-time.After(10 * time.Second):
		t.Fatalf("did not receive error within timeout")
	}
}
