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

// Command dmq-submit submits one CBOR-encoded DMQ message to a dmq-node's
// local socket and prints the verdict. The interoperability tests run it
// inside the dmq-node container, where that socket lives.
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagesubmission"
)

const verdictTimeout = 30 * time.Second

func main() {
	if len(os.Args) != 4 {
		fmt.Println("usage: dmq-submit <socket> <network-magic> <message.cbor>")
		os.Exit(2)
	}
	magic, err := strconv.ParseUint(os.Args[2], 10, 32)
	exitOn(err)
	// #nosec G703 -- a test helper that reads the file its caller names
	raw, err := os.ReadFile(os.Args[3])
	exitOn(err)
	var msg ocommon.DmqMessage
	_, err = cbor.Decode(raw, &msg)
	exitOn(err)

	verdict := make(chan string, 1)
	conn, err := ouroboros.NewConnection(
		ouroboros.WithDMQ(true),
		// #nosec G115 -- ParseUint above bounds it to 32 bits
		ouroboros.WithNetworkMagic(uint32(magic)),
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
	exitOn(err)
	exitOn(conn.Dial("unix", os.Args[1]))
	defer conn.Close()
	exitOn(conn.LocalMessageSubmission().Client.SubmitMessage(&msg))
	select {
	case v := <-verdict:
		fmt.Println(v)
	case <-time.After(verdictTimeout):
		fmt.Println("TIMEOUT: no verdict")
		os.Exit(1)
	}
}

func exitOn(err error) {
	if err != nil {
		fmt.Println("ERROR:", err)
		os.Exit(1)
	}
}
