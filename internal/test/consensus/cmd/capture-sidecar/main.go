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

// capture-sidecar dials a cardano-node NtN endpoint, drives a scripted
// chainsync/blockfetch conversation, and emits a category=consensus
// JSON vector. See internal/test/consensus/README.md for the shared-
// base + per-scenario layout this binary slots into.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/consensus"
)

func main() {
	var (
		address          = flag.String("address", "", "cardano-node NtN TCP endpoint (host:port)")
		networkMagic     = flag.Uint64("network-magic", 0, "Cardano network magic (uint32)")
		conversationPath = flag.String("conversation", "", "path to capture-conversation.json")
		outPath          = flag.String("out", "", "destination path for the produced JSON vector")
		peerID           = flag.Uint64("peer-id", 0, "peer id stamped on the resulting PeerInput")
		title            = flag.String("title", "", "vector title (defaults to the conversation's name)")
		runTimeout       = flag.Duration("timeout", 90*time.Second, "overall capture deadline")
		dialTimeout      = flag.Duration("dial-timeout", 10*time.Second, "TCP dial timeout")
	)
	flag.Parse()

	if *address == "" || *conversationPath == "" || *outPath == "" {
		fmt.Fprintln(os.Stderr,
			"capture-sidecar: -address, -conversation, and -out are required",
		)
		flag.Usage()
		os.Exit(2)
	}
	if *networkMagic > math.MaxUint32 {
		fmt.Fprintf(os.Stderr,
			"capture-sidecar: -network-magic %d exceeds uint32 max\n",
			*networkMagic,
		)
		os.Exit(2)
	}

	if err := run(
		*address,
		uint32(*networkMagic), //nolint:gosec // bounds-checked above
		*conversationPath,
		*outPath,
		*peerID,
		*title,
		*runTimeout,
		*dialTimeout,
	); err != nil {
		log.Fatalf("capture-sidecar: %v", err)
	}
}

func run(
	address string,
	networkMagic uint32,
	conversationPath, outPath string,
	peerID uint64,
	title string,
	runTimeout, dialTimeout time.Duration,
) error {
	conv, err := consensus.LoadConversation(conversationPath)
	if err != nil {
		return err
	}
	cfg := consensus.Config{
		Address:          address,
		NetworkMagic:     networkMagic,
		ConversationPath: conversationPath,
		OutputPath:       outPath,
		PeerID:           peerID,
		Title:            title,
		DialTimeout:      dialTimeout,
	}
	sc := consensus.NewSidecar(cfg, conv)
	if err := sc.Connect(); err != nil {
		return err
	}
	defer func() {
		_ = sc.Close()
	}()

	// SIGINT/SIGTERM aborts the run before the timeout elapses so
	// docker compose down can shut the sidecar promptly.
	ctx, cancel := context.WithTimeout(context.Background(), runTimeout)
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	if err := sc.Run(ctx); err != nil {
		return fmt.Errorf("run: %w", err)
	}
	if err := consensus.WriteVector(outPath, sc.Vector()); err != nil {
		return err
	}
	return nil
}
