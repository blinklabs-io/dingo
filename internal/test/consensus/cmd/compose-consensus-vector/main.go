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

// compose-consensus-vector merges N single-peer captures plus one
// observation-node capture into a multi-peer consensus vector and,
// optionally, diffs the result against a committed golden.
//
// Each input is a category=consensus vector produced by a single
// capture-sidecar invocation against one cardano-node — its
// capture.peers[] has exactly one entry. The composer assigns peer
// IDs by input order and lifts the observation node's served trace
// into expected_output.downstream_chainsync.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/blinklabs-io/dingo/internal/test/consensus"
)

func main() {
	var peerCaptures stringSlice
	flag.Var(&peerCaptures, "peer",
		"path to a single-peer capture (repeat once per upstream peer; "+
			"order determines peer_id)")
	observationCapture := flag.String("observation", "",
		"path to the observation-node capture")
	title := flag.String("title", "",
		"composed vector title (defaults to <multi-peer-<N>>)")
	outPath := flag.String("out", "",
		"destination path for the composed vector")
	goldenPath := flag.String("golden", "",
		"committed golden vector to diff against; non-zero exit on mismatch")
	flag.Parse()

	if len(peerCaptures) == 0 ||
		*observationCapture == "" ||
		*outPath == "" {
		fmt.Fprintln(os.Stderr,
			"compose-consensus-vector: -peer (at least one), "+
				"-observation, and -out are required",
		)
		flag.Usage()
		os.Exit(2)
	}

	vec, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       peerCaptures,
		ObservationCapturePath: *observationCapture,
		Title:                  *title,
	})
	if err != nil {
		log.Fatalf("compose-consensus-vector: %v", err)
	}
	if err := consensus.WriteVector(*outPath, vec); err != nil {
		log.Fatalf("compose-consensus-vector: %v", err)
	}

	if *goldenPath != "" {
		diff, err := consensus.DiffAgainstGolden(*goldenPath, vec)
		if err != nil {
			log.Fatalf("compose-consensus-vector: golden: %v", err)
		}
		if !diff.Match {
			fmt.Fprintln(os.Stderr,
				"compose-consensus-vector: golden mismatch")
			for _, d := range diff.Differences {
				fmt.Fprintln(os.Stderr, "  - "+d)
			}
			os.Exit(1)
		}
	}
}

// stringSlice implements flag.Value for a repeatable string flag.
type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", []string(*s))
}

func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}
