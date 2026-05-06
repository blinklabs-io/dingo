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

// demo-fetch is a small CLI used by the archive-demo's demo.sh to make
// the BlockFetch step of the demo visible: it connects to a Dingo NtN
// endpoint, ChainSync-walks from origin to find a block at or past a
// requested slot, then BlockFetches that block and reports the byte
// count and elapsed time.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/archive-demo/internal/archivedemo"
)

func main() {
	addr := flag.String("addr", "localhost:3113", "Dingo NtN address (host:port)")
	name := flag.String("name", "dingo-pruning", "endpoint name shown in log lines")
	magic := flag.Uint("magic", uint(archivedemo.DefaultNetworkMagic), "network magic")
	slot := flag.Uint64("slot", 50, "fetch the first block at or after this slot")
	resolveAddr := flag.String("resolve-addr", "localhost:3111", "endpoint used to resolve the target block hash (typically the archive node)")
	resolveName := flag.String("resolve-name", "dingo-archive", "name for the resolve endpoint")
	timeout := flag.Duration("timeout", 60*time.Second, "per-step timeout")
	flag.Parse()

	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "[demo-fetch] "+format+"\n", args...)
	}

	if *magic > math.MaxUint32 {
		fmt.Fprintf(os.Stderr, "FAIL: network magic %d exceeds uint32 max\n", *magic)
		os.Exit(1)
	}
	magic32 := uint32(*magic) // #nosec G115 -- bounds checked above

	resolve := archivedemo.Endpoint{Name: *resolveName, Address: *resolveAddr}
	target := archivedemo.Endpoint{Name: *name, Address: *addr}

	logf("resolving block at/after slot %d via %s...", *slot, resolve.Name)
	t0 := time.Now()
	point, err := archivedemo.FindBlockAtOrAfterSlot(
		resolve, magic32, *slot, *timeout, logf,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	logf("resolved: slot=%d hash=%x (took %s)", point.Slot, point.Hash, time.Since(t0).Round(time.Millisecond))

	logf("BlockFetch from %s for slot %d...", target.Name, point.Slot)
	t1 := time.Now()
	cbor, err := archivedemo.FetchBlock(target, magic32, point, *timeout)
	elapsed := time.Since(t1).Round(time.Millisecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL after %s: %v\n", elapsed, err)
		os.Exit(2)
	}

	fmt.Printf("OK: fetched %d bytes from %s in %s (slot=%d)\n",
		len(cbor), target.Name, elapsed, point.Slot)
}
