// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package badger

import (
	"errors"
	"strconv"
	"testing"

	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// BenchmarkValueLogGC provides a repeatable synthetic comparison of discard
// ratios. It deliberately disables the background ticker so the benchmark
// measures only the requested policy. Use -benchmem and compare each ratio's
// ns/op, allocations, and the GC metrics emitted by a registry-enabled store.
func BenchmarkValueLogGC(b *testing.B) {
	for _, ratio := range []float64{0.25, 0.5, 0.75} {
		b.Run(strconv.FormatFloat(ratio, 'f', 2, 64), func(b *testing.B) {
			store, err := New(
				WithDataDir(b.TempDir()),
				WithGc(false),
				WithValueThreshold(1),
			)
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, store.Close()) })

			for i := 0; i < b.N; i++ {
				txn := store.NewTransaction(true)
				require.NoError(b, store.Set(
					txn,
					[]byte("benchmark-key-"+strconv.Itoa(i)),
					make([]byte, 4096),
				))
				require.NoError(b, txn.Commit())
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := store.DB().RunValueLogGC(ratio)
				if err != nil && !errors.Is(err, badgerdb.ErrNoRewrite) {
					b.Fatal(err)
				}
			}
		})
	}
}
