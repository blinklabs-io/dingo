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
// ratios. It deliberately disables the background ticker and churns live keys
// through rewrites and deletes so the benchmark measures reclaimable data, not
// only ErrNoRewrite. Use -benchmem and compare each ratio's ns/op, allocations,
// and reclaimed bytes.
func BenchmarkValueLogGC(b *testing.B) {
	for _, ratio := range []float64{0.25, 0.5, 0.75} {
		b.Run(strconv.FormatFloat(ratio, 'f', 2, 64), func(b *testing.B) {
			store, err := New(
				WithDataDir(b.TempDir()),
				WithGc(false),
				WithValueThreshold(1),
				WithValueLogFileSize(1<<20),
			)
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, store.Close()) })

			for i := 0; i < 256; i++ {
				txn := store.NewTransaction(true)
				key := []byte("benchmark-key-" + strconv.Itoa(i%32))
				require.NoError(b, store.Set(txn, key, make([]byte, 4096)))
				if i%2 == 1 {
					require.NoError(b, store.Delete(
						txn,
						[]byte("benchmark-key-"+strconv.Itoa((i/2)%32)),
					))
				}
				require.NoError(b, txn.Commit())
			}
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for j := 0; j < 64; j++ {
					txn := store.NewTransaction(true)
					key := []byte("benchmark-key-" + strconv.Itoa(j))
					require.NoError(b, store.Set(txn, key, make([]byte, 4096)))
					require.NoError(b, txn.Commit())
				}
				before, err := store.DiskSize()
				require.NoError(b, err)
				b.StartTimer()
				err = store.DB().RunValueLogGC(ratio)
				b.StopTimer()
				if errors.Is(err, badgerdb.ErrNoRewrite) {
					continue
				}
				require.NoError(b, err)
				after, err := store.DiskSize()
				require.NoError(b, err)
				if before > after {
					b.ReportMetric(float64(before-after), "bytes_reclaimed")
				}
			}
		})
	}
}
