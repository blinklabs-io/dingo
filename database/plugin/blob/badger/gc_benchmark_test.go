// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package badger

import (
	"crypto/rand"
	"errors"
	"strconv"
	"testing"

	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// BenchmarkValueLogGC measures GC against a fixed-size dataset with rotated
// value-log files and both overwritten and deleted values. Setup is outside
// the timed region, so the benchmark compares the GC rewrite itself.
func BenchmarkValueLogGC(b *testing.B) {
	for _, ratio := range []float64{0.25, 0.5, 0.75} {
		b.Run(strconv.FormatFloat(ratio, 'f', 2, 64), func(b *testing.B) {
			store, err := New(WithDataDir(b.TempDir()), WithGc(false), WithValueThreshold(1), WithValueLogFileSize(1<<20), WithMemTableSize(1<<20))
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, store.Close()) })
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for pass := 0; pass < 2; pass++ {
					for batch := 0; batch < 5; batch++ {
						txn := store.DB().NewTransaction(true)
						for j := 0; j < 20; j++ {
							key := batch*20 + j
							value := make([]byte, 32<<10)
							_, err = rand.Read(value)
							require.NoError(b, err)
							entry := badgerdb.NewEntry([]byte("benchmark-key-"+strconv.Itoa(key)), value)
							if pass == 0 {
								entry.ExpiresAt = 1
							}
							require.NoError(b, txn.SetEntry(entry))
						}
						require.NoError(b, txn.Commit())
					}
				}
				for batch := 0; batch < 100; batch++ {
					txn := store.DB().NewTransaction(true)
					for j := 0; j < 1000; j++ {
						key := batch*1000 + j
						require.NoError(b, txn.SetEntry(badgerdb.NewEntry([]byte("benchmark-filler-"+strconv.Itoa(key)), []byte{1})))
					}
					require.NoError(b, txn.Commit())
				}
				for batch := 0; batch < 3; batch++ {
					txn := store.DB().NewTransaction(true)
					for j := 0; j < 20; j++ {
						key := batch*20 + j
						if key >= 45 {
							continue
						}
						require.NoError(b, txn.Delete([]byte("benchmark-key-"+strconv.Itoa(key))))
					}
					require.NoError(b, txn.Commit())
				}
				require.NoError(b, store.DB().Flatten(10))
				require.NoError(b, store.DB().Sync())
				b.StartTimer()
				successes := 0
				reclaimed := int64(0)
				for attempts := 0; attempts < 32; attempts++ {
					passBefore, sizeErr := store.DiskSize()
					require.NoError(b, sizeErr)
					err = store.DB().RunValueLogGC(ratio)
					if errors.Is(err, badgerdb.ErrNoRewrite) {
						continue
					}
					require.NoError(b, err)
					successes++
					passAfter, sizeErr := store.DiskSize()
					require.NoError(b, sizeErr)
					if passBefore > passAfter && passBefore-passAfter > reclaimed {
						reclaimed = passBefore - passAfter
					}
				}
				b.StopTimer()
				require.Greater(b, successes, 0, "GC did not perform a successful rewrite")
				if reclaimed > 0 {
					b.ReportMetric(float64(reclaimed), "bytes_reclaimed")
				}
			}
		})
	}
}
