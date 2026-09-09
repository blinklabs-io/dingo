// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//go:build dingo_extra_plugins

package aws

import (
	"encoding/binary"
	"os"
	"strings"
	"testing"
)

func TestReadBlobBodyWithLimit(t *testing.T) {
	data, err := readBlobBodyWithLimit(strings.NewReader("123"), 3)
	if err != nil || string(data) != "123" {
		t.Fatalf("read within limit = %q, %v", data, err)
	}
	if _, err := readBlobBodyWithLimit(strings.NewReader("1234"), 3); err == nil {
		t.Fatal("oversized object should be rejected")
	}
}

func TestS3TransactionStagesAndRollsBack(t *testing.T) {
	txn := &s3Txn{pending: make(map[string]s3PendingChange)}
	txn.stageSet([]byte("key"), []byte("value"))
	value, deleted, staged := txn.stagedValue([]byte("key"))
	if !staged || deleted || string(value) != "value" {
		t.Fatalf(
			"staged value = %q, deleted=%v, staged=%v",
			value,
			deleted,
			staged,
		)
	}
	txn.stageDelete([]byte("key"))
	value, deleted, staged = txn.stagedValue([]byte("key"))
	if !staged || !deleted || value != nil {
		t.Fatalf(
			"staged delete = %q, deleted=%v, staged=%v",
			value,
			deleted,
			staged,
		)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	if txn.pending != nil {
		t.Fatal("rollback should discard pending changes")
	}
}

func TestReverseKeyFile(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	f := &reverseKeyFile{file: file}
	for _, key := range []string{"a", "b", "c"} {
		if err := writeReverseKey(file, key); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"c", "b", "a"} {
		got, valid, err := f.nextReverse()
		if err != nil || !valid || got != want {
			t.Fatalf("reverse key = %q, %v, %v", got, valid, err)
		}
	}
	if _, valid, err := f.nextReverse(); err != nil || valid {
		t.Fatalf(
			"reverse iterator should be exhausted: valid=%v err=%v",
			valid,
			err,
		)
	}
	file.Close()
}

// Corrupt local spool framing must never yield a key to the cloud iterator.
func TestReverseKeyFileRejectsMalformedRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"short header", []byte{0, 0, 0}},
		{"missing prefix", []byte{0, 0, 0, 4}},
		{"truncated payload", []byte{0, 0, 0, 2, 'a', 0, 0, 0, 2}},
		{"mismatched lengths", []byte{0, 0, 0, 2, 'a', 0, 0, 0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "reverse-")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write(tc.data); err != nil {
				t.Fatal(err)
			}
			f := &reverseKeyFile{file: file}
			key, valid, err := f.nextReverse()
			if err == nil || valid || key != "" {
				t.Fatalf("malformed spool yielded key %q, valid=%v, err=%v", key, valid, err)
			}
		})
	}
}

func TestReverseKeyFileEmptyAndBinaryKeys(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	keys := []string{"", string(binary.BigEndian.AppendUint32(nil, 0xffffffff)), "last"}
	for _, key := range keys {
		if err := writeReverseKey(file, key); err != nil {
			t.Fatal(err)
		}
	}
	f := &reverseKeyFile{file: file}
	for i := len(keys) - 1; i >= 0; i-- {
		key, valid, err := f.nextReverse()
		if err != nil || !valid || key != keys[i] {
			t.Fatalf("reverse key = %q, valid=%v, err=%v; want %q", key, valid, err, keys[i])
		}
	}
	if _, valid, err := f.nextReverse(); err != nil || valid {
		t.Fatalf("exhausted iterator: valid=%v err=%v", valid, err)
	}
}

func TestReverseIteratorPropagatesSpoolCorruption(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := writeReverseKey(file, "a"); err != nil {
		t.Fatal(err)
	}
	// The trailer still declares one byte, but the prefix declares two.
	if _, err := file.WriteAt([]byte{0, 0, 0, 2}, 0); err != nil {
		t.Fatal(err)
	}
	it := &s3ReverseIterator{keys: &reverseKeyFile{file: file}}
	it.Rewind()
	if it.Err() == nil || it.Valid() || it.Item() != nil {
		t.Fatalf("corrupted spool iterator: valid=%v err=%v", it.Valid(), it.Err())
	}
}
