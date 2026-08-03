// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//go:build dingo_extra_plugins

package gcs

import (
	"os"
	"strings"
	"testing"
)

func TestReadBlobObjectWithLimit(t *testing.T) {
	data, err := readBlobObjectWithLimit(strings.NewReader("123"), 3)
	if err != nil || string(data) != "123" {
		t.Fatalf("read within limit = %q, %v", data, err)
	}
	if _, err := readBlobObjectWithLimit(strings.NewReader("1234"), 3); err == nil {
		t.Fatal("oversized object should be rejected")
	}
}

func TestGcsTransactionStagesAndRollsBack(t *testing.T) {
	txn := &gcsTxn{pending: make(map[string]gcsPendingChange)}
	txn.stageSet([]byte("key"), []byte("value"))
	value, ok := txn.stagedValue([]byte("key"))
	if !ok || string(value) != "value" {
		t.Fatalf("staged value = %q, %v", value, ok)
	}
	txn.stageDelete([]byte("key"))
	value, ok = txn.stagedValue([]byte("key"))
	if !ok || value != nil {
		t.Fatalf("staged delete = %q, %v", value, ok)
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
		t.Fatalf("reverse iterator should be exhausted: valid=%v err=%v", valid, err)
	}
	file.Close()
}
