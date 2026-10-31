//go:build unix

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package mithril

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCanonicalBootstrapParentResolvesAlias(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	realParent := filepath.Join(base, "real")
	require.NoError(t, os.Mkdir(realParent, 0o700))
	alias := filepath.Join(base, "alias")
	require.NoError(t, os.Symlink(realParent, alias))

	canonical, err := canonicalBootstrapParent(alias)
	require.NoError(t, err)
	require.Equal(t, realParent, canonical)
}

type bootstrapAncestorInfo struct {
	name string
	mode fs.FileMode
	uid  uint32
}

func (i bootstrapAncestorInfo) Name() string       { return i.name }
func (i bootstrapAncestorInfo) Size() int64        { return 0 }
func (i bootstrapAncestorInfo) Mode() fs.FileMode  { return i.mode }
func (i bootstrapAncestorInfo) ModTime() time.Time { return time.Time{} }
func (i bootstrapAncestorInfo) IsDir() bool        { return i.mode.IsDir() }
func (i bootstrapAncestorInfo) Sys() any           { return &syscall.Stat_t{Uid: i.uid} }

func bootstrapAncestorLookup(
	t *testing.T,
	entries map[string]bootstrapAncestorInfo,
) func(string) (os.FileInfo, error) {
	t.Helper()
	return func(path string) (os.FileInfo, error) {
		info, ok := entries[path]
		require.True(t, ok, "unexpected ancestor lookup %q", path)
		return info, nil
	}
}

func TestBootstrapParentRejectsAttackerOwnedStickyDirectory(t *testing.T) {
	t.Parallel()
	const currentUID = 1000
	err := validateBootstrapAncestorChain(
		"/shared",
		currentUID,
		bootstrapAncestorLookup(t, map[string]bootstrapAncestorInfo{
			"/": {
				name: "/", mode: os.ModeDir | 0o755, uid: 0,
			},
			"/shared": {
				name: "shared", mode: os.ModeDir | os.ModeSticky | 0o777, uid: 2000,
			},
		}),
	)
	require.ErrorContains(t, err, `bootstrap parent ancestor "/shared" is owned by UID 2000`)
}

func TestBootstrapParentRejectsWritableAncestor(t *testing.T) {
	t.Parallel()
	const currentUID = 1000
	err := validateBootstrapAncestorChain(
		"/trusted/private",
		currentUID,
		bootstrapAncestorLookup(t, map[string]bootstrapAncestorInfo{
			"/": {
				name: "/", mode: os.ModeDir | 0o755, uid: 0,
			},
			"/trusted": {
				name: "trusted", mode: os.ModeDir | 0o775, uid: currentUID,
			},
			"/trusted/private": {
				name: "private", mode: os.ModeDir | 0o700, uid: currentUID,
			},
		}),
	)
	require.ErrorContains(t, err, `bootstrap parent ancestor "/trusted" is writable`)
}

func TestBootstrapParentAcceptsTrustedStickyAncestor(t *testing.T) {
	t.Parallel()
	const currentUID = 1000
	err := validateBootstrapAncestorChain(
		"/shared/private",
		currentUID,
		bootstrapAncestorLookup(t, map[string]bootstrapAncestorInfo{
			"/": {
				name: "/", mode: os.ModeDir | 0o755, uid: 0,
			},
			"/shared": {
				name: "shared", mode: os.ModeDir | os.ModeSticky | 0o777, uid: 0,
			},
			"/shared/private": {
				name: "private", mode: os.ModeDir | 0o700, uid: currentUID,
			},
		}),
	)
	require.NoError(t, err)
}
