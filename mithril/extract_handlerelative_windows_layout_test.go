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

//go:build windows

package mithril

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// TestBuildRenameInformationLayout decodes the buffer by hand, at the byte
// level, rather than casting it back through fileRenameInformationHeader —
// the whole risk here is that Go's struct layout and NtSetInformationFile's
// expected wire layout could silently disagree, and reinterpreting the same
// bytes through the same struct would not catch that; only an independent,
// offset-by-offset reading would.
//
// The layout is: ReplaceIfExists (1 byte), 7 bytes of alignment padding on a
// 64-bit target, RootDirectory (pointer-sized), FileNameLength (4 bytes,
// little-endian), then FileName immediately after with no further padding.
func TestBuildRenameInformationLayout(t *testing.T) {
	const destDir = windows.Handle(0x1234)
	buf, err := buildRenameInformation(destDir, "dest.tmp", true)
	require.NoError(t, err)

	handleSize := int(unsafe.Sizeof(windows.Handle(0)))
	// A 1-byte bool pads out to the handle's own alignment, which for a
	// power-of-two size no larger than the handle equals the handle size
	// itself.
	rootDirOffset := handleSize
	nameOffset := rootDirOffset + handleSize + 4

	require.GreaterOrEqual(t, len(buf), nameOffset)
	assert.Equal(t, byte(1), buf[0], "ReplaceIfExists must be TRUE")

	var gotHandle uint64
	for i := range handleSize {
		gotHandle |= uint64(buf[rootDirOffset+i]) << (8 * i)
	}
	assert.Equal(t, uint64(destDir), gotHandle,
		"RootDirectory must round-trip through the buffer")

	nameLen := binary.LittleEndian.Uint32(buf[rootDirOffset+handleSize:])
	assert.Equal(t, uint32(len("dest.tmp")*2), nameLen,
		"FileNameLength must count UTF-16 bytes, excluding any terminator")
	assert.Len(
		t,
		buf,
		nameOffset+int(nameLen),
		"the buffer must hold exactly the header plus the encoded name, no extra padding",
	)

	gotName := windows.UTF16ToString(
		unsafe.Slice((*uint16)(unsafe.Pointer(&buf[nameOffset])), nameLen/2),
	)
	assert.Equal(t, "dest.tmp", gotName)
}

func TestBuildRenameInformationReplaceIfExistsFalse(t *testing.T) {
	buf, err := buildRenameInformation(windows.Handle(1), "x", false)
	require.NoError(t, err)
	assert.Equal(t, byte(0), buf[0], "ReplaceIfExists must be FALSE")
}

func TestBuildRenameInformationEmptyName(t *testing.T) {
	handleSize := int(unsafe.Sizeof(windows.Handle(0)))
	rootDirOffset := handleSize
	nameOffset := rootDirOffset + handleSize + 4
	buf, err := buildRenameInformation(windows.Handle(1), "", true)
	require.NoError(t, err)
	assert.Len(t, buf, nameOffset)
	assert.Equal(
		t,
		uint32(0),
		binary.LittleEndian.Uint32(buf[rootDirOffset+handleSize:]),
	)
}
