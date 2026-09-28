//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package mithril

import "os"

func renameExtractedDirectory(root *os.Root, oldname, newname string) error {
	return root.Rename(oldname, newname)
}
