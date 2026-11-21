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

package blob

import (
	"context"
	"io"
)

// ImmutableObject describes one remotely stored ImmutableDB file.
type ImmutableObject struct {
	Name string
	Size int64
}

// ImmutableObjects is the object-file surface used by remote ImmutableDB
// providers. Names are file names, not blob keys; providers scope them below
// the database's own namespace.
type ImmutableObjects interface {
	ReadImmutableFile(ctx context.Context, name string, dst io.Writer) (int64, error)
	WriteImmutableFile(ctx context.Context, name string, src io.Reader, size int64) error
	ListImmutableFiles(ctx context.Context) ([]ImmutableObject, error)
	DeleteImmutableFile(ctx context.Context, name string) error
	ResetImmutableFiles(ctx context.Context) error
}
