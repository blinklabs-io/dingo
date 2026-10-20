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

package lifecycle

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type snapshotUploadFile struct {
	name string
	file *os.File
}

// openSnapshotUploadFiles opens exactly the files declared by a snapshot,
// with the manifest last so it remains the remote completion marker. The root
// and each file are held by handle while names are checked, so replacing the
// directory or an entry cannot redirect a later upload.
func openSnapshotUploadFiles(
	dir string,
	maxManifestBytes int64,
) (_ []snapshotUploadFile, retErr error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open snapshot directory %q: %w", dir, err)
	}
	var files []snapshotUploadFile
	defer func() {
		retErr = errors.Join(retErr, root.Close())
		if retErr != nil {
			retErr = errors.Join(retErr, closeSnapshotUploadFiles(files))
		}
	}()

	manifestFile, err := openSnapshotUploadFile(root, ManifestFileName)
	if err != nil {
		return nil, err
	}
	files = []snapshotUploadFile{{name: ManifestFileName, file: manifestFile}}

	data, err := readManifestData(
		manifestFile,
		[]ManifestOption{WithManifestMaxBytes(maxManifestBytes)},
	)
	if err != nil {
		return nil, fmt.Errorf("read snapshot manifest for upload: %w", err)
	}
	manifest, err := ParseManifest(
		data,
		WithManifestMaxBytes(maxManifestBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("parse snapshot manifest for upload: %w", err)
	}
	if _, err := manifestFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind snapshot manifest for upload: %w", err)
	}

	ordered := make([]snapshotUploadFile, 0, len(manifest.payloadFiles())+1)
	for _, payload := range manifest.payloadFiles() {
		file, err := openSnapshotUploadFile(root, payload.name)
		if err != nil {
			return nil, err
		}
		files = append(files, snapshotUploadFile{name: payload.name, file: file})
		ordered = append(ordered, snapshotUploadFile{name: payload.name, file: file})
	}
	ordered = append(ordered, files[0])
	return ordered, nil
}

func openSnapshotUploadFile(root *os.Root, name string) (*os.File, error) {
	file, err := root.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open snapshot upload file %q: %w", name, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect opened snapshot upload file %q: %w", name, err)
	}
	entry, err := root.Lstat(name)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect snapshot upload entry %q: %w", name, err)
	}
	if !info.Mode().IsRegular() || !entry.Mode().IsRegular() ||
		!os.SameFile(info, entry) {
		_ = file.Close()
		return nil, fmt.Errorf("snapshot upload file %q is not a stable regular file", name)
	}
	return file, nil
}

func closeSnapshotUploadFiles(files []snapshotUploadFile) error {
	var err error
	for _, file := range files {
		err = errors.Join(err, file.file.Close())
	}
	return err
}
