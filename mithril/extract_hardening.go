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

package mithril

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Errors reported when an extraction destination cannot be trusted. The
// archive-content checks (Zip Slip, symlink entries) guard what the archive
// asks for; these guard the filesystem the archive is written into, which an
// attacker may have reached first.
var (
	// ErrExtractDestinationNotEmpty reports an exclusive extraction whose
	// destination already holds content.
	ErrExtractDestinationNotEmpty = errors.New(
		"mithril: extraction destination is not empty",
	)
	// ErrExtractUnsafePath reports a destination path component that is a
	// symlink, or an existing non-directory where a directory is required.
	ErrExtractUnsafePath = errors.New(
		"mithril: unsafe extraction path",
	)
	// ErrExtractConflictingOptions reports a caller asking for both
	// destination policies at once.
	ErrExtractConflictingOptions = errors.New(
		"mithril: WithMergeIntoDestination and WithReplaceDestination are mutually exclusive",
	)
)

// extractConfig holds the resolved destination policy for one extraction.
type extractConfig struct {
	merge   bool
	replace bool
}

// ExtractOption configures how ExtractArchive treats its destination.
type ExtractOption func(*extractConfig)

// WithMergeIntoDestination extracts directly into the destination, adding to
// whatever is already there.
//
// This exists for destinations that several archives populate together — the
// parallel immutable-archive download builds one directory from many
// archives, so it can neither refuse a non-empty destination nor swap the
// directory out from under a concurrent extraction. Merging forgoes the
// private-staging guarantee, so every write still goes through the
// per-component symlink checks below.
func WithMergeIntoDestination() ExtractOption {
	return func(c *extractConfig) { c.merge = true }
}

// WithReplaceDestination allows an exclusive extraction to proceed when the
// destination already holds content, replacing it with the freshly extracted
// tree instead of refusing.
//
// This is the recovery path for a destination left behind by an interrupted
// or superseded run. Replacement is a swap of a directory staged elsewhere,
// never a write into the existing one, so pre-existing content is discarded
// rather than merged with or written through.
func WithReplaceDestination() ExtractOption {
	return func(c *extractConfig) { c.replace = true }
}

func newExtractConfig(opts []ExtractOption) extractConfig {
	var cfg extractConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// assertSafeExtractRoot checks the extraction root itself, rejecting a root
// that is a symlink or an existing non-directory.
//
// Only the root entry is inspected, never its ancestors. Directories above the
// root are chosen by the operator and are outside the threat this guards
// against, which is content planted inside the destination. Walking to the
// filesystem root would also reject ordinary layouts: on macOS every temporary
// path sits under /var, which is a symlink to /private/var.
func assertSafeExtractRoot(root string) error {
	info, err := os.Lstat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("inspecting %s: %w", root, err)
	}
	return assertRealDir(root, info)
}

// assertRealDir rejects a path that is a symlink or is not a directory.
func assertRealDir(path string, info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf(
			"%w: %s is a symlink", ErrExtractUnsafePath, path,
		)
	}
	if !info.IsDir() {
		return fmt.Errorf(
			"%w: %s is not a directory", ErrExtractUnsafePath, path,
		)
	}
	return nil
}

// extractDirMode is the mode extraction directories carry. Group traversal is
// part of the contract: the extracted immutable tree is read by other members
// of the node's group in deployments that separate the downloader from the
// node.
const extractDirMode = 0o750

// assertNoSymlinkComponents rejects a name, relative to root, whose existing
// path components include a symlink.
//
// Every component is inspected rather than the complete path alone. Inspecting
// only the whole path reports on its last component and resolves everything
// before it, so a symlink at `immutable` goes unnoticed while a write to
// `immutable/00000.chunk` follows it. Components are walked shortest first, so
// each is inspected before it is used to reach the next.
//
// A missing component ends the walk: nothing can exist below a name that does
// not exist yet.
func assertNoSymlinkComponents(root *os.Root, name string) error {
	var walked string
	for part := range strings.SplitSeq(filepath.ToSlash(name), "/") {
		if part == "" || part == "." {
			continue
		}
		walked = path.Join(walked, part)
		info, err := root.Lstat(walked)
		switch {
		case err == nil:
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf(
					"%w: %s is a symlink", ErrExtractUnsafePath, walked,
				)
			}
		case errors.Is(err, os.ErrNotExist):
			return nil
		default:
			return fmt.Errorf(
				"%w: inspecting %s: %w", ErrExtractUnsafePath, walked, err,
			)
		}
	}
	return nil
}

// openExtractRoot creates name under parentRoot if absent and returns a handle
// on it.
//
// Both steps resolve through parentRoot, so neither can be redirected outside
// it, and the directory is created rather than opened blindly because merge
// mode has no staging directory to fall back on.
//
// Opening cannot be made to reject a symlink outright — Root follows one whose
// target stays inside the root, and Go offers no directory open keyed on
// O_NOFOLLOW — so the handle is compared against the entry afterwards instead.
// A writer who substitutes the destination between the creation and the open
// leaves the two disagreeing, which is what this rejects; a symlink present
// beforehand is caught by the same comparison.
func openExtractRoot(parentRoot *os.Root, name string) (*os.Root, error) {
	if err := mkdirExtracted(parentRoot, name); err != nil {
		return nil, err
	}
	return openVerifiedRoot(parentRoot, name)
}

// openVerifiedParent walks the directory components of name from root and
// returns a handle on the immediate parent, the final component, and a
// function releasing the handles the walk opened.
//
// Each component is opened through the one above it and confirmed to be the
// entry that name denotes (openVerifiedRoot), rather than handing the whole
// path to Root.OpenRoot. Root confines resolution to the root but still
// follows a symlink whose target stays inside it, so one component substituted
// mid-extraction would redirect whatever the caller does with the handle at
// another directory in the tree — and extraction's symlink checks run once,
// before the work, which is precisely the window that matters here.
//
// The returned handle is root itself when name has no directory part; the
// release function is a no-op then, since closing the caller's root is not
// this function's to do.
func openVerifiedParent(
	root *os.Root,
	name string,
) (*os.Root, string, func(), error) {
	parts := strings.Split(filepath.ToSlash(filepath.Clean(name)), "/")
	parent := root
	var opened []*os.Root
	release := func() {
		for i := len(opened) - 1; i >= 0; i-- {
			_ = opened[i].Close()
		}
	}
	for _, part := range parts[:len(parts)-1] {
		if part == "" || part == "." {
			continue
		}
		next, err := openVerifiedRoot(parent, part)
		if err != nil {
			release()
			return nil, "", nil, err
		}
		opened = append(opened, next)
		parent = next
	}
	return parent, parts[len(parts)-1], release, nil
}

// openVerifiedRoot opens an existing name under parentRoot and confirms the
// handle refers to the entry that name refers to.
//
// Opening cannot be made to reject a symlink outright — Root follows one whose
// target stays inside the root, and Go offers no directory open keyed on
// O_NOFOLLOW — so the handle is compared against the entry afterwards instead.
// A writer who substitutes the name between the open and the comparison leaves
// the two disagreeing, which is what this rejects; a symlink present
// beforehand is caught by the same comparison.
func openVerifiedRoot(parentRoot *os.Root, name string) (*os.Root, error) {
	root, err := parentRoot.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: opening extraction root %s: %w",
			ErrExtractUnsafePath, name, err,
		)
	}
	opened, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf(
			"%w: inspecting extraction root %s: %w",
			ErrExtractUnsafePath, name, err,
		)
	}
	named, err := parentRoot.Lstat(name)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf(
			"%w: inspecting %s: %w", ErrExtractUnsafePath, name, err,
		)
	}
	if named.Mode()&os.ModeSymlink != 0 || !os.SameFile(named, opened) {
		_ = root.Close()
		return nil, fmt.Errorf(
			"%w: %s was substituted before it could be opened",
			ErrExtractUnsafePath, name,
		)
	}
	return root, nil
}

// openVerifiedDir opens an existing directory through a handle on its parent,
// refusing a symlink or a substituted entry at the final component.
//
// This is for the paths that decide whether a previous run already produced a
// usable tree. Those directories are derived inside the download directory
// rather than chosen by the operator, so a symlink at one of them is planted
// content rather than a layout decision, and following it would hand back a
// tree this node never extracted. Directories *above* the candidate are the
// operator's and are resolved normally, matching where extraction draws the
// same line.
func openVerifiedDir(dir string) (*os.Root, error) {
	clean := filepath.Clean(dir)
	parentRoot, err := os.OpenRoot(filepath.Dir(clean))
	if err != nil {
		return nil, fmt.Errorf(
			"%w: opening %s: %w", ErrExtractUnsafePath, dir, err,
		)
	}
	defer parentRoot.Close()
	return openVerifiedRoot(parentRoot, filepath.Base(clean))
}

// vettedDir is a directory that was inspected through an open handle, carrying
// that handle rather than only the name it was inspected under.
//
// A handle refers to a directory. The name it was opened under refers to
// whatever occupies that name at the moment it is resolved — which is not the
// same thing, and stops being the same thing the instant somebody else can
// write to the enclosing directory. Everything the cache-reuse lookups
// establish is established through the handle, so handing back only the name
// would discard it at the boundary: the consumer resolves the name afresh and
// reads whatever is there by then.
//
// So the handle is what travels. Path is for messages and for the consumers
// that have no way to take a handle; Root is what the ImmutableDB is opened
// through, and the answer the lookup gave stays true for as long as it is held.
//
// Close it when the tree is no longer needed. Holding it open costs one
// descriptor and is what keeps the reads bound to the inspected directory.
//
// A nil *vettedDir is a valid value meaning there is no such tree — an
// ancillary download that failed, a lookup that found nothing — and every
// method below tolerates it: Path returns "", Root returns nil, Close does
// nothing. Callers on paths where the tree is optional carry the nil rather
// than branching around it, so "absent" travels as a value instead of as a
// second code path that has to be kept in step with the first.
type vettedDir struct {
	root *os.Root
	path string
}

// Path is the name the directory was inspected under. Reopening it is a fresh
// resolution and carries none of the vetting — use Root for reads.
func (d *vettedDir) Path() string {
	if d == nil {
		return ""
	}
	return d.path
}

// Root is the handle the directory was inspected through.
func (d *vettedDir) Root() *os.Root {
	if d == nil {
		return nil
	}
	return d.root
}

func (d *vettedDir) Close() {
	if d == nil || d.root == nil {
		return
	}
	_ = d.root.Close()
}

// vetted pairs an inspected directory handle with dir joined with rel, but only
// while that name still denotes the directory the handle refers to. It takes
// ownership of inspected: the handle is either carried by the returned
// vettedDir or closed.
//
// The comparison is against the handle on the directory that was read, not
// against a fresh resolution of rel through its parent. Re-resolving would
// compare two readings of one name with each other: a candidate replaced after
// it was read appears on both sides, the two agree, and a tree that was never
// inspected is returned.
//
// The name is checked even though the handle is what the reads use, because
// consumers that cannot take a handle still resolve it — and a name that
// already disagrees with the inspected tree is evidence of interference, not a
// detail to paper over.
func vetted(inspected *os.Root, dir, rel string) *vettedDir {
	read, err := inspected.Stat(".")
	if err != nil {
		_ = inspected.Close()
		return nil
	}
	name := filepath.Join(dir, rel)
	// Resolved the way a pathname consumer would resolve it, so the comparison
	// answers the question such a consumer would be asking.
	named, err := os.Stat(name)
	if err != nil || !os.SameFile(read, named) {
		_ = inspected.Close()
		return nil
	}
	return &vettedDir{root: inspected, path: name}
}

// dirIsEmpty reports whether dir exists and contains no entries. A missing
// directory counts as empty.
func dirIsEmpty(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	return len(entries) == 0, nil
}

// prepareExtractDestination applies the destination policy and returns a
// directory handle extraction must write through, plus a publish function to
// run on success and a cleanup function to run unconditionally.
//
// The handle is what makes the guarantee hold under mutation. Every write is
// resolved relative to it rather than by re-walking a pathname, so a directory
// swapped after a check cannot redirect a later write, and an entry resolving
// outside the root is refused by the runtime rather than by an inspection that
// raced. Checking components and then opening by path cannot offer that.
//
// In exclusive mode the handle is on a freshly created 0700 staging directory
// alongside the destination, so a partial extraction is never visible at the
// destination path. Publishing renames the staging directory into place.
func prepareExtractDestination(
	destDir string,
	cfg extractConfig,
) (root *os.Root, publish func() error, cleanup func(), err error) {
	cleanDest := filepath.Clean(destDir)

	// The two policies describe incompatible things — merge accumulates into
	// the destination, replace swaps it wholesale — and merge used to win
	// silently, so a caller meaning to replace would have quietly kept the old
	// tree. Neither reading is safe to guess at.
	if cfg.merge && cfg.replace {
		return nil, nil, nil, ErrExtractConflictingOptions
	}

	// Checked before anything is created or read, so a destination that is
	// itself a symlink never gets followed.
	if err := assertSafeExtractRoot(cleanDest); err != nil {
		return nil, nil, nil, err
	}

	if !cfg.merge && !cfg.replace {
		empty, err := dirIsEmpty(cleanDest)
		if err != nil {
			return nil, nil, nil, fmt.Errorf(
				"inspecting extraction destination: %w", err,
			)
		}
		if !empty {
			return nil, nil, nil, fmt.Errorf(
				"%w: %s", ErrExtractDestinationNotEmpty, cleanDest,
			)
		}
	}

	parent := filepath.Dir(cleanDest)
	destName := filepath.Base(cleanDest)
	if err := os.MkdirAll(parent, extractDirMode); err != nil {
		return nil, nil, nil, fmt.Errorf(
			"creating extraction parent directory: %w", err,
		)
	}
	// The parent is held open for the whole extraction so everything acting on
	// the destination — creating it, opening it, publishing into it — resolves
	// relative to this handle. Those are otherwise pathname operations, and a
	// parent replaced after any check would redirect them however recently that
	// check ran.
	parentRoot, err := os.OpenRoot(parent)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(
			"%w: opening extraction parent %s: %w",
			ErrExtractUnsafePath, parent, err,
		)
	}

	if cfg.merge {
		// Merge writes into the destination itself, so the destination is what
		// gets opened. Going through the parent handle is what keeps it from
		// being a symlink someone swapped in: opening the pathname directly
		// would follow one, and the check above cannot cover the gap between
		// itself and the open.
		mergeRoot, err := openExtractRoot(parentRoot, destName)
		if err != nil {
			_ = parentRoot.Close()
			return nil, nil, nil, err
		}
		return mergeRoot,
			func() error { return nil },
			func() {
				_ = mergeRoot.Close()
				_ = parentRoot.Close()
			},
			nil
	}

	// Staged alongside the destination so publishing is a rename within one
	// filesystem. MkdirTemp creates with 0700 and never reuses a name; the
	// mode is widened to extractDirMode at publication, once the tree is
	// complete, so a partially written extraction is never group-readable.
	staging, err := os.MkdirTemp(parent, ".extract-*")
	if err != nil {
		_ = parentRoot.Close()
		return nil, nil, nil, fmt.Errorf(
			"creating extraction staging directory: %w", err,
		)
	}
	stagingName := filepath.Base(staging)
	stagingRoot, err := parentRoot.OpenRoot(stagingName)
	if err != nil {
		_ = os.RemoveAll(staging)
		_ = parentRoot.Close()
		return nil, nil, nil, fmt.Errorf(
			"%w: opening staging root %s: %w",
			ErrExtractUnsafePath, staging, err,
		)
	}
	// Identity of the directory extraction actually writes into, taken
	// through the handle. Renaming can only name its source, so this is what
	// lets publication tell the staging directory apart from anything that
	// later occupies its name.
	stagingInfo, err := stagingRoot.Stat(".")
	if err != nil {
		_ = stagingRoot.Close()
		_ = os.RemoveAll(staging)
		_ = parentRoot.Close()
		return nil, nil, nil, fmt.Errorf(
			"inspecting extraction staging directory: %w", err,
		)
	}

	cleanup = func() {
		_ = stagingRoot.Close()
		// Removal goes through the parent handle for the same reason
		// publication does.
		_ = parentRoot.RemoveAll(stagingName)
		_ = parentRoot.Close()
	}
	publish = func() error {
		// MkdirTemp creates the staging directory 0700 and rename preserves
		// the source mode, so without this the published destination would
		// inherit 0700 rather than the 0750 the extracted tree is expected to
		// carry, silently dropping group traversal.
		//
		// Applied to the staging handle itself rather than by name. Resolving
		// the name here would follow a symlink a writer had substituted for
		// the staging entry and change the mode of whatever it pointed at; the
		// open handle cannot be redirected.
		if err := stagingRoot.Chmod(".", extractDirMode); err != nil {
			return fmt.Errorf("setting extraction destination mode: %w", err)
		}
		// Release the handle before moving the directory it refers to.
		if err := stagingRoot.Close(); err != nil {
			return fmt.Errorf("closing staging root: %w", err)
		}
		// Replacing is destructive by request, so the existing destination
		// goes first. RemoveAll unlinks a symlink at the destination rather
		// than following it, and resolving through the parent handle means a
		// swapped parent cannot redirect it.
		if cfg.replace {
			if err := parentRoot.RemoveAll(destName); err != nil {
				return fmt.Errorf(
					"clearing extraction destination: %w", err,
				)
			}
		}
		// The rename goes first, before anything inspects or clears the
		// destination, because on a POSIX filesystem it already is the whole
		// contract in one atomic step. Renaming a directory over an absent or
		// empty destination succeeds; over a populated one it fails with
		// ENOTEMPTY; over a file it fails with ENOTDIR and leaves the file
		// exactly as it was. There is no window in a single syscall, so
		// nothing a concurrent writer does between two of our operations can
		// cost them content.
		//
		// Inspecting the destination first and then acting on it — which is
		// what this used to do — can only be worse than that, however narrow
		// the gap is made.
		renameErr := parentRoot.Rename(stagingName, destName)
		if renameErr != nil && !cfg.replace {
			// The destination is occupied. Windows will not rename over an
			// existing directory even when it is empty, so reaching here does
			// not by itself mean the destination holds anything; clearing an
			// empty one and retrying is what keeps behaviour uniform across
			// platforms.
			//
			// Which is safe only because the removal is directory-only. A
			// writer can still swap the destination for a file after it is
			// identified as a directory, and rmdir refuses a file where an
			// unlink would have destroyed it.
			if err := clearEmptyDestination(
				parentRoot, destName, cleanDest, renameErr,
			); err != nil {
				return err
			}
			renameErr = parentRoot.Rename(stagingName, destName)
		}
		if renameErr != nil {
			return fmt.Errorf("publishing extraction: %w", renameErr)
		}
		// Rename names its source, so it moves whatever occupies stagingName
		// at that instant, not necessarily the directory extraction wrote
		// into. A writer with access to the parent can move the staging
		// directory aside and leave a tree or symlink of their own under that
		// name, which this rename would then publish. Go offers no rename
		// keyed on a descriptor, so confirm afterwards that what landed at
		// the destination is the directory extraction actually filled.
		published, err := parentRoot.Lstat(destName)
		if err != nil {
			return fmt.Errorf(
				"%w: inspecting published destination: %w",
				ErrExtractUnsafePath, err,
			)
		}
		if published.Mode()&os.ModeSymlink != 0 ||
			!os.SameFile(stagingInfo, published) {
			// Only this rename put anything here — the destination was
			// left empty or absent above — so removing it restores the
			// prior state rather than destroying anyone's content.
			_ = parentRoot.RemoveAll(destName)
			return fmt.Errorf(
				"%w: staging directory was substituted before publication",
				ErrExtractUnsafePath,
			)
		}
		return nil
	}
	return stagingRoot, publish, cleanup, nil
}

// clearEmptyDestination removes an empty directory occupying the destination
// so publication can retry the rename, and refuses anything else.
//
// The destination must be empty, which is not the same as absent: an operator
// creating the directory ahead of time, or a previous run cleaning up after
// itself, both leave one behind, and refusing those would turn a supported
// arrangement into a failure. Anything holding content is refused untouched —
// clearing it is what WithReplaceDestination exists to authorise.
//
// renameErr is the failure that led here. It is reported unchanged when the
// destination turns out to be absent, since the rename failed for some reason
// this cannot explain and reporting a destination problem would misdescribe it.
func clearEmptyDestination(
	parentRoot *os.Root,
	destName, cleanDest string,
	renameErr error,
) error {
	info, err := parentRoot.Lstat(destName)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("publishing extraction: %w", renameErr)
	case err != nil:
		return fmt.Errorf("inspecting extraction destination: %w", err)
	case !info.IsDir():
		return fmt.Errorf(
			"%w: %s already exists",
			ErrExtractDestinationNotEmpty, cleanDest,
		)
	}
	if err := removeEmptyExtractDir(parentRoot, destName, cleanDest); err != nil {
		return fmt.Errorf(
			"%w: %s: %w", ErrExtractDestinationNotEmpty, cleanDest, err,
		)
	}
	return nil
}

// createExtractedFile opens name for writing relative to the extraction root.
//
// The root refuses any name resolving outside it, so containment does not
// depend on the component checks below. Those exist to reject writing through
// a symlink that sits inside the root, which an archive should never do, and
// to give a clearer error than the runtime would; O_NOFOLLOW makes the final
// component race-free where the platform supports it.
//
// The file is created exclusively, so what gets written is always an inode
// created here. Opening an existing name with O_CREATE|O_TRUNC keeps whatever
// inode is there, including its owner and its mode — and merge extraction
// writes straight into a shared destination, where a name it is about to write
// can already be occupied by a file somebody else created, world-writable.
// Certified bytes would then live inside a file that is still theirs to
// rewrite, which no later verification can catch: a same-inode write is
// visible through a descriptor already open on it, so binding the read to the
// descriptor does not help. Owning the inode is what makes that binding worth
// anything.
//
// An occupied name is cleared and the create retried once, because O_EXCL
// alone would refuse a resume — which legitimately overwrites a partial file
// an interrupted run left behind. The clearing removes files and refuses
// directories (removeExtractedFile), so a directory at the name still fails
// the extraction as it always did rather than being deleted; it is one
// operation, not a type check followed by a removal that could act on
// something else by then. Whoever wins the window between the two creates
// makes the extraction fail, not succeed into their file.
func createExtractedFile(root *os.Root, name string) (*os.File, error) {
	if err := assertNoSymlinkComponents(root, name); err != nil {
		return nil, err
	}
	file, err := createExtractedFileExclusive(root, name)
	if err == nil {
		return file, nil
	}
	if !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf(
			"%w: opening %s: %w", ErrExtractUnsafePath, name, err,
		)
	}
	// Unlinking rather than truncating is what breaks the association with an
	// inode somebody else may own or hold open. A symlink at the name is
	// unlinked as the link rather than followed, which is also what the checks
	// above want.
	if err := removeExtractedFile(
		root, name, filepath.Join(root.Name(), name),
	); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf(
			"%w: clearing %s: %w", ErrExtractUnsafePath, name, err,
		)
	}
	file, err = createExtractedFileExclusive(root, name)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: opening %s: %w", ErrExtractUnsafePath, name, err,
		)
	}
	return file, nil
}

func createExtractedFileExclusive(
	root *os.Root,
	name string,
) (*os.File, error) {
	return root.OpenFile(
		name,
		os.O_CREATE|os.O_EXCL|os.O_WRONLY|openNoFollowFlag,
		0o640,
	)
}

// mkdirExtracted creates a directory relative to the extraction root.
//
// A name resolving outside the root is refused by the root itself. A symlink
// anywhere along the name is rejected here so extraction never populates a
// directory the archive did not create, even one pointing back inside the
// root.
func mkdirExtracted(root *os.Root, name string) error {
	if name == "." || name == "" {
		return nil
	}
	if err := assertNoSymlinkComponents(root, name); err != nil {
		return err
	}
	if err := root.MkdirAll(name, extractDirMode); err != nil {
		return fmt.Errorf(
			"%w: creating %s: %w", ErrExtractUnsafePath, name, err,
		)
	}
	return nil
}
