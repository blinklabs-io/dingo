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

package metadata

import "context"

// Backuper is implemented by metadata store plugins that can write a
// self-contained, restorable copy of their contents to a local filesystem
// path. Path-based (rather than io.Writer-based) because the underlying
// mechanism for most relational backends (e.g. SQLite's `VACUUM INTO`) is
// itself path-oriented; streaming to a remote destination is layered on
// top of the local copy by the caller, not by the plugin.
type Backuper interface {
	// BackupTo writes a self-contained copy of the store's current
	// contents to dstPath, which must not already exist.
	BackupTo(ctx context.Context, dstPath string) error
}

// Restorer is implemented by metadata store plugins that can replace their
// contents from a backup produced by Backuper.BackupTo.
type Restorer interface {
	// RestoreFrom replaces this store's on-disk state with the copy at
	// srcPath. Must be called before the store has been started against
	// its real data directory, or after Close().
	RestoreFrom(ctx context.Context, srcPath string) error
}

// Resettable is implemented by metadata store plugins whose Restore
// orchestration (database/lifecycle/restore.go's restoreMetadataStore)
// needs more than a directory wipe to satisfy RestoreFrom's "never
// started against its real data directory, or after Close()" precondition.
// That orchestration briefly resolves-and-starts the plugin against the
// target just to type-assert it, since plugin.Resolve always constructs
// and starts a provider together with no construct-only step -- for a
// file-based metadata store (sqlite is the only one today; badger is a
// blob backend, not metadata) simply deleting the target directory undoes
// that brief start completely, but a live client/server store
// (postgres/mysql) has no such directory: its Start already ran real
// migrations against the actual configured remote database, and a
// directory wipe does nothing to undo that. Reset clears exactly that
// server-side state (found via a live end-to-end restore attempt, which
// failed against a real Postgres server for exactly this reason) so the
// store is as empty as it would be immediately after a fresh Start with
// no prior data.
type Resettable interface {
	Reset(ctx context.Context) error
}

// resetPopulatedTargetContextKey is unexported: AllowResetOfPopulatedTarget
// and ResetOfPopulatedTargetAllowed are the only supported way to set or
// read it.
type resetPopulatedTargetContextKey struct{}

// AllowResetOfPopulatedTarget marks ctx so a Resettable implementation's own
// "refuse to reset a target that already holds real data" guard (added to
// stop Reset from silently destroying a live node's own database reached
// via a reused or misconfigured DSN) does not apply. Restore is inherently
// destructive to its target by design, and a live node restoring itself
// from its own earlier snapshot -- Bark's DatabaseService.Restore RPC,
// gated on its own explicit mTLS-authenticated caller plus dingoctl's own
// "this will replace the node's current database" confirmation prompt --
// always targets the exact database that node is already configured for,
// never an independently-resolved one a misconfigured DSN could point
// elsewhere. Call this only from that call path. The offline `dingo
// database restore` CLI command has no equivalent guarantee (nothing ties
// its target config to any specific already-running node) and must keep
// going through the unmarked path so the guard still applies there.
func AllowResetOfPopulatedTarget(ctx context.Context) context.Context {
	return context.WithValue(ctx, resetPopulatedTargetContextKey{}, true)
}

// ResetOfPopulatedTargetAllowed reports whether ctx was marked by
// AllowResetOfPopulatedTarget.
func ResetOfPopulatedTargetAllowed(ctx context.Context) bool {
	allowed, _ := ctx.Value(resetPopulatedTargetContextKey{}).(bool)
	return allowed
}

// BackupValidator is implemented by metadata store plugins that can check a
// backup file's structural integrity before it's used to restore, without
// touching any target database. RestoreFrom's own parsing eventually
// catches a corrupt or truncated backup on its own for every plugin, but
// for a Resettable provider specifically that discovery happens too late:
// restoreMetadataStore (database/lifecycle/restore.go) unconditionally
// resets a live remote target -- dropping its real tables, with no staging
// copy and no rollback -- before RestoreFrom ever runs, so a backup that
// turns out to be invalid only once RestoreFrom tries to use it means the
// reset already happened for nothing, with the target now empty and no way
// back. Validating first, while the target is still untouched, lets that
// failure abort before the reset instead of after it.
type BackupValidator interface {
	ValidateBackup(ctx context.Context, srcPath string) error
}
