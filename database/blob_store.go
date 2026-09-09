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

package database

import (
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
)

// Blob-store ownership, locking, and replacement.
//
// A Database holds a reference to a blob store; the store itself is owned by
// whoever constructed it (the plugin host — see New's doc comment). The
// reference can be replaced while the database is live: node.go and
// node_lifecycle.go wrap the installed store in a bark archive client and
// install the wrapper with SetBlobStore, the latter on the running-node
// reconfigure path.
//
// One strategy covers every reader in this package:
//
//   - blobMu guards Database.blobRef. Nothing reads or writes the field
//     directly; Blob and pinBlobStore are the only readers, SetBlobStore the
//     only writer.
//
//   - An operation that does blob work pins the store it works on and works
//     on exactly that store for the whole operation. A Txn pins at
//     construction and releases when it reaches Commit, Rollback, or
//     Release, and Txn.BlobStore returns the store it pinned — so a store
//     and the types.Txn opened on it can never come from two different
//     installations, which a bare "read the field again later" would allow.
//     Code that touches the blob store outside a Txn brackets the operation
//     with PinBlob and the release func it returns.
//
//   - SetBlobStore installs the new reference and hands back the replaced
//     store together with a drain func. The replaced store may be closed
//     once drain returns: every operation that pinned it has finished, and
//     no new one can pin it because the reference it would have come from is
//     already gone. Until then the previous store must stay open, because an
//     operation that pinned it before the swap is still using it.
//     SetBlobStore never closes anything itself — the two production callers
//     wrap the previous store rather than retiring it, and closing a store
//     still reachable through a wrapper would break the wrapper.
//
//   - One consequence of "never closes" is load-bearing beyond drain.
//     Txn.Commit's partial-commit path (types.ErrPartialCommit: the blob
//     transaction committed, the metadata did not) releases the Txn's pin
//     when the transaction finishes, but the recovery that trims the blob
//     store back to the metadata tip runs later and from the caller —
//     LedgerState.RecoverCommitTimestampConflict — against the store
//     installed at that point. The pin is deliberately not held across that
//     gap: recovery is caller-scheduled and unbounded, so holding one would
//     block drain for as long as recovery is pending. What makes the gap
//     safe is that the replaced store stays open and stays reachable, since
//     the wrapper installed over it forwards to it.
//
// Blob returns the currently installed store without a pin. It is the
// accessor for callers that only need to identify, wrap, or ask a
// whole-store question of the current store (bark wrapping it, a DiskSize
// gauge, a Backuper type assertion); its result must be used within the call
// that obtained it and must not be retained. Callers doing blob work across
// several calls use a Txn (which pins) or PinBlob, so that drain's guarantee
// covers them.

// blobStoreRef is one installed blob store plus the set of operations
// currently pinning it. A replacement retires the whole ref rather than
// mutating it, so a pin taken before the swap keeps naming the store it was
// taken on.
type blobStoreRef struct {
	store blob.BlobStore
	// users counts the operations holding a pin on store.
	//
	// sync.WaitGroup requires that an Add which raises the counter from
	// zero happen before a concurrent Wait. That holds here without extra
	// care: every Add runs under blobMu.RLock, and SetBlobStore takes
	// blobMu for writing before it can hand this ref's Wait to anyone, so
	// every Add on a retired ref happened before the swap that retired it,
	// and no Add can start afterwards.
	users sync.WaitGroup
}

// newBlobStoreRef wraps store in a fresh, unpinned ref.
func newBlobStoreRef(store blob.BlobStore) *blobStoreRef {
	return &blobStoreRef{store: store}
}

// blobStore returns the referenced store. It tolerates a nil receiver so
// callers can pin unconditionally and nil-check the store, matching how the
// package already treats an absent blob store (types.ErrBlobStoreUnavailable)
// rather than a missing database.
func (r *blobStoreRef) blobStore() blob.BlobStore {
	if r == nil {
		return nil
	}
	return r.store
}

// release drops one pin taken by pinBlobStore. It must be called exactly
// once per pin: a second call is an unmatched WaitGroup.Done and panics, the
// same contract sync.WaitGroup itself has.
func (r *blobStoreRef) release() {
	if r == nil {
		return
	}
	r.users.Done()
}

// pinBlobStore pins the currently installed blob store. The returned ref must
// be released exactly once, and stays valid (naming the same store) until it
// is, even across a concurrent SetBlobStore.
func (d *Database) pinBlobStore() *blobStoreRef {
	d.blobMu.RLock()
	defer d.blobMu.RUnlock()
	ref := d.blobRef
	if ref != nil {
		ref.users.Add(1)
	}
	return ref
}

// PinBlob pins the currently installed blob store for the duration of one
// operation and returns it alongside the func that releases the pin. Call the
// release func exactly once, normally with defer:
//
//	store, release := db.PinBlob()
//	defer release()
//	if store == nil {
//		return types.ErrBlobStoreUnavailable
//	}
//
// A concurrent SetBlobStore's drain does not return while the pin is held, so
// the store cannot be closed out from under the operation. Work that already
// runs inside a database Txn does not need this — the Txn holds a pin for its
// whole lifetime and Txn.BlobStore returns the store it pinned.
func (d *Database) PinBlob() (blob.BlobStore, func()) {
	ref := d.pinBlobStore()
	return ref.blobStore(), ref.release
}

// pinBlobForTxn returns the blob store an operation must run against, together
// with the func that releases whatever pin it took.
//
// A transaction that already holds a blob handle pinned its store at
// construction, and that handle means nothing to any other store. Pinning
// whichever store is installed now would pair a handle from one installation
// with a store from another -- the pairing the notes at the top of this file
// exist to rule out -- so an operation handed such a transaction uses the
// transaction's store. The transaction's own pin keeps that store alive for as
// long as its caller holds the transaction open, so no second pin is taken and
// the returned release func is a no-op. With no transaction, or one that opened
// no blob handle, the operation is not attached to an installation yet and pins
// the installed store like any other PinBlob caller.
func (d *Database) pinBlobForTxn(txn *Txn) (blob.BlobStore, func()) {
	if txn != nil && txn.Blob() != nil {
		if store := txn.BlobStore(); store != nil {
			return store, func() {}
		}
	}
	return d.PinBlob()
}

// Blob returns the currently installed blob store without pinning it. See the
// ownership notes at the top of this file: use the returned store within the
// call that obtained it, and use a Txn or PinBlob for anything longer.
func (d *Database) Blob() blob.BlobStore {
	d.blobMu.RLock()
	defer d.blobMu.RUnlock()
	return d.blobRef.blobStore()
}

// SetBlobStore installs b as the database's blob store and returns the store
// it replaced along with a drain func.
//
// Operations already in flight keep running against the store they pinned;
// operations started after this call get b. drain blocks until every
// operation pinned on the replaced store has finished, which is the point at
// which prev may be closed — nothing can reach it afterwards. A caller that
// keeps prev alive (the bark wrapper in node.go and node_lifecycle.go wraps
// the store it was handed and forwards Close to it) has nothing to drain and
// may ignore both results; SetBlobStore itself never closes prev.
//
// drain covers the reference this call retires, which is the only route by
// which prev can still be reached: the pins taken on it before the swap, and no
// new ones. Installing prev again before drain returns creates a second
// reference to the same store whose pins are counted separately, so drain would
// then report only the first reference as idle while the second is in use. A
// caller that intends to close prev must therefore not re-install it -- install
// a fresh store, or drain before re-installing. Neither production caller
// closes prev at all, so neither can reach that case.
//
// drain is never nil, so it is always safe to call.
func (d *Database) SetBlobStore(
	b blob.BlobStore,
) (prev blob.BlobStore, drain func()) {
	next := newBlobStoreRef(b)
	d.blobMu.Lock()
	old := d.blobRef
	d.blobRef = next
	d.blobMu.Unlock()
	if old == nil {
		return nil, func() {}
	}
	return old.store, old.users.Wait
}
