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
	"context"
	"sync"
)

// txnContext forwards caller cancellation until commit starts. database/sql
// binds automatic rollback to the context passed to BeginTx, so detaching at
// the Commit call site would be too late to protect a committed blob write.
// Parent deadlines arrive through cancellation; exposing a deadline would let
// child contexts retain a timer that cannot be detached when commit starts.
type txnContext struct {
	context.Context
	parent     context.Context
	mu         sync.Mutex
	committing bool
	err        error
	cancel     context.CancelCauseFunc
	stop       func() bool
}

func (t *Txn) metadataWriteContext(ctx context.Context) context.Context {
	if ctx.Done() == nil {
		return ctx
	}
	t.writeContext = newTxnContext(ctx)
	return t.writeContext
}

func newTxnContext(parent context.Context) *txnContext {
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	c := &txnContext{Context: ctx, parent: parent, cancel: cancel}
	c.stop = context.AfterFunc(parent, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !c.committing {
			c.cancelLocked(c.parent.Err(), context.Cause(c.parent))
		}
	})
	if err := parent.Err(); err != nil {
		c.mu.Lock()
		c.cancelLocked(err, context.Cause(parent))
		c.mu.Unlock()
	}
	return c
}

func (c *txnContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *txnContext) cancelLocked(err, cause error) {
	if c.err != nil {
		return
	}
	c.err = err
	c.cancel(cause)
}

func (c *txnContext) beginCommit() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.parent.Err(); err != nil {
		c.cancelLocked(err, context.Cause(c.parent))
		return err
	}
	// Serialize with an already-running cancellation callback: stopping an
	// AfterFunc alone does not wait for that callback to finish.
	c.committing = true
	c.stop()
	return nil
}

func (c *txnContext) release() {
	c.stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelLocked(context.Canceled, context.Canceled)
}
