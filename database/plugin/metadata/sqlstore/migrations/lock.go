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

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// Locker serializes migration runners. Advisory implementations must use the
// supplied connection so lock ownership lasts until release.
type Locker interface {
	Acquire(context.Context, *sql.Conn) (func() error, error)
}

type processLocker struct {
	token chan struct{}
}

// NewProcessLocker returns a process-wide lock suitable for isolated in-memory
// SQLite databases.
func NewProcessLocker() Locker {
	locker := &processLocker{token: make(chan struct{}, 1)}
	locker.token <- struct{}{}
	return locker
}

func (l *processLocker) Acquire(
	ctx context.Context,
	_ *sql.Conn,
) (func() error, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.token:
	}
	var once sync.Once
	return func() error {
		once.Do(func() { l.token <- struct{}{} })
		return nil
	}, nil
}

type advisoryLocker struct {
	dialect string
	key     int64
	timeout time.Duration
}

// NewAdvisoryLocker returns a connection-owned PostgreSQL or MySQL lock.
func NewAdvisoryLocker(
	dialect string,
	key int64,
	timeout time.Duration,
) Locker {
	return &advisoryLocker{dialect: dialect, key: key, timeout: timeout}
}

func (l *advisoryLocker) Acquire(
	ctx context.Context,
	conn *sql.Conn,
) (func() error, error) {
	switch l.dialect {
	case "postgres":
		acquireCtx := ctx
		var cancel context.CancelFunc
		if l.timeout > 0 {
			acquireCtx, cancel = context.WithTimeout(ctx, l.timeout)
			defer cancel()
		}
		if _, err := conn.ExecContext(
			acquireCtx,
			"SELECT pg_advisory_lock($1)",
			l.key,
		); err != nil {
			if errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(acquireCtx.Err(), context.DeadlineExceeded) {
				return nil, fmt.Errorf(
					"acquire PostgreSQL migration lock timed out after %s: %w",
					l.timeout,
					context.DeadlineExceeded,
				)
			}
			return nil, fmt.Errorf("acquire PostgreSQL migration lock: %w", err)
		}
		// The release callback deliberately outlives the acquisition context:
		// advisory locks still need releasing after startup cancellation.
		return func() error { //nolint:contextcheck
			_, err := conn.ExecContext(
				context.Background(),
				"SELECT pg_advisory_unlock($1)",
				l.key,
			)
			return err
		}, nil
	case "mysql":
		timeoutSeconds := mysqlLockTimeoutSeconds(l.timeout)
		var acquired sql.NullInt64
		if err := conn.QueryRowContext(
			ctx,
			"SELECT GET_LOCK(?, ?)",
			fmt.Sprintf("dingo-metadata-%d", l.key),
			timeoutSeconds,
		).Scan(&acquired); err != nil {
			return nil, fmt.Errorf("acquire MySQL migration lock: %w", err)
		}
		if !acquired.Valid {
			return nil, errors.New("MySQL metadata migration lock returned NULL")
		}
		if acquired.Int64 == 0 {
			return nil, errors.New("MySQL metadata migration lock timed out")
		}
		if acquired.Int64 != 1 {
			return nil, fmt.Errorf("MySQL metadata migration lock returned %d", acquired.Int64)
		}
		// The release callback deliberately outlives the acquisition context:
		// advisory locks still need releasing after startup cancellation.
		return func() error { //nolint:contextcheck
			var released sql.NullInt64
			return conn.QueryRowContext(
				context.Background(),
				"SELECT RELEASE_LOCK(?)",
				fmt.Sprintf("dingo-metadata-%d", l.key),
			).Scan(&released)
		}, nil
	default:
		return nil, fmt.Errorf(
			"advisory migration locks are unsupported for %q",
			l.dialect,
		)
	}
}

func mysqlLockTimeoutSeconds(timeout time.Duration) int64 {
	if timeout <= 0 {
		// GET_LOCK interprets a negative timeout as wait indefinitely.  Zero
		// means a non-blocking try-lock, which would make an omitted timeout
		// fail startup spuriously while another node is migrating.
		return -1
	}
	return int64(math.Ceil(timeout.Seconds()))
}
