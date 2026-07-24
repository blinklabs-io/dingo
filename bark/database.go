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

package bark

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ databaseconnect.DatabaseServiceHandler = &databaseServiceHandler{}

// defaultCatalogPageSize / defaultHistoryPageSize are used when a request
// leaves page_size unset (0), matching the proto's documented "0 means
// server default".
const (
	defaultCatalogPageSize = 50
	defaultHistoryPageSize = 50
)

// streamProgressPollInterval is how often StreamOperationProgress polls
// the operation's in-memory state. There is no push-based notification
// path between an operation's background goroutine and a concurrently
// open stream, so this is a plain poll loop — operations are expected to
// run for seconds to hours, not sub-second, so this interval is far
// finer-grained than actually needed for a useful progress stream.
const streamProgressPollInterval = 250 * time.Millisecond

// databaseServiceHandler implements bark's full DatabaseService: async
// CreateSnapshot/Restore/Truncate/VerifySnapshot (each with a pollable
// status RPC and, via StreamOperationProgress, a push-based one), the
// snapshot catalog (ListSnapshots/DeleteSnapshot/ListAvailableSnapshots),
// GetOperationHistory, GetDatabaseInfo, and CancelOperation.
//
// Snapshots are identified by a generated ID that is also its directory
// name directly under BarkConfig.SnapshotDir — there is no separate
// catalog store; ListSnapshots scans that directory for manifest.json
// files (lifecycle.ListSnapshots), which also means automatic
// epoch-boundary snapshots (internal/dblifecycle.Manager) show up in the
// same catalog for free, since they live under the same directory.
// ListAvailableSnapshots (mergedSnapshotCatalogPage) additionally merges
// in whatever lifecycle.ListCloudSnapshots finds at
// BarkConfig.SnapshotCloudDestination, deduplicated by ID (local wins) —
// so a snapshot whose local copy is gone but whose cloud mirror survives
// still shows up. Restore/VerifySnapshot/DeleteSnapshot
// (resolveSnapshotSource/cloudSnapshotExists) all fall back to that same
// cloud copy when no local one exists, so a snapshot
// ListAvailableSnapshots surfaces as cloud-only is fully usable through
// bark, not just the offline CLI: it can be restored, verified, and
// deleted (DeleteSnapshot removes whichever of the local/cloud copies
// actually exist, not just the local one).
//
// Only one operation may be in flight at a time, matching the service's
// documented invariant: a second CreateSnapshot/Restore/Truncate/
// VerifySnapshot while one is already running is rejected with
// CodeFailedPrecondition rather than queued.
//
// Operation history is in-memory only and does not survive a bark
// restart — GetOperationHistory reports what this process has seen since
// it started, not a durable audit log.
//
// CancelOperation cancels the context passed to the underlying
// lifecycle.Snapshot/Restore/Truncate call. Badger's native
// DB.Backup/DB.Load have no context parameter of their own; the blob
// plugin (database/plugin/blob/badger/backup.go) closes that gap by
// wrapping the io.Writer/io.Reader Badger streams through so cancellation
// is checked on every internal Write/Read Badger makes, not just once
// before the whole operation starts — so cancelling a running
// Snapshot/Restore takes effect within a chunk or two, not only once it
// would have finished anyway.
type databaseServiceHandler struct {
	databaseconnect.UnimplementedDatabaseServiceHandler
	bark *Bark

	mu         sync.Mutex
	operations map[string]*operation
	busy       bool
}

func newDatabaseServiceHandler(b *Bark) *databaseServiceHandler {
	return &databaseServiceHandler{
		bark:       b,
		operations: make(map[string]*operation),
	}
}

// operation tracks one async CreateSnapshot/Restore/Truncate/
// VerifySnapshot call from the moment its RPC returns an operation_id
// through its terminal state.
type operation struct {
	id        string
	opType    databasev1alpha1.OperationType
	startedAt time.Time
	cancel    context.CancelFunc

	mu              sync.Mutex
	status          databasev1alpha1.OperationStatus
	message         string
	updatedAt       time.Time
	completedAt     time.Time
	hasCompleted    bool
	cancelRequested bool
	snapshotID      string // CreateSnapshot only
	blocksRemoved   uint64 // Truncate only
}

func (o *operation) setRunning() {
	o.mu.Lock()
	o.status = databasev1alpha1.OperationStatus_OPERATION_STATUS_RUNNING
	o.message = "running"
	o.updatedAt = time.Now()
	o.mu.Unlock()
}

// complete records the terminal outcome of the operation. An error that
// is context.Canceled *and* followed a CancelOperation call is reported
// as OPERATION_STATUS_CANCELLED rather than FAILED; a context deadline or
// cancellation the operation hit on its own (no CancelOperation call) is
// still reported as FAILED, since nothing asked for it.
func (o *operation) complete(err error, blocksRemoved uint64) {
	o.mu.Lock()
	now := time.Now()
	o.updatedAt = now
	o.completedAt = now
	o.hasCompleted = true
	o.blocksRemoved = blocksRemoved
	switch {
	case err == nil:
		o.status = databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED
		o.message = "completed"
	case o.cancelRequested && errors.Is(err, context.Canceled):
		o.status = databasev1alpha1.OperationStatus_OPERATION_STATUS_CANCELLED
		o.message = "cancelled"
	default:
		o.status = databasev1alpha1.OperationStatus_OPERATION_STATUS_FAILED
		o.message = err.Error()
	}
	o.mu.Unlock()
}

// requestCancel cancels the operation's context and marks it as having
// been asked to cancel (see complete's doc comment), returning its status
// at the moment the request was accepted — which may still be RUNNING if
// the operation hasn't yet reached a context-checked boundary.
func (o *operation) requestCancel() databasev1alpha1.OperationStatus {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.hasCompleted {
		o.cancelRequested = true
		o.cancel()
	}
	return o.status
}

func (o *operation) progress() *databasev1alpha1.OperationProgress {
	o.mu.Lock()
	defer o.mu.Unlock()
	p := &databasev1alpha1.OperationProgress{
		OperationId: o.id,
		Status:      o.status,
		Message:     o.message,
		StartedAt:   timestamppb.New(o.startedAt),
		UpdatedAt:   timestamppb.New(o.updatedAt),
	}
	if o.status == databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED {
		p.ProgressPercent = 100
	}
	if o.hasCompleted {
		p.CompletedAt = timestamppb.New(o.completedAt)
	}
	return p
}

func (o *operation) record() *databasev1alpha1.OperationRecord {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec := &databasev1alpha1.OperationRecord{
		OperationId: o.id,
		Type:        o.opType,
		Status:      o.status,
		Message:     o.message,
		StartedAt:   timestamppb.New(o.startedAt),
	}
	if o.hasCompleted {
		rec.CompletedAt = timestamppb.New(o.completedAt)
	}
	return rec
}

// startOperation registers a new operation and marks the handler busy, or
// returns CodeFailedPrecondition if one is already running. The returned
// context is cancelled by CancelOperation (or by the caller, on request
// completion) and should be threaded through to whatever
// lifecycle.Snapshot/Restore/Truncate call the operation performs.
func (h *databaseServiceHandler) startOperation(
	opType databasev1alpha1.OperationType,
) (*operation, context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.busy {
		return nil, nil, connect.NewError(
			connect.CodeFailedPrecondition,
			errors.New("another database operation is already in progress"),
		)
	}
	ctx, cancel := context.WithCancel(context.Background())
	op := &operation{
		id:        uuid.NewString(),
		opType:    opType,
		startedAt: time.Now(),
		cancel:    cancel,
		status:    databasev1alpha1.OperationStatus_OPERATION_STATUS_PENDING,
		message:   "pending",
		updatedAt: time.Now(),
	}
	h.operations[op.id] = op
	h.busy = true
	return op, ctx, nil
}

func (h *databaseServiceHandler) finishOperation() {
	h.mu.Lock()
	h.busy = false
	h.mu.Unlock()
}

func (h *databaseServiceHandler) lookupOperation(id string) (*operation, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	op, ok := h.operations[id]
	if !ok {
		return nil, connect.NewError(
			connect.CodeNotFound,
			fmt.Errorf("unknown operation %q", id),
		)
	}
	return op, nil
}

// runProtected calls fn and recovers from any panic, converting it into a
// regular error. This matters because Badger's DB.Load can panic (not
// just return an error) on malformed input rather than the file it
// actually expects — reproduced by feeding VerifySnapshot a corrupted
// blob.bak — and a stored snapshot is exactly the kind of on-disk state
// Restore/VerifySnapshot must be able to safely reject as a failed
// operation instead of crashing the whole bark server process.
func runProtected(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("operation panicked: %v", r)
		}
	}()
	return fn()
}

// resolveSnapshotDir validates that snapshotID is a bare directory name —
// no path separators, and not "." or ".." — and resolves it under the
// configured SnapshotDir. snapshotID comes from an RPC request, so this
// rejects a path-traversal attempt (e.g. "../../etc") rather than trusting
// it to already be a safe single path component.
func (h *databaseServiceHandler) resolveSnapshotDir(snapshotID string) (string, error) {
	if snapshotID == "" {
		return "", connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("snapshot_id is required"),
		)
	}
	if snapshotID != filepath.Base(snapshotID) ||
		snapshotID == "." || snapshotID == ".." {
		return "", connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("invalid snapshot_id %q", snapshotID),
		)
	}
	return filepath.Join(h.bark.config.SnapshotDir, snapshotID), nil
}

//nolint:contextcheck // deliberately detached from the request context via startOperation; the async operation must outlive this RPC call
func (h *databaseServiceHandler) CreateSnapshot(
	_ context.Context,
	req *connect.Request[databasev1alpha1.CreateSnapshotRequest],
) (*connect.Response[databasev1alpha1.CreateSnapshotResponse], error) {
	op, ctx, err := h.startOperation(
		databasev1alpha1.OperationType_OPERATION_TYPE_SNAPSHOT,
	)
	if err != nil {
		return nil, err
	}
	snapshotID := uuid.NewString()
	op.mu.Lock()
	op.snapshotID = snapshotID
	op.mu.Unlock()
	destDir := filepath.Join(h.bark.config.SnapshotDir, snapshotID)
	name := req.Msg.GetName()
	description := req.Msg.GetDescription()

	go func() {
		defer h.finishOperation()
		op.setRunning()
		op.complete(runProtected(func() error {
			_, snapErr := h.bark.config.Lifecycle.Snapshot(ctx, destDir)
			if snapErr == nil && (name != "" || description != "") {
				if labelErr := lifecycle.LabelSnapshot(destDir, name, description); labelErr != nil {
					h.bark.config.Logger.Warn(
						"failed to label snapshot with name/description",
						"snapshot_id", snapshotID,
						"error", labelErr,
					)
				}
			}
			return snapErr
		}), 0)
	}()

	return connect.NewResponse(&databasev1alpha1.CreateSnapshotResponse{
		OperationId: op.id,
		SnapshotId:  snapshotID,
	}), nil
}

func (h *databaseServiceHandler) GetSnapshotStatus(
	_ context.Context,
	req *connect.Request[databasev1alpha1.GetSnapshotStatusRequest],
) (*connect.Response[databasev1alpha1.GetSnapshotStatusResponse], error) {
	op, err := h.lookupOperation(req.Msg.GetOperationId())
	if err != nil {
		return nil, err
	}
	op.mu.Lock()
	snapshotID := op.snapshotID
	op.mu.Unlock()
	return connect.NewResponse(&databasev1alpha1.GetSnapshotStatusResponse{
		Progress:   op.progress(),
		SnapshotId: snapshotID,
	}), nil
}

// snapshotInfoFromEntry converts a lifecycle.SnapshotEntry into the proto
// SnapshotInfo shape.
func snapshotInfoFromEntry(dir string, e lifecycle.SnapshotEntry) *databasev1alpha1.SnapshotInfo {
	m := e.Manifest
	return &databasev1alpha1.SnapshotInfo{
		SnapshotId:  e.ID,
		Name:        m.Name,
		Description: m.Description,
		Tip: &databasev1alpha1.BlockRef{
			Hash:        proto.String(hex.EncodeToString(m.TipHash)),
			Slot:        proto.Uint64(m.TipSlot),
			BlockNumber: proto.Uint64(m.TipBlockNumber),
		},
		SizeBytes: uint64(m.BlobBytes + m.MetadataBytes), //nolint:gosec // G115: file sizes are never negative
		Checksum:  "sha256:" + m.Checksum,
		CreatedAt: timestamppb.New(m.CreatedAt),
		Location:  dir,
	}
}

// encodePageToken/decodePageToken implement pagination as a plain
// offset into the (already sorted) result list. The proto documents page
// tokens as opaque, so this encoding is a private implementation detail,
// not a wire contract.
func encodePageToken(offset int) string {
	if offset <= 0 {
		return ""
	}
	return strconv.Itoa(offset)
}

func decodePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid page token %q", token)
	}
	return offset, nil
}

// snapshotCatalogPage returns one page of the local snapshot catalog.
func (h *databaseServiceHandler) snapshotCatalogPage(
	pageSize uint32,
	pageToken string,
) ([]*databasev1alpha1.SnapshotInfo, string, error) {
	offset, err := decodePageToken(pageToken)
	if err != nil {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, err)
	}
	entries, err := lifecycle.ListSnapshots(h.bark.config.SnapshotDir)
	if err != nil {
		return nil, "", connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("list snapshots: %w", err),
		)
	}
	if offset > len(entries) {
		offset = len(entries)
	}
	size := int(pageSize)
	if size <= 0 {
		size = defaultCatalogPageSize
	}
	end := offset + size
	var nextToken string
	if end < len(entries) {
		nextToken = encodePageToken(end)
	} else {
		end = len(entries)
	}
	infos := make([]*databasev1alpha1.SnapshotInfo, 0, end-offset)
	for _, e := range entries[offset:end] {
		infos = append(
			infos,
			snapshotInfoFromEntry(filepath.Join(h.bark.config.SnapshotDir, e.ID), e),
		)
	}
	return infos, nextToken, nil
}

func (h *databaseServiceHandler) ListSnapshots(
	_ context.Context,
	req *connect.Request[databasev1alpha1.ListSnapshotsRequest],
) (*connect.Response[databasev1alpha1.ListSnapshotsResponse], error) {
	infos, nextToken, err := h.snapshotCatalogPage(
		req.Msg.GetPageSize(),
		req.Msg.GetPageToken(),
	)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&databasev1alpha1.ListSnapshotsResponse{
		Snapshots:     infos,
		NextPageToken: nextToken,
	}), nil
}

// snapshotCatalogItem pairs a snapshot entry with the location string its
// SnapshotInfo should report — a local path for a locally-known entry, a
// cloud URI for one only discoverable via ListCloudSnapshots.
type snapshotCatalogItem struct {
	location string
	entry    lifecycle.SnapshotEntry
}

// mergedSnapshotCatalogPage returns one page of the combined local + cloud
// snapshot catalog, for ListAvailableSnapshots. A cloud entry whose ID
// already appears in the local catalog is skipped in favor of the local
// one (a real, already-open path beats reconstructing a cloud URI for
// something available locally); a cloud-only entry — its local copy
// already deleted via DeleteSnapshot, or the local directory pruned, but
// the cloud mirror still present — is what actually makes this RPC
// discover something ListSnapshots can't.
func (h *databaseServiceHandler) mergedSnapshotCatalogPage(
	ctx context.Context,
	pageSize uint32,
	pageToken string,
) ([]*databasev1alpha1.SnapshotInfo, string, error) {
	offset, err := decodePageToken(pageToken)
	if err != nil {
		return nil, "", connect.NewError(connect.CodeInvalidArgument, err)
	}

	localEntries, err := lifecycle.ListSnapshots(h.bark.config.SnapshotDir)
	if err != nil {
		return nil, "", connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("list snapshots: %w", err),
		)
	}

	seen := make(map[string]bool, len(localEntries))
	items := make([]snapshotCatalogItem, 0, len(localEntries))
	for _, e := range localEntries {
		seen[e.ID] = true
		items = append(items, snapshotCatalogItem{
			location: filepath.Join(h.bark.config.SnapshotDir, e.ID),
			entry:    e,
		})
	}

	cloudEntries, ok, err := lifecycle.ListCloudSnapshots(
		ctx, h.bark.config.SnapshotCloudDestination,
	)
	if err != nil {
		return nil, "", connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("list cloud snapshots: %w", err),
		)
	}
	if ok {
		for _, e := range cloudEntries {
			if seen[e.ID] {
				continue
			}
			seen[e.ID] = true
			items = append(items, snapshotCatalogItem{
				location: lifecycle.JoinCloudURI(h.bark.config.SnapshotCloudDestination, e.ID),
				entry:    e,
			})
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].entry.Manifest.CreatedAt.After(items[j].entry.Manifest.CreatedAt)
	})

	if offset > len(items) {
		offset = len(items)
	}
	size := int(pageSize)
	if size <= 0 {
		size = defaultCatalogPageSize
	}
	end := offset + size
	var nextToken string
	if end < len(items) {
		nextToken = encodePageToken(end)
	} else {
		end = len(items)
	}
	infos := make([]*databasev1alpha1.SnapshotInfo, 0, end-offset)
	for _, item := range items[offset:end] {
		infos = append(infos, snapshotInfoFromEntry(item.location, item.entry))
	}
	return infos, nextToken, nil
}

// ListAvailableSnapshots returns the combined local + cloud snapshot
// catalog (see mergedSnapshotCatalogPage) — snapshots eligible for
// restoration, including ones whose local copy is gone but a cloud
// mirror still exists. If databaseLifecycle.snapshotCloudDestination
// isn't configured, or its destination type doesn't support listing,
// this degrades to exactly ListSnapshots's local-only result.
func (h *databaseServiceHandler) ListAvailableSnapshots(
	ctx context.Context,
	req *connect.Request[databasev1alpha1.ListAvailableSnapshotsRequest],
) (*connect.Response[databasev1alpha1.ListAvailableSnapshotsResponse], error) {
	infos, nextToken, err := h.mergedSnapshotCatalogPage(
		ctx,
		req.Msg.GetPageSize(),
		req.Msg.GetPageToken(),
	)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&databasev1alpha1.ListAvailableSnapshotsResponse{
		Snapshots:     infos,
		NextPageToken: nextToken,
	}), nil
}

// cloudSnapshotExists checks whether snapshotID exists at the configured
// cloud destination — an empty SnapshotCloudDestination, an invalid one,
// or a destination type that doesn't implement CloudManifestFetcher all
// count as "no" (exists=false, err=nil) here, since none of those are
// really "no such snapshot," just "nothing to check." A confirmed-absent
// manifest (errors.Is(fetchErr, lifecycle.ErrCloudSnapshotNotFound)) is
// also exists=false, err=nil — that IS a real "no."
//
// Anything else — a real communication failure while trying to check
// (auth, network, timeout, throttling) — is returned as a non-nil err,
// not silently folded into exists=false: a caller that can't reach cloud
// storage right now must not report "snapshot not found" for a snapshot
// that may well still be there, since that's indistinguishable from
// actual data loss to whoever reads the response.
func (h *databaseServiceHandler) cloudSnapshotExists(
	ctx context.Context,
	snapshotID string,
) (cloudURI string, exists bool, err error) {
	if h.bark.config.SnapshotCloudDestination == "" {
		return "", false, nil
	}
	cloudURI = lifecycle.JoinCloudURI(h.bark.config.SnapshotCloudDestination, snapshotID)
	_, ok, fetchErr := lifecycle.FetchCloudManifest(ctx, cloudURI)
	if !ok {
		return cloudURI, false, nil
	}
	if fetchErr != nil {
		if errors.Is(fetchErr, lifecycle.ErrCloudSnapshotNotFound) {
			return cloudURI, false, nil
		}
		return cloudURI, false, fetchErr
	}
	return cloudURI, true, nil
}

// resolveSnapshotSource resolves snapshotID to wherever it can actually be
// read from for Restore/VerifySnapshot: its local directory if a valid
// manifest exists there, otherwise the configured cloud destination if a
// copy exists there. This is what lets a snapshot ListAvailableSnapshots
// surfaced as cloud-only (its local copy deleted or pruned) actually be
// restored/verified through bark, not just the offline CLI. Returns
// CodeNotFound if the snapshot exists in neither place.
func (h *databaseServiceHandler) resolveSnapshotSource(
	ctx context.Context,
	snapshotID string,
) (source string, err error) {
	localDir, err := h.resolveSnapshotDir(snapshotID)
	if err != nil {
		return "", err
	}
	_, localErr := lifecycle.ReadManifest(localDir)
	if localErr == nil {
		return localDir, nil
	}
	// A corrupted/hand-edited manifest means the snapshot IS there, just
	// unusable — report that distinctly rather than falling through to
	// "not found", which would otherwise be indistinguishable from a
	// snapshot ID that was never valid in the first place.
	if errors.Is(localErr, lifecycle.ErrManifestCorrupted) {
		return "", connect.NewError(
			connect.CodeDataLoss,
			fmt.Errorf(
				"snapshot %q has a corrupted manifest: %w",
				snapshotID,
				localErr,
			),
		)
	}
	cloudURI, exists, cloudErr := h.cloudSnapshotExists(ctx, snapshotID)
	if cloudErr != nil {
		return "", connect.NewError(
			connect.CodeUnavailable,
			fmt.Errorf(
				"check cloud destination for snapshot %q: %w",
				snapshotID,
				cloudErr,
			),
		)
	}
	if exists {
		return cloudURI, nil
	}
	return "", connect.NewError(
		connect.CodeNotFound,
		fmt.Errorf(
			"snapshot %q not found locally or in the configured cloud destination",
			snapshotID,
		),
	)
}

// DeleteSnapshot removes snapshotID's local copy, its cloud copy, or both
// — whichever actually exist. A snapshot present in both places (the
// common case, since "store at both" is the default) has both removed; a
// snapshot ListAvailableSnapshots surfaced as cloud-only (no local
// directory) still gets deleted, via lifecycle.DeleteCloudSnapshot,
// instead of silently no-op'ing on the missing local half.
func (h *databaseServiceHandler) DeleteSnapshot(
	ctx context.Context,
	req *connect.Request[databasev1alpha1.DeleteSnapshotRequest],
) (*connect.Response[databasev1alpha1.DeleteSnapshotResponse], error) {
	snapshotID := req.Msg.GetSnapshotId()
	localDir, err := h.resolveSnapshotDir(snapshotID)
	if err != nil {
		return nil, err
	}
	// Check the directory's existence directly rather than gating on a
	// readable manifest: a corrupted/hand-edited manifest must still be
	// deletable, or an operator would have no way to clean up a snapshot
	// that resolveSnapshotSource/VerifySnapshot already report as unusable.
	localExists := true
	if _, statErr := os.Stat(localDir); statErr != nil {
		if !errors.Is(statErr, os.ErrNotExist) {
			return nil, connect.NewError(
				connect.CodeInternal,
				fmt.Errorf("check local snapshot %q: %w", snapshotID, statErr),
			)
		}
		localExists = false
	}

	cloudURI, cloudExists, cloudErr := h.cloudSnapshotExists(ctx, snapshotID)
	if cloudErr != nil {
		return nil, connect.NewError(
			connect.CodeUnavailable,
			fmt.Errorf(
				"check cloud destination for snapshot %q: %w",
				snapshotID,
				cloudErr,
			),
		)
	}

	if !localExists && !cloudExists {
		return nil, connect.NewError(
			connect.CodeNotFound,
			fmt.Errorf(
				"snapshot %q not found locally or in the configured cloud destination",
				snapshotID,
			),
		)
	}

	if localExists {
		if err := os.RemoveAll(localDir); err != nil {
			return nil, connect.NewError(
				connect.CodeInternal,
				fmt.Errorf("delete local snapshot %q: %w", snapshotID, err),
			)
		}
	}
	if cloudExists {
		ok, delErr := lifecycle.DeleteCloudSnapshot(ctx, cloudURI)
		if delErr != nil {
			return nil, connect.NewError(
				connect.CodeInternal,
				fmt.Errorf("delete cloud snapshot %q: %w", snapshotID, delErr),
			)
		}
		if !ok {
			return nil, connect.NewError(
				connect.CodeUnimplemented,
				fmt.Errorf(
					"snapshot %q exists at %q but its destination type doesn't support deletion",
					snapshotID, cloudURI,
				),
			)
		}
	}

	return connect.NewResponse(&databasev1alpha1.DeleteSnapshotResponse{
		DeletedAt: timestamppb.New(time.Now()),
	}), nil
}

// verifySnapshotIntegrity performs a full restore of the snapshot at
// snapshotDir into a throwaway temporary directory, reusing
// lifecycle.Restore's existing validation (manifest checksum, both
// stores' restore, database.New's startup consistency checks, and a tip
// comparison) as the actual integrity check, rather than duplicating any
// of that logic. The temporary directory is always removed before
// returning.
func verifySnapshotIntegrity(ctx context.Context, snapshotDir string) error {
	tempDir, err := os.MkdirTemp("", "dingo-verify-snapshot-*")
	if err != nil {
		return fmt.Errorf("create verification directory: %w", err)
	}
	defer os.RemoveAll(tempDir)
	if _, err := lifecycle.Restore(ctx, snapshotDir, tempDir); err != nil {
		return fmt.Errorf("verify snapshot: %w", err)
	}
	return nil
}

// VerifySnapshot resolves snapshotID via resolveSnapshotSource — a local
// copy if one exists, otherwise the cloud copy — so a cloud-only snapshot
// can be verified the same way a local one can.
//
//nolint:contextcheck // deliberately detached from the request context via startOperation; the async operation must outlive this RPC call
func (h *databaseServiceHandler) VerifySnapshot(
	ctx context.Context,
	req *connect.Request[databasev1alpha1.VerifySnapshotRequest],
) (*connect.Response[databasev1alpha1.VerifySnapshotResponse], error) {
	source, err := h.resolveSnapshotSource(ctx, req.Msg.GetSnapshotId())
	if err != nil {
		return nil, err
	}
	op, opCtx, err := h.startOperation(
		databasev1alpha1.OperationType_OPERATION_TYPE_VERIFY,
	)
	if err != nil {
		return nil, err
	}

	go func() {
		defer h.finishOperation()
		op.setRunning()
		op.complete(runProtected(func() error {
			return verifySnapshotIntegrity(opCtx, source)
		}), 0)
	}()

	return connect.NewResponse(&databasev1alpha1.VerifySnapshotResponse{
		OperationId: op.id,
	}), nil
}

// Restore resolves snapshotID via resolveSnapshotSource — a local copy if
// one exists, otherwise the cloud copy — so a snapshot
// ListAvailableSnapshots surfaced as cloud-only can actually be restored
// through bark, not just the offline `dingo database restore <cloud-uri>`
// CLI. lifecycle.Restore already knows how to download a cloud URI
// source itself, so this needs no further change once the source is
// resolved.
//
//nolint:contextcheck // deliberately detached from the request context via startOperation; the async operation must outlive this RPC call
func (h *databaseServiceHandler) Restore(
	ctx context.Context,
	req *connect.Request[databasev1alpha1.RestoreRequest],
) (*connect.Response[databasev1alpha1.RestoreResponse], error) {
	source, err := h.resolveSnapshotSource(ctx, req.Msg.GetSnapshotId())
	if err != nil {
		return nil, err
	}
	op, opCtx, err := h.startOperation(
		databasev1alpha1.OperationType_OPERATION_TYPE_RESTORE,
	)
	if err != nil {
		return nil, err
	}

	go func() {
		defer h.finishOperation()
		op.setRunning()
		op.complete(runProtected(func() error {
			_, restoreErr := h.bark.config.Lifecycle.Restore(opCtx, source)
			return restoreErr
		}), 0)
	}()

	return connect.NewResponse(&databasev1alpha1.RestoreResponse{
		OperationId: op.id,
	}), nil
}

func (h *databaseServiceHandler) GetRestoreStatus(
	_ context.Context,
	req *connect.Request[databasev1alpha1.GetRestoreStatusRequest],
) (*connect.Response[databasev1alpha1.GetRestoreStatusResponse], error) {
	op, err := h.lookupOperation(req.Msg.GetOperationId())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&databasev1alpha1.GetRestoreStatusResponse{
		Progress: op.progress(),
	}), nil
}

// blockRefToTarget converts a BlockRef to a dblifecycle.TruncateTarget.
// The proto allows hash/slot/block_number to be combined as long as they
// agree on the same block; this doesn't implement that cross-validation
// yet, so — for a destructive operation — it requires exactly one field
// rather than risk silently trusting an unverified combination.
func blockRefToTarget(
	ref *databasev1alpha1.BlockRef,
) (dblifecycle.TruncateTarget, error) {
	if ref == nil {
		return dblifecycle.TruncateTarget{}, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("target is required"),
		)
	}
	var target dblifecycle.TruncateTarget
	set := 0
	if ref.Hash != nil {
		decoded, err := hex.DecodeString(ref.GetHash())
		if err != nil {
			return dblifecycle.TruncateTarget{}, connect.NewError(
				connect.CodeInvalidArgument,
				fmt.Errorf("invalid hash: %w", err),
			)
		}
		target.Hash = decoded
		set++
	}
	if ref.Slot != nil {
		target.Slot = ref.Slot
		set++
	}
	if ref.BlockNumber != nil {
		target.BlockNumber = ref.BlockNumber
		set++
	}
	if set != 1 {
		return dblifecycle.TruncateTarget{}, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New(
				"target must set exactly one of hash, slot, or block_number "+
					"(combined cross-validation not yet implemented)",
			),
		)
	}
	return target, nil
}

//nolint:contextcheck // deliberately detached from the request context via startOperation; the async operation must outlive this RPC call
func (h *databaseServiceHandler) Truncate(
	_ context.Context,
	req *connect.Request[databasev1alpha1.TruncateRequest],
) (*connect.Response[databasev1alpha1.TruncateResponse], error) {
	target, err := blockRefToTarget(req.Msg.GetTarget())
	if err != nil {
		return nil, err
	}
	op, ctx, err := h.startOperation(
		databasev1alpha1.OperationType_OPERATION_TYPE_TRUNCATE,
	)
	if err != nil {
		return nil, err
	}

	go func() {
		defer h.finishOperation()
		op.setRunning()
		var blocksRemoved uint64
		err := runProtected(func() error {
			var truncErr error
			blocksRemoved, truncErr = h.bark.config.Lifecycle.Truncate(ctx, target)
			return truncErr
		})
		op.complete(err, blocksRemoved)
	}()

	return connect.NewResponse(&databasev1alpha1.TruncateResponse{
		OperationId: op.id,
	}), nil
}

func (h *databaseServiceHandler) GetTruncateStatus(
	_ context.Context,
	req *connect.Request[databasev1alpha1.GetTruncateStatusRequest],
) (*connect.Response[databasev1alpha1.GetTruncateStatusResponse], error) {
	op, err := h.lookupOperation(req.Msg.GetOperationId())
	if err != nil {
		return nil, err
	}
	op.mu.Lock()
	blocksRemoved := op.blocksRemoved
	op.mu.Unlock()
	return connect.NewResponse(&databasev1alpha1.GetTruncateStatusResponse{
		Progress:      op.progress(),
		BlocksRemoved: blocksRemoved,
	}), nil
}

// StreamOperationProgress polls the operation's in-memory progress every
// streamProgressPollInterval and sends it to the client, returning once
// the operation reaches a terminal state or the stream's context is
// cancelled (e.g. the client disconnects). There is no push-based
// notification from an operation's background goroutine, so this is a
// plain poll loop rather than a true event stream — adequate given
// operations run for seconds to hours, far coarser than the poll
// interval.
func (h *databaseServiceHandler) StreamOperationProgress(
	ctx context.Context,
	req *connect.Request[databasev1alpha1.StreamOperationProgressRequest],
	stream *connect.ServerStream[databasev1alpha1.StreamOperationProgressResponse],
) error {
	op, err := h.lookupOperation(req.Msg.GetOperationId())
	if err != nil {
		return err
	}
	ticker := time.NewTicker(streamProgressPollInterval)
	defer ticker.Stop()
	for {
		progress := op.progress()
		if err := stream.Send(&databasev1alpha1.StreamOperationProgressResponse{
			Progress: progress,
		}); err != nil {
			return err
		}
		switch progress.GetStatus() { //nolint:exhaustive // only terminal statuses stop the loop; every other status falls through to keep streaming
		case databasev1alpha1.OperationStatus_OPERATION_STATUS_COMPLETED,
			databasev1alpha1.OperationStatus_OPERATION_STATUS_FAILED,
			databasev1alpha1.OperationStatus_OPERATION_STATUS_CANCELLED:
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *databaseServiceHandler) GetOperationHistory(
	_ context.Context,
	req *connect.Request[databasev1alpha1.GetOperationHistoryRequest],
) (*connect.Response[databasev1alpha1.GetOperationHistoryResponse], error) {
	offset, err := decodePageToken(req.Msg.GetPageToken())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	h.mu.Lock()
	all := make([]*operation, 0, len(h.operations))
	for _, op := range h.operations {
		all = append(all, op)
	}
	h.mu.Unlock()
	sort.Slice(all, func(i, j int) bool {
		return all[i].startedAt.After(all[j].startedAt)
	})

	typeFilter := req.Msg.TypeFilter
	statusFilter := req.Msg.StatusFilter

	records := make([]*databasev1alpha1.OperationRecord, 0, len(all))
	for _, op := range all {
		rec := op.record()
		if typeFilter != nil && rec.GetType() != *typeFilter {
			continue
		}
		if statusFilter != nil && rec.GetStatus() != *statusFilter {
			continue
		}
		records = append(records, rec)
	}

	if offset > len(records) {
		offset = len(records)
	}
	size := int(req.Msg.GetPageSize())
	if size <= 0 {
		size = defaultHistoryPageSize
	}
	end := offset + size
	var nextToken string
	if end < len(records) {
		nextToken = encodePageToken(end)
	} else {
		end = len(records)
	}

	return connect.NewResponse(&databasev1alpha1.GetOperationHistoryResponse{
		Records:       records[offset:end],
		NextPageToken: nextToken,
	}), nil
}

func (h *databaseServiceHandler) GetDatabaseInfo(
	_ context.Context,
	_ *connect.Request[databasev1alpha1.GetDatabaseInfoRequest],
) (*connect.Response[databasev1alpha1.GetDatabaseInfoResponse], error) {
	db, release, err := h.bark.Acquire()
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	defer release()

	tip, err := db.GetTip(nil)
	if err != nil {
		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("get tip: %w", err),
		)
	}

	// A full scan of the block-index keyspace (see its doc comment) —
	// deliberately accepted for this operator-facing diagnostic RPC over
	// adding a maintained counter, which would touch every block
	// insert/delete path across the codebase.
	blockCount, oldestSlot, err := db.CountBlocksAndOldestSlot(nil)
	if err != nil {
		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("count blocks: %w", err),
		)
	}

	h.mu.Lock()
	busy := h.busy
	var currentID string
	if busy {
		for id, op := range h.operations {
			op.mu.Lock()
			terminal := op.hasCompleted
			op.mu.Unlock()
			if !terminal {
				currentID = id
				break
			}
		}
	}
	h.mu.Unlock()

	// SizeBytes sums both stores' on-disk size. Both blob.BlobStore and
	// metadata.MetadataStore expose DiskSize as a cheap accessor (badger's
	// is an in-memory counter; sqlite's is a file/pragma lookup) — the
	// same accessor database.go's own background metrics goroutine
	// already calls. A plugin without a meaningful notion of on-disk size
	// (the s3/gcs blob plugins, which store nothing locally) returns
	// (0, nil), so this degrades to 0 for those rather than erroring.
	var sizeBytes uint64
	if blobSize, err := db.Blob().DiskSize(); err == nil && blobSize > 0 {
		sizeBytes += uint64(blobSize) //nolint:gosec // G115: guarded by blobSize > 0 above
	}
	if metadataSize, err := db.Metadata().DiskSize(); err == nil && metadataSize > 0 {
		sizeBytes += uint64(metadataSize) //nolint:gosec // G115: guarded by metadataSize > 0 above
	}

	resp := &databasev1alpha1.GetDatabaseInfoResponse{
		Tip: &databasev1alpha1.BlockRef{
			Hash:        proto.String(hex.EncodeToString(tip.Point.Hash)),
			Slot:        proto.Uint64(tip.Point.Slot),
			BlockNumber: proto.Uint64(tip.BlockNumber),
		},
		SizeBytes:           sizeBytes,
		BlockCount:          blockCount,
		OldestSlot:          oldestSlot,
		OperationInProgress: busy,
	}
	if currentID != "" {
		resp.CurrentOperationId = proto.String(currentID)
	}
	return connect.NewResponse(resp), nil
}

func (h *databaseServiceHandler) CancelOperation(
	_ context.Context,
	req *connect.Request[databasev1alpha1.CancelOperationRequest],
) (*connect.Response[databasev1alpha1.CancelOperationResponse], error) {
	op, err := h.lookupOperation(req.Msg.GetOperationId())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&databasev1alpha1.CancelOperationResponse{
		Status: op.requestCancel(),
	}), nil
}
