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

package ouroboros

import (
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
)

type leiosBackfillPointSources struct {
	announcers map[ouroboros.ConnectionId]leiosBackfillSource
	seq        uint64
}

type leiosBackfillSource struct {
	owner      *oleiosnotify.Client
	recordedAt time.Time
	seq        uint64
}

// leiosBackfillMaxSourcesPerPoint bounds connection churn independently of
// the number of serveable endorser-block points retained in the cache.
const leiosBackfillMaxSourcesPerPoint = 64

func (o *Ouroboros) recordLeiosBackfillSource(
	point ocommon.Point,
	connId ouroboros.ConnectionId,
	owner *oleiosnotify.Client,
) {
	if owner == nil {
		return
	}
	now := time.Now()
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosBackfillSourcesMu.Lock()
	defer o.leiosBackfillSourcesMu.Unlock()
	o.pruneLeiosBackfillSourcesLocked(now)
	sources := o.leiosBackfillSources[key]
	if sources == nil {
		sources = &leiosBackfillPointSources{
			announcers: make(map[ouroboros.ConnectionId]leiosBackfillSource),
		}
		o.leiosBackfillSources[key] = sources
	}
	o.leiosBackfillSourceSeq++
	sources.seq = o.leiosBackfillSourceSeq
	sources.announcers[connId] = leiosBackfillSource{
		owner:      owner,
		recordedAt: now,
		seq:        o.leiosBackfillSourceSeq,
	}
	o.pruneLeiosBackfillSourcesLocked(now)
}

func (o *Ouroboros) leiosBackfillSourceConn(
	point ocommon.Point,
	connId ouroboros.ConnectionId,
) *ouroboros.Connection {
	if o.connManager == nil {
		return nil
	}
	conn := o.connManager.GetConnectionById(connId)
	if conn == nil || conn.LeiosNotify() == nil || conn.LeiosFetch() == nil ||
		conn.LeiosFetch().Client == nil {
		return nil
	}
	if !o.leiosRecordedBackfillSource(
		point,
		connId,
		conn.LeiosNotify().Client,
	) {
		return nil
	}
	return conn
}

func (o *Ouroboros) leiosRecordedBackfillSource(
	point ocommon.Point,
	connId ouroboros.ConnectionId,
	owner *oleiosnotify.Client,
) bool {
	now := time.Now()
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosBackfillSourcesMu.Lock()
	defer o.leiosBackfillSourcesMu.Unlock()
	o.pruneLeiosBackfillSourcesLocked(now)
	sources := o.leiosBackfillSources[key]
	if sources == nil {
		return false
	}
	source, ok := sources.announcers[connId]
	return ok && owner != nil && source.owner == owner
}

// pruneLeiosBackfillSourcesLocked gives source provenance the same time and
// point-count horizon as the serveable endorser-block cache, with a separate
// per-point bound for reconnect churn. Callers hold leiosBackfillSourcesMu.
func (o *Ouroboros) pruneLeiosBackfillSourcesLocked(now time.Time) {
	cutoff := now.Add(-leiosEndorserBlockCacheTTL)
	for key, sources := range o.leiosBackfillSources {
		if sources == nil {
			delete(o.leiosBackfillSources, key)
			continue
		}
		for connId, source := range sources.announcers {
			if source.recordedAt.Before(cutoff) {
				delete(sources.announcers, connId)
			}
		}
		if len(sources.announcers) == 0 {
			delete(o.leiosBackfillSources, key)
		}
	}
	for _, sources := range o.leiosBackfillSources {
		for len(sources.announcers) > leiosBackfillMaxSourcesPerPoint {
			oldestConn, found := oldestLeiosBackfillSource(sources.announcers)
			if !found {
				break
			}
			delete(sources.announcers, oldestConn)
		}
	}
	for len(o.leiosBackfillSources) > leiosEndorserBlockCacheMaxEntries {
		var oldestKey string
		var oldestSeq uint64
		for key, sources := range o.leiosBackfillSources {
			if oldestKey == "" || sources.seq < oldestSeq {
				oldestKey = key
				oldestSeq = sources.seq
			}
		}
		delete(o.leiosBackfillSources, oldestKey)
	}
}

func oldestLeiosBackfillSource(
	sources map[ouroboros.ConnectionId]leiosBackfillSource,
) (ouroboros.ConnectionId, bool) {
	var oldest ouroboros.ConnectionId
	var oldestSeq uint64
	found := false
	for connId, source := range sources {
		if !found || source.seq < oldestSeq {
			oldest = connId
			oldestSeq = source.seq
			found = true
		}
	}
	return oldest, found
}
