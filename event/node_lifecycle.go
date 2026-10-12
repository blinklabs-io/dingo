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

package event

import "time"

// NodeLifecycleEventType is emitted when the node accepts a remote stop or
// restart request, before the shutdown begins.
const NodeLifecycleEventType = EventType("node.lifecycle")

// NodeLifecycleState is the transition a NodeLifecycleEvent announces.
type NodeLifecycleState string

const (
	// NodeLifecycleStopping announces a graceful stop.
	NodeLifecycleStopping NodeLifecycleState = "stopping"
	// NodeLifecycleRestarting announces a graceful stop followed by
	// re-execution of the node process.
	NodeLifecycleRestarting NodeLifecycleState = "restarting"
)

// NodeLifecycleEvent carries the accepted request's graceful timeout and the
// deadline after which the process is forced to exit or re-execute.
type NodeLifecycleEvent struct {
	State    NodeLifecycleState
	Timeout  time.Duration
	Deadline time.Time
}
