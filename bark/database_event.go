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
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	"github.com/blinklabs-io/dingo/event"
)

// DatabaseOperationEventType carries a DatabaseOperationEvent. The
// DatabaseService publishes it each time a snapshot, restore, truncate or
// verify operation enters a new status.
const DatabaseOperationEventType event.EventType = "database.operation"

// DatabaseOperationEvent reports the status a DatabaseService operation
// entered. OperationID is the id GetOperationHistory and the status calls use.
type DatabaseOperationEvent struct {
	OperationID string
	Type        databasev1alpha1.OperationType
	Status      databasev1alpha1.OperationStatus
	// Error is why the operation failed. Empty unless Status is FAILED.
	Error string
}
