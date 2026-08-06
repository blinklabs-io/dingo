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

//go:build dingo_extra_plugins

package conformance

// NewDingoMysqlStateManager retains the tagged conformance entry point. The
// conformance state machine is backend-neutral and no longer duplicates the
// metadata schema through an ORM.
func NewDingoMysqlStateManager(_ string) (*DingoStateManager, error) {
	return NewDingoStateManager()
}
