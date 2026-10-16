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

package dmq

// topicMagic maps a DMQ topic to its network magic on each Cardano network.
var topicMagic = map[string]map[string]uint32{
	"mithril": {
		"mainnet": 2912307721,
		"preprod": 2147483649,
		"preview": 2147483650,
	},
}

// TopicNetworkMagic returns the DMQ network magic for topic on the named
// Cardano network. It reports false when the topic is unknown or has no magic
// on that network.
func TopicNetworkMagic(topic, network string) (uint32, bool) {
	magic, ok := topicMagic[topic][network]
	return magic, ok
}
