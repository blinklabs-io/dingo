#!/usr/bin/env bash

# Copyright 2026 Blink Labs Software
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Legacy setup script -- no longer needed.
#
# Genesis files and pool keys are now generated automatically by the
# Antithesis configurator service defined in docker-compose.yml.
# The configurator runs before any nodes start (via depends_on) and
# generates unique key sets for each pool from testnet.yaml.
#
# This script is kept as a no-op for backwards compatibility with
# any external tooling that may call it.

echo "Genesis setup is handled by the configurator service."
echo "Run 'docker compose up -d' to start the DevNet."
