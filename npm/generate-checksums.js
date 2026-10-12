// Copyright 2026 Blink Labs Software
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
'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

function releaseFilenames(version) {
  return [
    'linux-amd64', 'linux-arm64',
    'freebsd-amd64', 'freebsd-arm64',
    'darwin-amd64', 'darwin-arm64',
  ].map((target) => `dingo-v${version}-${target}.tar.gz`).sort();
}

function generateChecksums(assetDirectory, output, version) {
  const expected = releaseFilenames(version);
  const actual = fs.readdirSync(assetDirectory).sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`release assets differ: expected ${expected}; got ${actual}`);
  }
  const checksums = Object.fromEntries(expected.map((filename) => [
    filename,
    crypto.createHash('sha256')
      .update(fs.readFileSync(path.join(assetDirectory, filename)))
      .digest('hex'),
  ]));
  fs.writeFileSync(output, `${JSON.stringify(checksums, null, 2)}\n`);
  return checksums;
}

if (require.main === module) {
  if (!process.env.ASSET_DIR) throw new Error('ASSET_DIR is required');
  const { version } = require('../package.json');
  generateChecksums(process.env.ASSET_DIR, path.join(__dirname, 'checksums.json'), version);
}

module.exports = { generateChecksums, releaseFilenames };
