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

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { generateChecksums } = require('../generate-checksums');

test('checksum generation requires and hashes all six release targets', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-checksums-test-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const assets = path.join(dir, 'assets');
  const output = path.join(dir, 'checksums.json');
  await fs.mkdir(assets);
  const expected = [
    'dingo-v1.2.3-darwin-amd64.tar.gz',
    'dingo-v1.2.3-darwin-arm64.tar.gz',
    'dingo-v1.2.3-freebsd-amd64.tar.gz',
    'dingo-v1.2.3-freebsd-arm64.tar.gz',
    'dingo-v1.2.3-linux-amd64.tar.gz',
    'dingo-v1.2.3-linux-arm64.tar.gz',
  ];
  for (const [index, filename] of expected.entries()) {
    await fs.writeFile(path.join(assets, filename), `asset-${index}`);
  }
  const checksums = generateChecksums(assets, output, '1.2.3');
  assert.deepEqual(Object.keys(checksums).sort(), expected);
  for (const [index, filename] of expected.entries()) {
    assert.equal(checksums[filename],
      crypto.createHash('sha256').update(`asset-${index}`).digest('hex'));
  }
  assert.deepEqual(JSON.parse(await fs.readFile(output, 'utf8')), checksums);

  await fs.rm(path.join(assets, expected[0]));
  assert.throws(() => generateChecksums(assets, output, '1.2.3'), /release assets differ/);
  await fs.writeFile(path.join(assets, expected[0]), 'restored');
  await fs.writeFile(path.join(assets, 'unexpected.tar.gz'), 'extra');
  assert.throws(() => generateChecksums(assets, output, '1.2.3'), /release assets differ/);
});
