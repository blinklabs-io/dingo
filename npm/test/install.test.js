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
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { EventEmitter } = require('node:events');
const { Readable } = require('node:stream');
const { binaryPath, download, install, releaseURL } = require('../install');
const exec = promisify(execFile);

test('release targets match the shipped archive names and reject absent targets', () => {
  for (const [platform, arch, goArch] of [
    ['linux', 'x64', 'amd64'], ['linux', 'arm64', 'arm64'],
    ['freebsd', 'x64', 'amd64'], ['freebsd', 'arm64', 'arm64'],
    ['darwin', 'x64', 'amd64'], ['darwin', 'arm64', 'arm64'],
  ]) {
    assert.equal(releaseURL('1.2.3-rc.1', platform, arch),
      'https://github.com/blinklabs-io/dingo/releases/download/v1.2.3-rc.1/' +
      `dingo-v1.2.3-rc.1-${platform}-${goArch}.tar.gz`);
  }
  for (const [platform, arch] of [['win32', 'x64'], ['darwin', 'ia32'], ['linux', 'ia32']]) {
    assert.throws(() => releaseURL('1.2.3', platform, arch), /No Dingo release binary/);
  }
  assert.throws(() => releaseURL('../other', 'linux', 'x64'), /Invalid Dingo package version/);
  assert.throws(() => releaseURL('1.2.3+build.7', 'linux', 'x64'), /Invalid Dingo package version/);
});

function requests(responses) {
  const urls = [];
  const get = (url, callback) => {
    urls.push(url.href);
    const request = new EventEmitter();
    request.setTimeout = () => request;
    request.destroy = (error) => request.emit('error', error);
    queueMicrotask(() => {
      const response = responses.shift();
      const stream = Readable.from(response.body || []);
      stream.statusCode = response.status;
      stream.headers = response.headers || {};
      callback(stream);
    });
    return request;
  };
  return { get, urls };
}

test('download follows HTTPS redirects and preserves archive bytes', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-download-test-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const fixture = requests([
    { status: 302, headers: { location: 'https://release-assets.githubusercontent.com/archive' } },
    { status: 200, body: [Buffer.from([0, 1, 255])] },
  ]);
  const target = path.join(dir, 'archive');
  await download('https://github.com/archive', target, fixture.get);
  assert.deepEqual(await fs.readFile(target), Buffer.from([0, 1, 255]));
  assert.deepEqual(fixture.urls, [
    'https://github.com/archive', 'https://release-assets.githubusercontent.com/archive',
  ]);
});

test('download rejects HTTP failures, downgrade and redirect loops', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-download-test-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  for (const status of [404, 500]) {
    await assert.rejects(download('https://github.com/archive', path.join(dir, String(status)),
      requests([{ status }]).get), new RegExp(`HTTP ${status}`));
  }
  await assert.rejects(download('https://github.com/archive', path.join(dir, 'downgrade'),
    requests([{ status: 302, headers: { location: 'http://other/archive' } }]).get), /require HTTPS/);
  await assert.rejects(download('https://github.com/archive', path.join(dir, 'malformed'),
    requests([{ status: 302, headers: { location: 'https://[' } }]).get), /Invalid URL/);
  await assert.rejects(download('https://github.com/archive', path.join(dir, 'loop'),
    requests(Array.from({ length: 6 }, () => ({
      status: 302, headers: { location: '/archive' },
    }))).get), /excessive/);
  const unavailable = () => {
    const request = new EventEmitter(); request.setTimeout = () => request;
    queueMicrotask(() => request.emit('error', new Error('connection interrupted')));
    return request;
  };
  await assert.rejects(
    download('https://github.com/archive', path.join(dir, 'interrupted'), unavailable),
    /connection interrupted/,
  );
});

test('installation extracts only dingo and preserves an installed binary on failure', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-install-test-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const root = path.join(dir, 'package');
  const inputs = path.join(dir, 'inputs');
  await fs.mkdir(root); await fs.mkdir(inputs);
  await fs.writeFile(path.join(root, 'package.json'), JSON.stringify({ version: '1.2.3' }));
  await fs.writeFile(path.join(inputs, 'dingo'), '#!/bin/sh\nprintf "fixture\\n"\n');
  await fs.writeFile(path.join(inputs, 'other'), 'must not be installed');
  const archive = path.join(dir, 'release.tar.gz');
  await exec('tar', ['-czf', archive, '-C', inputs, 'dingo', 'other']);
  const target = await install(root, { platform: 'linux', arch: 'x64', download: async (url, destination) => {
    assert.equal(url, releaseURL('1.2.3', 'linux', 'x64'));
    await fs.copyFile(archive, destination);
  } });
  assert.equal(target, binaryPath(root));
  assert.equal((await fs.stat(target)).mode & 0o777, 0o755);
  assert.equal((await exec(target)).stdout, 'fixture\n');
  assert.deepEqual(await fs.readdir(path.dirname(target)), ['dingo']);
  await assert.rejects(install(root, { platform: 'linux', arch: 'x64', download: async (_, destination) => {
    await fs.writeFile(destination, 'not an archive');
  } }), /Cannot extract Dingo release/);
  assert.equal((await exec(target)).stdout, 'fixture\n');
  assert.deepEqual(await fs.readdir(path.dirname(target)), ['dingo']);
});
