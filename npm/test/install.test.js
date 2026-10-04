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
      if (response instanceof Error) {
        request.emit('error', response);
        return;
      }
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
      requests(Array.from({ length: status === 500 ? 3 : 1 }, () => ({ status }))).get,
      { retryDelays: [0, 0] }), new RegExp(`HTTP ${status}`));
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
    download('https://github.com/archive', path.join(dir, 'interrupted'), unavailable,
      { retryDelays: [] }),
    /connection interrupted/,
  );
});

test('download retries transient failures, removes partial files, and keeps 4xx terminal', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-download-retry-test-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const target = path.join(dir, 'archive');
  const fixture = requests([
    { status: 503 },
    Object.assign(new Error('socket reset'), { code: 'ECONNRESET' }),
    { status: 200, body: [Buffer.from('complete')] },
  ]);
  const delays = [];
  await download('https://github.com/archive', target, fixture.get, {
    retryDelays: [7, 11], wait: async (delay) => delays.push(delay),
  });
  assert.equal(await fs.readFile(target, 'utf8'), 'complete');
  assert.deepEqual(delays, [7, 11]);
  assert.equal(fixture.urls.length, 3);

  const terminal = requests([{ status: 404 }, { status: 200, body: ['unexpected'] }]);
  await assert.rejects(download('https://github.com/missing', path.join(dir, 'missing'),
    terminal.get, { retryDelays: [0, 0] }), /HTTP 404/);
  assert.equal(terminal.urls.length, 1);
});

test('download bounds archive bytes and removes the partial destination', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-download-size-test-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const target = path.join(dir, 'archive');
  await assert.rejects(download('https://github.com/archive', target,
    requests([{ status: 200, body: [Buffer.alloc(9)] }]).get,
    { maxBytes: 8, retryDelays: [] }), /archive exceeds 8 bytes/);
  await assert.rejects(fs.stat(target), (error) => error.code === 'ENOENT');
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
  const digest = crypto.createHash('sha256').update(await fs.readFile(archive)).digest('hex');
  const filename = 'dingo-v1.2.3-linux-amd64.tar.gz';
  const checksums = { [filename]: digest };
  const target = await install(root, { platform: 'linux', arch: 'x64', download: async (url, destination) => {
    assert.equal(url, releaseURL('1.2.3', 'linux', 'x64'));
    await fs.copyFile(archive, destination);
  }, checksums });
  assert.equal(target, binaryPath(root));
  assert.equal((await fs.stat(target)).mode & 0o777, 0o755);
  assert.equal((await exec(target)).stdout, 'fixture\n');
  assert.deepEqual(await fs.readdir(path.dirname(target)), ['dingo']);
  await assert.rejects(install(root, { platform: 'linux', arch: 'x64', download: async (_, destination) => {
    await fs.writeFile(destination, 'not an archive');
  }, checksums: { [filename]: crypto.createHash('sha256').update('not an archive').digest('hex') } }),
  /Cannot extract Dingo release/);
  assert.equal((await exec(target)).stdout, 'fixture\n');
  assert.deepEqual(await fs.readdir(path.dirname(target)), ['dingo']);
});

test('installation rejects missing and mismatched checksums before replacing the binary', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-install-integrity-test-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const root = path.join(dir, 'package');
  await fs.mkdir(path.join(root, 'npm', 'vendor'), { recursive: true });
  await fs.writeFile(path.join(root, 'package.json'), JSON.stringify({ version: '1.2.3' }));
  const target = binaryPath(root);
  await fs.writeFile(target, 'installed');
  const filename = 'dingo-v1.2.3-linux-amd64.tar.gz';
  let downloaded = false;
  await assert.rejects(install(root, {
    platform: 'linux', arch: 'x64', checksums: {},
    download: async () => { downloaded = true; },
  }), /No valid checksum/);
  assert.equal(downloaded, false);
  await assert.rejects(install(root, {
    platform: 'linux', arch: 'x64', checksums: { [filename]: '0'.repeat(64) },
    download: async (_, destination) => fs.writeFile(destination, 'substituted'),
  }), /Checksum mismatch/);
  assert.equal(await fs.readFile(target, 'utf8'), 'installed');
  assert.deepEqual(await fs.readdir(path.dirname(target)), ['dingo']);
});

test('installation bounds archives and extracted binaries before atomic replacement', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-install-size-test-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const root = path.join(dir, 'package');
  const inputs = path.join(dir, 'inputs');
  await fs.mkdir(path.join(root, 'npm', 'vendor'), { recursive: true });
  await fs.mkdir(inputs);
  await fs.writeFile(path.join(root, 'package.json'), JSON.stringify({ version: '1.2.3' }));
  const target = binaryPath(root);
  await fs.writeFile(target, 'installed');
  await fs.writeFile(path.join(inputs, 'dingo'), Buffer.alloc(32, 1));
  const archive = path.join(dir, 'release.tar.gz');
  await exec('tar', ['-czf', archive, '-C', inputs, 'dingo']);
  const archiveBytes = await fs.readFile(archive);
  const filename = 'dingo-v1.2.3-linux-amd64.tar.gz';
  const options = {
    platform: 'linux', arch: 'x64',
    checksums: { [filename]: crypto.createHash('sha256').update(archiveBytes).digest('hex') },
    download: async (_, destination) => fs.copyFile(archive, destination),
  };
  await assert.rejects(install(root, { ...options, maxArchiveBytes: archiveBytes.length - 1 }),
    /archive exceeds/);
  await assert.rejects(install(root, { ...options, maxBinaryBytes: 31 }), /binary exceeds 31 bytes/);
  assert.equal(await fs.readFile(target, 'utf8'), 'installed');
  assert.deepEqual(await fs.readdir(path.dirname(target)), ['dingo']);
});
