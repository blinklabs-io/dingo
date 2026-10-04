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
const { spawn, execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { once } = require('node:events');
const exec = promisify(execFile);
const root = process.env.DINGO_NPM_TEST_PACKAGE_ROOT || path.join(__dirname, '..', '..');

test('packed package installs through local, global and npx entry points', { timeout: 30_000 }, async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-npm-package-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const cache = path.join(dir, 'cache');
  const { version } = JSON.parse(await fs.readFile(path.join(root, 'package.json'), 'utf8'));
  const packed = JSON.parse((await exec('npm', [
    'pack', '--ignore-scripts', '--json', '--pack-destination', dir, '--cache', cache,
  ], { cwd: root })).stdout)[0];
  assert.deepEqual(packed.files.map((file) => file.path).sort(), [
    'LICENSE', 'README.md', 'npm/bin/dingo.js', 'npm/install.js', 'npm/postinstall.js', 'package.json',
  ]);
  const archive = path.join(dir, packed.filename);
  const fixtures = path.join(dir, 'fixture');
  await fs.mkdir(fixtures);
  await fs.writeFile(path.join(fixtures, 'dingo'), `#!/usr/bin/env node
const signalExitCodes = { SIGINT: 21, SIGTERM: 22, SIGHUP: 23 };
if (process.argv[2] === 'wait') {
  const signal = process.argv[3];
  process.on(signal, () => process.exit(signalExitCodes[signal]));
  console.log('ready ' + process.pid); setInterval(() => {}, 1000);
} else if (process.argv[2] === 'stdio') {
  let input = '';
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', (chunk) => { input += chunk; });
  process.stdin.on('end', () => process.stderr.write(input));
} else {
  console.log(JSON.stringify(process.argv.slice(2))); process.exit(Number(process.env.FIXTURE_EXIT || 0));
}
`);
  const release = path.join(dir, 'release.tar.gz');
  await exec('tar', ['-czf', release, '-C', fixtures, 'dingo']);
  const hook = path.join(dir, 'https-fixture.cjs');
  await fs.writeFile(hook, `const https = require('node:https');
const fs = require('node:fs'); const { EventEmitter } = require('node:events');
https.get = (url, callback) => {
  const version = ${JSON.stringify(version)};
  const expected = 'https://github.com/blinklabs-io/dingo/releases/download/v' + version +
    '/dingo-v' + version + '-' + process.platform + '-' +
    (process.arch === 'x64' ? 'amd64' : process.arch) + '.tar.gz';
  if (url.href !== expected) throw new Error('Unexpected release URL: ' + url.href);
  const request = new EventEmitter(); request.setTimeout = () => request;
  request.destroy = error => request.emit('error', error);
  queueMicrotask(() => { const response = fs.createReadStream(${JSON.stringify(release)});
    response.statusCode = 200; response.headers = {}; callback(response); });
  return request;
};
`);
  const env = { ...process.env, NODE_OPTIONS: `--require=${hook}` };
  const local = path.join(dir, 'local'); await fs.mkdir(local);
  await exec('npm', ['install', '--no-audit', '--no-fund', '--cache', cache, archive], { cwd: local, env });
  const executable = path.join(local, 'node_modules', '.bin', 'dingo');
  assert.equal((await exec(executable, ['one', 'two words', '--flag=value'])).stdout,
    '["one","two words","--flag=value"]\n');
  const streams = spawn(executable, ['stdio'], { stdio: ['pipe', 'ignore', 'pipe'] });
  t.after(() => { if (streams.exitCode === null) streams.kill('SIGKILL'); });
  let stderr = '';
  streams.stderr.setEncoding('utf8');
  streams.stderr.on('data', (chunk) => { stderr += chunk; });
  const streamsClosed = once(streams, 'close');
  streams.stdin.end('forwarded input');
  assert.deepEqual(await streamsClosed, [0, null]);
  assert.equal(stderr, 'forwarded input');
  await assert.rejects(exec(executable, ['exit'], {
    env: { ...process.env, FIXTURE_EXIT: '17' },
  }), (error) => error.code === 17);
  for (const [signal, exitCode] of [['SIGINT', 21], ['SIGTERM', 22], ['SIGHUP', 23]]) {
    const terminated = spawn(executable, ['wait', signal], { stdio: ['ignore', 'pipe', 'pipe'] });
    let fixturePid;
    t.after(() => {
      if (terminated.exitCode === null && terminated.signalCode === null) terminated.kill('SIGKILL');
      if (fixturePid) {
        try { process.kill(fixturePid, 'SIGKILL'); } catch (error) {
          if (error.code !== 'ESRCH') throw error;
        }
      }
    });
    const terminatedExit = once(terminated, 'exit');
    await new Promise((resolve, reject) => {
      let output = '';
      terminated.stdout.on('data', (chunk) => {
        output += chunk;
        const ready = output.match(/ready (\d+)\n/);
        if (ready) {
          fixturePid = Number(ready[1]);
          resolve();
        }
      });
      terminated.once('error', reject);
      terminated.once('exit', () => {
        if (!fixturePid) reject(new Error('fixture exited before ready'));
      });
    });
    terminated.kill(signal);
    let timeout;
    const timedOut = new Promise((_, reject) => {
      timeout = setTimeout(
        () => reject(new Error(`wrapper did not forward ${signal}`)),
        5_000,
      );
    });
    const forwardedExit = await Promise.race([terminatedExit, timedOut]);
    clearTimeout(timeout);
    assert.deepEqual(forwardedExit, [exitCode, null]);
    fixturePid = undefined;
  }
  const prefix = path.join(dir, 'global');
  await exec('npm', [
    'install', '--global', '--prefix', prefix, '--no-audit', '--no-fund', '--cache', cache, archive,
  ], { env });
  assert.equal((await exec(path.join(prefix, 'bin', 'dingo'), ['global'])).stdout, '["global"]\n');
  assert.equal((await exec('npm', [
    'exec', '--yes', '--offline', '--cache', cache, `--package=${archive}`, '--', 'dingo', 'npx',
  ], { cwd: dir, env })).stdout.trim(), '["npx"]');
  const failureHook = path.join(dir, 'https-failure.cjs');
  await fs.writeFile(failureHook, (await fs.readFile(hook, 'utf8'))
    .replace('response.statusCode = 200', 'response.statusCode = 404'));
  const failed = path.join(dir, 'failed'); await fs.mkdir(failed);
  await assert.rejects(exec('npm', ['install', '--no-audit', '--no-fund', '--cache', cache, archive], {
    cwd: failed, env: { ...process.env, NODE_OPTIONS: `--require=${failureHook}` },
  }), (error) => {
    assert.equal(error.code, 1);
    assert.match(error.stderr, /Cannot install Dingo:.*HTTP 404/);
    return true;
  });
});

test('wrapper fails clearly when installation was disabled', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'dingo-npm-missing-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  await fs.cp(path.join(root, 'npm'), path.join(dir, 'npm'), {
    recursive: true, filter: (source) => !source.includes(`${path.sep}vendor`),
  });
  await assert.rejects(exec(process.execPath, [path.join(dir, 'npm', 'bin', 'dingo.js')]), (error) => {
    assert.equal(error.code, 1); assert.match(error.stderr, /Cannot run Dingo:.*npm rebuild/); return true;
  });
});
