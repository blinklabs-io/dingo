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
const fsp = require('node:fs/promises');
const https = require('node:https');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { Transform } = require('node:stream');
const { pipeline } = require('node:stream/promises');

const MAX_ARCHIVE_BYTES = 256 * 1024 * 1024;
const MAX_BINARY_BYTES = 512 * 1024 * 1024;
const RETRY_DELAYS_MS = [100, 500];

function releaseTarget(version, platform = process.platform, arch = process.arch) {
  const targets = {
    linux: { x64: 'amd64', arm64: 'arm64' },
    freebsd: { x64: 'amd64', arm64: 'arm64' },
    darwin: { x64: 'amd64', arm64: 'arm64' },
  };
  const goArch = targets[platform]?.[arch];
  if (!goArch) throw new Error(`No Dingo release binary for ${platform}/${arch}`);
  if (!/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error(`Invalid Dingo package version: ${version}`);
  }
  const filename = `dingo-v${version}-${platform}-${goArch}.tar.gz`;
  return {
    filename,
    url: `https://github.com/blinklabs-io/dingo/releases/download/v${version}/${filename}`,
  };
}

function releaseURL(version, platform = process.platform, arch = process.arch) {
  return releaseTarget(version, platform, arch).url;
}

function byteLimit(maxBytes, description) {
  let received = 0;
  return new Transform({
    transform(chunk, encoding, callback) {
      received += chunk.length;
      if (received > maxBytes) {
        callback(new Error(`${description} exceeds ${maxBytes} bytes`));
        return;
      }
      callback(null, chunk);
    },
  });
}

function downloadOnce(url, destination, get, redirects, maxBytes) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    if (parsed.protocol !== 'https:') {
      reject(new Error('Dingo release downloads require HTTPS'));
      return;
    }
    const request = get(parsed, (response) => {
      if ([301, 302, 303, 307, 308].includes(response.statusCode)) {
        response.resume();
        if (!response.headers.location || redirects >= 5) {
          reject(new Error('Invalid or excessive Dingo release redirects'));
          return;
        }
        let redirect;
        try {
          redirect = new URL(response.headers.location, parsed).href;
        } catch (error) {
          reject(error);
          return;
        }
        downloadOnce(redirect, destination, get, redirects + 1, maxBytes)
          .then(resolve, reject);
        return;
      }
      if (response.statusCode !== 200) {
        response.resume();
        const error = new Error(`Dingo release download returned HTTP ${response.statusCode}`);
        error.retryable = response.statusCode >= 500 && response.statusCode <= 599;
        reject(error);
        return;
      }
      response.on('error', (error) => { error.retryable = true; });
      pipeline(
        response,
        byteLimit(maxBytes, 'Dingo release archive'),
        fs.createWriteStream(destination, { flags: 'wx' }),
      ).then(resolve, reject);
    });
    request.on('error', (error) => {
      error.retryable = true;
      reject(error);
    });
    request.setTimeout(30_000, () => request.destroy(new Error('Dingo release download timed out')));
  });
}

async function download(url, destination, get = https.get, options = {}) {
  const delays = options.retryDelays ?? RETRY_DELAYS_MS;
  const wait = options.wait ?? ((milliseconds) => new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  }));
  const maxBytes = options.maxBytes ?? MAX_ARCHIVE_BYTES;
  for (let attempt = 0; ; attempt += 1) {
    await fsp.rm(destination, { force: true });
    try {
      await downloadOnce(url, destination, get, 0, maxBytes);
      return;
    } catch (error) {
      await fsp.rm(destination, { force: true });
      if (!error.retryable || attempt >= delays.length) throw error;
      await wait(delays[attempt]);
    }
  }
}

async function sha256(filename) {
  const digest = crypto.createHash('sha256');
  await pipeline(fs.createReadStream(filename), digest);
  return digest.digest('hex');
}

async function extractBinary(archive, destination, maxBytes = MAX_BINARY_BYTES) {
  // Stream only the shipped member: archive paths never become filesystem paths.
  const child = spawn('tar', ['-xzOf', archive, 'dingo'], { stdio: ['ignore', 'pipe', 'pipe'] });
  let diagnostic = '';
  child.stderr.on('data', (chunk) => { diagnostic = (diagnostic + chunk).slice(-8192); });
  const exited = new Promise((resolve) => {
    child.once('error', (error) => resolve({ error }));
    child.once('close', (code, signal) => {
      resolve({ code, signal });
    });
  });
  try {
    await pipeline(
      child.stdout,
      byteLimit(maxBytes, 'Dingo release binary'),
      fs.createWriteStream(destination, { flags: 'wx', mode: 0o755 }),
    );
    const result = await exited;
    if (result.error) throw result.error;
    if (result.code !== 0) {
      throw new Error(
        `Cannot extract Dingo release (${result.signal || result.code}): ${diagnostic.trim()}`,
      );
    }
  } catch (error) {
    child.kill();
    await exited;
    throw error;
  }
  if ((await fsp.stat(destination)).size === 0) throw new Error('Dingo release binary is empty');
}

function binaryPath(packageRoot) {
  return path.join(packageRoot, 'npm', 'vendor', 'dingo');
}

async function install(packageRoot, options = {}) {
  const { version } = JSON.parse(await fsp.readFile(path.join(packageRoot, 'package.json'), 'utf8'));
  const targetRelease = releaseTarget(version, options.platform, options.arch);
  const checksums = options.checksums ?? JSON.parse(
    await fsp.readFile(path.join(packageRoot, 'npm', 'checksums.json'), 'utf8'),
  );
  const expected = checksums[targetRelease.filename];
  if (typeof expected !== 'string' || !/^[0-9a-f]{64}$/.test(expected)) {
    throw new Error(`No valid checksum for ${targetRelease.filename}`);
  }
  const directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'dingo-npm-'));
  let pending;
  try {
    const archive = path.join(directory, 'release.tar.gz');
    await (options.download || download)(targetRelease.url, archive);
    const archiveSize = (await fsp.stat(archive)).size;
    const maxArchiveBytes = options.maxArchiveBytes ?? MAX_ARCHIVE_BYTES;
    if (archiveSize > maxArchiveBytes) {
      throw new Error(`Dingo release archive exceeds ${maxArchiveBytes} bytes`);
    }
    const actual = await sha256(archive);
    if (!crypto.timingSafeEqual(Buffer.from(actual, 'hex'), Buffer.from(expected, 'hex'))) {
      throw new Error(`Checksum mismatch for ${targetRelease.filename}`);
    }
    const target = binaryPath(packageRoot);
    await fsp.mkdir(path.dirname(target), { recursive: true });
    pending = await fsp.mkdtemp(path.join(path.dirname(target), '.install-'));
    const extracted = path.join(pending, 'dingo');
    await extractBinary(archive, extracted, options.maxBinaryBytes);
    await fsp.chmod(extracted, 0o755);
    await fsp.rename(extracted, target);
    return target;
  } finally {
    await fsp.rm(directory, { recursive: true, force: true });
    if (pending) await fsp.rm(pending, { recursive: true, force: true });
  }
}

module.exports = { binaryPath, download, install, releaseURL };
