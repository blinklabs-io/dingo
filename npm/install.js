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

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const https = require('node:https');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');
const { pipeline } = require('node:stream/promises');

function releaseURL(version, platform = process.platform, arch = process.arch) {
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
  return `https://github.com/blinklabs-io/dingo/releases/download/v${version}/${filename}`;
}

function download(url, destination, get = https.get, redirects = 0) {
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
        download(redirect, destination, get, redirects + 1)
          .then(resolve, reject);
        return;
      }
      if (response.statusCode !== 200) {
        response.resume();
        reject(new Error(`Dingo release download returned HTTP ${response.statusCode}`));
        return;
      }
      pipeline(response, fs.createWriteStream(destination, { flags: 'wx' }))
        .then(resolve, reject);
    });
    request.on('error', reject);
    request.setTimeout(30_000, () => request.destroy(new Error('Dingo release download timed out')));
  });
}

async function extractBinary(archive, destination) {
  // Stream only the shipped member: archive paths never become filesystem paths.
  const child = spawn('tar', ['-xzOf', archive, 'dingo'], { stdio: ['ignore', 'pipe', 'pipe'] });
  let diagnostic = '';
  child.stderr.on('data', (chunk) => { diagnostic = (diagnostic + chunk).slice(-8192); });
  const exited = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`Cannot extract Dingo release (${signal || code}): ${diagnostic.trim()}`));
    });
  });
  const copied = pipeline(child.stdout, fs.createWriteStream(destination, { flags: 'wx', mode: 0o755 }))
    .catch((error) => { child.kill(); throw error; });
  await Promise.all([exited, copied]);
  if ((await fsp.stat(destination)).size === 0) throw new Error('Dingo release binary is empty');
}

function binaryPath(packageRoot) {
  return path.join(packageRoot, 'npm', 'vendor', 'dingo');
}

async function install(packageRoot, options = {}) {
  const { version } = JSON.parse(await fsp.readFile(path.join(packageRoot, 'package.json'), 'utf8'));
  const url = releaseURL(version, options.platform, options.arch);
  const directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'dingo-npm-'));
  let pending;
  try {
    const archive = path.join(directory, 'release.tar.gz');
    await (options.download || download)(url, archive);
    const target = binaryPath(packageRoot);
    await fsp.mkdir(path.dirname(target), { recursive: true });
    pending = await fsp.mkdtemp(path.join(path.dirname(target), '.install-'));
    const extracted = path.join(pending, 'dingo');
    await extractBinary(archive, extracted);
    await fsp.chmod(extracted, 0o755);
    await fsp.rename(extracted, target);
    return target;
  } finally {
    await fsp.rm(directory, { recursive: true, force: true });
    if (pending) await fsp.rm(pending, { recursive: true, force: true });
  }
}

module.exports = { binaryPath, download, install, releaseURL };
