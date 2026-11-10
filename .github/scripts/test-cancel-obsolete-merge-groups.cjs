const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {test} = require('node:test');

const workflow = readFileSync(join(__dirname, '../workflows/cancel-obsolete-merge-groups.yml'), 'utf8');
const script = workflow.split('          script: |\n')[1]
  .split('\n').map(line => line.replace(/^ {12}/, '')).join('\n');
const execute = new (Object.getPrototypeOf(async function () {}).constructor)('github', 'context', 'core', 'setTimeout', script);
const context = {repo: {owner: 'blinklabs-io', repo: 'dingo'}};
const group = (id, overrides = {}) => ({
  id, event: 'merge_group', head_branch: `gh-readonly-queue/main/pr-${id}-base`, ...overrides,
});
const httpError = status => Object.assign(new Error(`HTTP ${status}`), {status});
const containsHTTPError = status => error => error instanceof AggregateError &&
  error.errors.some(cause => cause.status === status);

function fixture({runs = {queued: [group(1)]}, refs = {}, completed = [], cancelError, forceError,
  completesAfterConflict = false, completesAfterCancellation = false} = {}) {
  const cancelled = [];
  const forced = [];
  const statuses = [];
  const checkedRefs = [];
  const reads = new Map();
  const listWorkflowRunsForRepo = () => {};
  const github = {
    rest: {
      actions: {
        listWorkflowRunsForRepo,
        getWorkflowRun: async args => {
          assert.equal(args.owner, context.repo.owner);
          assert.equal(args.repo, context.repo.repo);
          const count = (reads.get(args.run_id) ?? 0) + 1;
          reads.set(args.run_id, count);
          return {data: {status: completed.includes(args.run_id) ||
            ((completesAfterConflict || completesAfterCancellation) && count > 1)
            ? 'completed' : 'in_progress'}};
        },
      },
      git: {
        getRef: async args => {
          checkedRefs.push(args.ref);
          const result = refs[args.ref] ?? 404;
          if (result !== 200) throw httpError(result);
          return {data: {object: {sha: 'current'}}};
        },
      },
    },
    paginate: async (method, args) => {
      assert.equal(method, listWorkflowRunsForRepo);
      assert.deepEqual(args, {...context.repo, event: 'merge_group', status: args.status, per_page: 100});
      statuses.push(args.status);
      return runs[args.status] ?? [];
    },
    request: async (route, args) => {
      assert.deepEqual(args, {...context.repo, run_id: args.run_id});
      if (route === 'POST /repos/{owner}/{repo}/actions/runs/{run_id}/force-cancel') {
        assert.ok(cancelled.includes(args.run_id), 'normal cancellation must precede force cancellation');
        if (forceError) throw httpError(forceError);
        forced.push(args.run_id);
        return;
      }
      assert.equal(route, 'POST /repos/{owner}/{repo}/actions/runs/{run_id}/cancel');
      if (cancelError) throw httpError(cancelError);
      cancelled.push(args.run_id);
    },
  };
  return {
    cancelled, forced, statuses, checkedRefs,
    run: () => execute(github, context, {info: () => {}, error: () => {}}, (resolve, delay) => {
      assert.equal(delay, 30000);
      resolve();
    }),
  };
}

test('cancels obsolete groups across active statuses without cancelling live groups', async () => {
  const f = fixture({
    runs: {queued: [group(1), group(2)], in_progress: [group(1), group(3)],
      waiting: [group(4)], pending: [group(5)], requested: [group(6)]},
    refs: {'heads/gh-readonly-queue/main/pr-2-base': 200},
  });
  await f.run();
  assert.deepEqual(f.cancelled, [1, 3, 4, 5, 6]);
  assert.deepEqual(f.forced, [1, 3, 4, 5, 6]);
  assert.deepEqual(f.statuses, ['queued', 'in_progress', 'waiting', 'pending', 'requested']);
});

test('keeps other events and branches even if their refs are absent', async () => {
  const f = fixture({runs: {queued: [group(1, {event: 'pull_request'}),
    group(2, {head_branch: 'main'}), group(3, {head_branch: null})]}});
  await f.run();
  assert.deepEqual(f.cancelled, []);
  assert.deepEqual(f.checkedRefs, []);
});

for (const status of [403, 429, 500]) {
  test(`does not treat HTTP ${status} as evidence a group is obsolete`, async () => {
    const f = fixture({refs: {'heads/gh-readonly-queue/main/pr-1-base': status}});
    await assert.rejects(f.run(), containsHTTPError(status));
    assert.deepEqual(f.cancelled, []);
  });
}

test('does not cancel a run that completed during the scan', async () => {
  const f = fixture({completed: [1]});
  await f.run();
  assert.deepEqual(f.cancelled, []);
});

test('reports cancellation failures', async () => {
  const f = fixture({cancelError: 403});
  await assert.rejects(f.run(), containsHTTPError(403));
});

test('reports a cancellation conflict when the run is still active', async () => {
  const f = fixture({cancelError: 409});
  await assert.rejects(f.run(), containsHTTPError(409));
});

test('accepts a cancellation conflict only after confirming the run completed', async () => {
  const f = fixture({cancelError: 409, completesAfterConflict: true});
  await f.run();
  assert.deepEqual(f.cancelled, []);
});

test('cleans other obsolete runs while reporting a ref lookup failure', async () => {
  const f = fixture({runs: {queued: [group(1), group(2)]},
    refs: {'heads/gh-readonly-queue/main/pr-1-base': 500}});
  let failure;
  try { await f.run(); } catch (error) { failure = error; }
  assert.deepEqual(f.cancelled, [2]);
  assert.ok(containsHTTPError(500)(failure));
});

test('preserves normal cancellation cleanup when the run finishes', async () => {
  const f = fixture({completesAfterCancellation: true});
  await f.run();
  assert.deepEqual(f.cancelled, [1]);
  assert.deepEqual(f.forced, []);
});

test('reports a force-cancellation failure after requesting normal cancellation', async () => {
  const f = fixture({forceError: 403});
  await assert.rejects(f.run(), containsHTTPError(403));
  assert.deepEqual(f.cancelled, [1]);
  assert.deepEqual(f.forced, []);
});
