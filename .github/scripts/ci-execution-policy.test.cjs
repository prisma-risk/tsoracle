// SPDX-License-Identifier: Apache-2.0
'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const policy = require('./ci-execution-policy.cjs');

function fixture(workflowId = 42) {
  const repo = { owner: 'prisma-risk', repo: 'tsoracle' };
  const context = { repo, sha: 'a'.repeat(40), runId: 100, eventName: 'push', ref: 'refs/heads/main' };
  const pr = {
    number: 529, merged_at: '2026-10-08T23:57:43Z', merge_commit_sha: context.sha,
    base: { ref: 'main', repo: { full_name: 'prisma-risk/tsoracle' } },
    head: { sha: 'b'.repeat(40), ref: 'topic', repo: {full_name: 'prisma-risk/tsoracle'} },
  };
  const run = {
    id: 101, event: 'pull_request', head_sha: pr.head.sha, head_branch: pr.head.ref,
    pull_requests: [{ number: pr.number }], status: 'completed', conclusion: 'success',
    html_url: 'https://github.com/prisma-risk/tsoracle/actions/runs/101',
  };
  const state = { prs: [pr], runs: [run], fail: undefined, calls: [], output: {}, warnings: [], summaries: [] };
  const listPRs = Symbol('list PRs');
  const github = {
    paginate: async (method, args) => {
      assert.equal(method, listPRs);
      assert.deepEqual(args, { ...repo, commit_sha: context.sha, per_page: 100 });
      state.calls.push('prs');
      if (state.fail === 'prs') throw { status: 403 };
      return state.prs;
    },
    rest: {
      repos: { listPullRequestsAssociatedWithCommit: listPRs },
      actions: {
        getWorkflowRun: async args => {
          assert.deepEqual(args, { ...repo, run_id: context.runId });
          state.calls.push('current');
          if (state.fail === 'current') throw { status: 404 };
          return { data: { workflow_id: workflowId } };
        },
        listWorkflowRuns: async args => {
          assert.deepEqual(args, { ...repo, workflow_id: workflowId, head_sha: pr.head.sha,
            event: 'pull_request', per_page: 100 });
          state.calls.push('runs');
          if (state.fail === 'runs') throw { status: 503 };
          return { data: { workflow_runs: state.runs } };
        },
      },
    },
  };
  const summary = {
    addHeading: () => summary,
    addRaw: text => { state.summaries.push(text); return summary; },
    write: async () => {},
  };
  const core = { setOutput: (name, value) => { state.output[name] = value; },
    info: () => {}, warning: text => state.warnings.push(text), summary };
  return { github, context, core, state, pr, run };
}

for (const id of [42, 43]) {
  test(`reuse binds to the current workflow ${id}`, async () => {
    const f = fixture(id);
    await policy(f);
    assert.equal(f.state.output.validate, 'false');
    assert.match(f.state.summaries[0], /PR #529/);
    assert.match(f.state.summaries[0], new RegExp(f.pr.head.sha));
    assert.deepEqual(f.state.calls, ['prs', 'current', 'runs']);
  });
}

test('PR, scheduled, manual and non-main events always validate without API reads', async () => {
  for (const [eventName, ref] of [['pull_request', 'refs/pull/529/merge'], ['schedule', 'refs/heads/main'],
    ['workflow_dispatch', 'refs/heads/main'], ['push', 'refs/heads/topic'], ['push', 'refs/tags/v1']]) {
    const f = fixture();
    Object.assign(f.context, { eventName, ref });
    await policy(f);
    assert.equal(f.state.output.validate, 'true');
    assert.deepEqual(f.state.calls, []);
  }
});

test('missing or foreign merged PR identity retains validation', async () => {
  for (const change of ['missing', 'open', 'sha', 'base', 'repo', 'fork', 'missing-origin']) {
    const f = fixture();
    if (change === 'missing') f.state.prs = [];
    if (change === 'open') f.pr.merged_at = null;
    if (change === 'sha') f.pr.merge_commit_sha = 'c'.repeat(40);
    if (change === 'base') f.pr.base.ref = 'other';
    if (change === 'repo') f.pr.base.repo.full_name = 'foreign/repo';
    if (change === 'fork') f.pr.head.repo.full_name = 'foreign/repo';
    if (change === 'missing-origin') delete f.pr.head.repo;
    await policy(f);
    assert.equal(f.state.output.validate, 'true', change);
    assert.deepEqual(f.state.calls, ['prs']);
  }
});

test('unexecuted, unsuccessful or mismatched PR runs retain validation', async () => {
  for (const change of ['missing', 'pending', 'failed', 'cancelled', 'event', 'sha', 'branch', 'pr']) {
    const f = fixture();
    if (change === 'missing') f.state.runs = [];
    if (change === 'pending') f.run.status = 'in_progress';
    if (change === 'failed') f.run.conclusion = 'failure';
    if (change === 'cancelled') f.run.conclusion = 'cancelled';
    if (change === 'event') f.run.event = 'push';
    if (change === 'sha') f.run.head_sha = 'c'.repeat(40);
    if (change === 'branch') f.run.head_branch = 'other';
    if (change === 'pr') f.run.pull_requests = [{ number: 530 }];
    await policy(f);
    assert.equal(f.state.output.validate, 'true', change);
    assert.deepEqual(f.state.summaries, []);
  }
});

test('a newer failed or pending run prevents reuse of an older green run', async () => {
  for (const status of ['failure', 'in_progress']) {
    const f = fixture();
    f.state.runs = [{ ...f.run, id: 102, status: status === 'in_progress' ? status : 'completed', conclusion: status }, f.run];
    await policy(f);
    assert.equal(f.state.output.validate, 'true');
  }
});

test('completed merged-PR runs may have their PR associations cleared', async () => {
  const f = fixture();
  f.run.pull_requests = [];
  await policy(f);
  assert.equal(f.state.output.validate, 'false');
});

test('API failures request validation and never claim reused evidence', async () => {
  for (const endpoint of ['prs', 'current', 'runs']) {
    const f = fixture();
    f.state.fail = endpoint;
    await policy(f);
    assert.equal(f.state.output.validate, 'true');
    assert.equal(f.state.warnings.length, 1);
    assert.deepEqual(f.state.summaries, []);
  }
});
