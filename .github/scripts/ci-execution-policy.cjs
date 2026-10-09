// SPDX-License-Identifier: Apache-2.0
'use strict';

// Reuse this workflow's latest successful validation of the merged PR source.
const ciExecutionPolicy = async function ciExecutionPolicy({ github, context, core }) {
  let validate = true;
  let evidence;
  if (context.eventName === 'push' && context.ref === 'refs/heads/main') {
    try {
      const prs = await github.paginate(github.rest.repos.listPullRequestsAssociatedWithCommit, {
        ...context.repo, commit_sha: context.sha, per_page: 100,
      });
      // Fork PRs can pass with privileged proof jobs skipped; validate their merged source.
      const pr = prs.find(pr => pr.merged_at && pr.merge_commit_sha === context.sha &&
        pr.base.ref === 'main' && pr.base.repo.full_name === `${context.repo.owner}/${context.repo.repo}` &&
        pr.head.repo?.full_name === `${context.repo.owner}/${context.repo.repo}`);
      if (pr) {
        const { data: current } = await github.rest.actions.getWorkflowRun({
          ...context.repo, run_id: context.runId,
        });
        const { data } = await github.rest.actions.listWorkflowRuns({
          ...context.repo, workflow_id: current.workflow_id,
          head_sha: pr.head.sha, event: 'pull_request', per_page: 100,
        });
        // Use the latest matching run, not an older green run hidden by a
        // newer failed/pending rerun. Bind it to this PR and exact source SHA.
        const run = data.workflow_runs.find(run => run.event === 'pull_request' &&
          run.head_sha === pr.head.sha && run.head_branch === pr.head.ref &&
          (!run.pull_requests?.length || run.pull_requests.some(item => item.number === pr.number)));
        // GitHub clears pull_requests on many completed merged-PR runs. The
        // exact source SHA and branch remain bound to the merged PR above.
        if (run?.status === 'completed' && run.conclusion === 'success') {
          validate = false;
          evidence = { pr: pr.number, sha: pr.head.sha, url: run.html_url };
        }
      }
    } catch (error) {
      core.warning(`PR validation evidence unavailable (${error.status || 'API error'}); running normal validation`);
    }
  }
  core.setOutput('validate', String(validate));
  if (evidence) {
    core.info(`Reuse PR #${evidence.pr} validation at ${evidence.sha}: ${evidence.url}`);
    await core.summary.addHeading('PR validation reused').addRaw(
      `PR #${evidence.pr} passed this workflow at ${evidence.sha}: ${evidence.url}. ` +
      'Repeated main-push validation is skipped; the daily main run tests the merged branch.\n',
    ).write();
  } else {
    core.info('Run normal validation (PR, daily health check, manual run, or missing green merged-PR evidence)');
  }
};

module.exports = ciExecutionPolicy;
