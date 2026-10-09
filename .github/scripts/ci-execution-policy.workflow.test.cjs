// SPDX-License-Identifier: Apache-2.0
'use strict';
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const test = require('node:test');
const workflows = [
  {
    "file": "ci.yml",
    "jobs": [
      "lint",
      "build",
      "test",
      "cross-platform-driver-file",
      "buf",
      "deny",
      "critical-path",
      "header-check",
      "fuzz-lockfile",
      "coverage",
      "stress-smoke",
      "ci-policy-tests"
    ],
    "preserved": []
  }
];
const root = join(__dirname, '..', 'workflows');
const read = name => readFileSync(join(root, name), 'utf8');
const job = (text, name) => {
  const match = text.match(new RegExp('^  ' + name + ':\\n([\\s\\S]*?)(?=^  [a-z]|$(?![\\s\\S]))', 'm'));
  assert.ok(match, `missing job ${name}`);
  return match[1];
};
const condition = text => {
  const match = text.match(/^    if: (.+)(?:\n((?:      .+\n)+))?/m);
  assert.ok(match, 'missing job condition');
  return (match[1] === '>-' ? match[2].trim() : match[1]).replace(/^\$\{\{\s*/, '').replace(/\s*\}\}$/, '');
};
function evaluate(expression, result, validate, cancelled = false) {
  const needs = {
    'ci-policy': {result, outputs:{validate}},
    'workflow-pins': {result:'success', outputs:{'run-ci':'true'}},
    changes: {result:'success', outputs:{code:'true'}},
  };
  const github = {event_name:'push', ref:'refs/heads/main', repository:'prisma-risk/tsoracle',
    event:{pull_request:{head:{repo:{full_name:'prisma-risk/tsoracle'}}}}, head_ref:''};
  expression = expression.replace(/needs\.([a-z][a-z0-9-]*)/g, "needs['$1']")
    .replace(/\.run-ci/g, "['run-ci']");
  return new Function('needs','github','inputs','cancelled','startsWith', 'return Boolean(' + expression + ')')(
    needs, github, {request_id:''}, () => cancelled, (value,prefix) => value.startsWith(prefix));
}
for (const config of workflows) {
  test(`${config.file}: green PR evidence skips duplicates; policy failure validates`, () => {
    const text = read(config.file);
    for (const name of config.jobs) {
      const definition = job(text,name);
      assert.match(definition, /^    needs: .*ci-policy/m, name);
      const expression = condition(definition);
      assert.equal(evaluate(expression,'success','false'),false,name);
      for (const [result,validate] of [['success','true'],['failure','false'],['failure',undefined],['skipped',undefined]]) {
        assert.equal(evaluate(expression,result,validate),true,`${name}: ${result}/${validate}`);
      }
      assert.equal(evaluate(expression,'success','true',true),false,name);
    }
  });
  test(`${config.file}: policy loads only trusted main-push source`, async () => {
    const definition = job(read(config.file),'ci-policy');
    for (const permission of ['contents','actions','pull-requests']) {
      assert.match(definition,new RegExp('^      '+permission+': read$', 'm'));
    }
    assert.match(definition, /if: github.event_name == 'push' && github.ref == 'refs\/heads\/main'/);
    assert.match(definition, /ref: \$\{\{ github.sha \}\}/);
    assert.match(definition, /persist-credentials: false/);
    const script = definition.match(/          script: \|\n((?:            .+\n|\n)+)/)[1]
      .split('\n').map(line => line.replace(/^ {12}/,'')).join('\n');
    const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
    for (const [eventName,ref,expected] of [['push','refs/heads/main','false'],
      ['pull_request','refs/pull/1/merge','true'],['schedule','refs/heads/main','true'],
      ['workflow_dispatch','refs/heads/main','true'],['push','refs/heads/topic','true']]) {
      let calls=0; let output;
      const core={setOutput:(name,value)=>{assert.equal(name,'validate');output=value;},info:()=>{}};
      const require=path=>{assert.equal(path,'/trusted/.github/scripts/ci-execution-policy.cjs');
        calls++;return async ({core})=>core.setOutput('validate','false');};
      await new AsyncFunction('github','context','core','require','process',script)(
        {},{eventName,ref},core,require,{env:{GITHUB_WORKSPACE:'/trusted'}});
      assert.equal(output,expected,eventName);
      assert.equal(calls,expected === 'false' ? 1 : 0,eventName);
    }
  });
  test(`${config.file}: PR, main, daily and manual triggers remain available`, () => {
    const text=read(config.file).split(/^jobs:/m)[0];
    for (const event of ['pull_request','push','schedule','workflow_dispatch']) {
      assert.match(text,new RegExp('^  '+event+':','m'));
    }
  });
  for (const name of config.preserved || []) {
    test(`${config.file}: ${name} retains its independent contract`, () => {
      const definition = job(read(config.file),name);
      assert.doesNotMatch(definition, /needs: .*ci-policy/);
      assert.doesNotMatch(definition, /needs\.ci-policy/);
      if (name === 'recover') {
        assert.match(definition, /needs: \[workflow-pins\]/);
        assert.match(definition, /name: production/);
        const check = new Function('needs','inputs', 'return Boolean(' + condition(definition)
          .replace(/needs\.workflow-pins/g, "needs['workflow-pins']").replace(/\.run-ci/g, "['run-ci']") + ')');
        const needs = {'workflow-pins':{outputs:{'run-ci':'true'}}};
        assert.equal(check(needs,{request_id:'recovery'}),true);
        assert.equal(check(needs,{request_id:''}),false);
        needs['workflow-pins'].outputs['run-ci']='false';
        assert.equal(check(needs,{request_id:'recovery'}),false);
      }
      if (name === 'release-gates') {
        const check = new Function('github', 'return Boolean(' + condition(definition) + ')');
        assert.equal(check({event_name:'workflow_dispatch'}),true);
        assert.equal(check({event_name:'push'}),false);
        assert.equal(check({event_name:'pull_request'}),false);
      }
      if (name === 'buf') {
        assert.doesNotMatch(definition, /^    if:/m);
        assert.match(definition, /github\.event\.pull_request\.base\.sha \|\| github\.event\.before \|\| github\.sha/);
        assert.match(definition, /buf breaking --against/);
      }
    });
  }
}
test('the primary workflow runs both policy regression suites with Node 24', () => {
  const text=job(read("ci.yml"),'ci-policy-tests');
  assert.match(text,/node-version: ["']24["']/);
  assert.match(text,/node --test \.github\/scripts\/ci-execution-policy\.test\.cjs \.github\/scripts\/ci-execution-policy\.workflow\.test\.cjs/);
});
