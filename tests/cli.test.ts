import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

import { makeTempDir, removeTempDir, initGitRepo } from './testUtils.js';

let tempDir = '';
let appDir = '';

beforeEach(() => {
  tempDir = makeTempDir('wtm-cli-');
  appDir = join(tempDir, 'app-home');
});

afterEach(() => {
  removeTempDir(tempDir);
});

function runCli(args: string[]) {
  return spawnSync('node', ['--import', 'tsx', 'src/cli.ts', ...args], {
    cwd: process.cwd(),
    encoding: 'utf-8',
    env: {
      ...process.env,
      WORKTREE_MANAGER_HOME: appDir,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

describe('cli', () => {
  it('registers, adds, and lists repositories as json', () => {
    const repoPath = join(tempDir, 'henon');
    initGitRepo(repoPath, { 'README.md': '# henon\n' });

    const register = runCli(['register', 'henon', join(tempDir, 'runs'), '--json']);
    expect(register.status).toBe(0);

    const add = runCli(['add', 'henon', 'henon', repoPath, '--json']);
    expect(add.status).toBe(0);

    const list = runCli(['list', 'henon', '--json']);
    expect(list.status).toBe(0);

    const payload = JSON.parse(String(list.stdout || '')) as {
      project: { nickname: string };
      repositories: Array<{ name: string; localPath: string }>;
    };
    expect(payload.project.nickname).toBe('henon');
    expect(payload.repositories).toHaveLength(1);
    expect(payload.repositories[0]?.name).toBe('henon');
    expect(payload.repositories[0]?.localPath).toBe(repoPath);
  });

  it('fails cleanly when adding a repo for an unknown nickname', () => {
    const repoPath = join(tempDir, 'henon');
    initGitRepo(repoPath, { 'README.md': '# henon\n' });

    const result = runCli(['add', 'missing', 'henon', repoPath]);

    expect(result.status).toBe(1);
    expect(String(result.stderr || '')).toContain('Unknown nickname');
  });

  it('shows slot inspection and stats as json', () => {
    const repoPath = join(tempDir, 'henon');
    initGitRepo(repoPath, { 'README.md': '# henon\n' });

    expect(runCli(['register', 'henon', join(tempDir, 'runs'), '--json']).status).toBe(0);
    expect(runCli(['add', 'henon', 'henon', repoPath, '--primary', '--json']).status).toBe(0);

    const created = runCli(['new-tree', 'henon', '--consumer', 'agent-9', '--json']);
    expect(created.status).toBe(0);
    const createdPayload = JSON.parse(String(created.stdout || '')) as {
      run: { runId: string };
      worktrees: Array<{ repoName: string }>;
    };
    expect(createdPayload.worktrees).toHaveLength(1);

    const slots = runCli(['list-slots', 'henon', '--json']);
    expect(slots.status).toBe(0);
    const slotsPayload = JSON.parse(String(slots.stdout || '')) as {
      slots: Array<{ slotId: string; state: string }>;
    };
    expect(slotsPayload.slots).toHaveLength(1);

    const inspect = runCli(['inspect-slot', slotsPayload.slots[0]!.slotId, '--json']);
    expect(inspect.status).toBe(0);
    const inspectPayload = JSON.parse(String(inspect.stdout || '')) as {
      inspection: {
        slot: { slotId: string; currentConsumerId: string | null };
        runs: Array<{ runId: string }>;
      };
    };
    expect(inspectPayload.inspection.slot.currentConsumerId).toBe('agent-9');
    expect(inspectPayload.inspection.runs[0]?.runId).toBe(createdPayload.run.runId);

    const stats = runCli(['slot-stats', 'henon', '--json']);
    expect(stats.status).toBe(0);
    const statsPayload = JSON.parse(String(stats.stdout || '')) as {
      stats: {
        total: number;
        byState: Array<{ state: string; total: number }>;
      };
    };
    expect(statsPayload.stats.total).toBe(1);
    expect(statsPayload.stats.byState[0]?.state).toBe('BUSY');
  });

  it('shows pool inspection and pool stats as json', () => {
    const repoPath = join(tempDir, 'henon');
    initGitRepo(repoPath, { 'README.md': '# henon\n' });

    expect(runCli(['register', 'henon', join(tempDir, 'runs'), '--json']).status).toBe(0);
    expect(runCli(['add', 'henon', 'henon', repoPath, '--primary', '--json']).status).toBe(0);
    expect(runCli(['new-tree', 'henon', '--consumer', 'agent-10', '--json']).status).toBe(0);

    const pools = runCli(['list-pool-worktrees', 'henon', '--json']);
    expect(pools.status).toBe(0);
    const poolsPayload = JSON.parse(String(pools.stdout || '')) as {
      poolWorktrees: Array<{ poolWorktreeId: string; state: string }>;
    };
    expect(poolsPayload.poolWorktrees).toHaveLength(1);

    const inspect = runCli(['inspect-pool-worktree', poolsPayload.poolWorktrees[0]!.poolWorktreeId, '--json']);
    expect(inspect.status).toBe(0);
    const inspectPayload = JSON.parse(String(inspect.stdout || '')) as {
      inspection: {
        poolWorktree: { currentConsumerId: string | null };
        slots: Array<{ slotId: string }>;
      };
    };
    expect(inspectPayload.inspection.poolWorktree.currentConsumerId).toBe('agent-10');
    expect(inspectPayload.inspection.slots).toHaveLength(1);

    const stats = runCli(['pool-stats', 'henon', '--json']);
    expect(stats.status).toBe(0);
    const statsPayload = JSON.parse(String(stats.stdout || '')) as {
      stats: { total: number; byState: Array<{ state: string; total: number }> };
    };
    expect(statsPayload.stats.total).toBe(1);
    expect(statsPayload.stats.byState[0]?.state).toBe('BUSY');
  });

  it('shows run inspection, run summaries, and db-backed notes as json', () => {
    const mainRepoPath = join(tempDir, 'henon');
    const secondaryRepoPath = join(tempDir, 'henon-pub02');
    initGitRepo(mainRepoPath, { 'README.md': '# henon\n' });
    initGitRepo(secondaryRepoPath, { 'README.md': '# henon-pub02\n' });

    expect(runCli(['register', 'henon', join(tempDir, 'runs'), '--json']).status).toBe(0);
    expect(runCli(['add', 'henon', 'henon', mainRepoPath, '--primary', '--json']).status).toBe(0);
    expect(runCli(['add', 'henon', 'henon-pub02', secondaryRepoPath, '--json']).status).toBe(0);

    const created = runCli(['new-tree', 'henon', '--consumer', 'agent-11', '--json']);
    expect(created.status).toBe(0);
    const createdPayload = JSON.parse(String(created.stdout || '')) as {
      run: { runId: string };
    };
    expect(runCli(['promote', createdPayload.run.runId, 'henon-pub02', '--consumer', 'agent-11', '--json']).status).toBe(0);
    expect(runCli(['note-run', createdPayload.run.runId, 'RUN_RETAINED', 'Kept for manual merge recovery', '--level', 'WARN', '--payload-json', '{"repoName":"henon-pub02"}', '--json']).status).toBe(0);

    const listRuns = runCli(['list-runs', 'henon', '--json']);
    expect(listRuns.status).toBe(0);
    const listRunsPayload = JSON.parse(String(listRuns.stdout || '')) as {
      runs: Array<{ runId: string; repoCount: number; latestEventType: string | null; latestEventLevel: string | null }>;
    };
    expect(listRunsPayload.runs).toHaveLength(1);
    expect(listRunsPayload.runs[0]?.runId).toBe(createdPayload.run.runId);
    expect(listRunsPayload.runs[0]?.repoCount).toBe(2);
    expect(listRunsPayload.runs[0]?.latestEventType).toBe('RUN_RETAINED');
    expect(listRunsPayload.runs[0]?.latestEventLevel).toBe('WARN');

    const inspectRun = runCli(['inspect-run', createdPayload.run.runId, '--json']);
    expect(inspectRun.status).toBe(0);
    const inspectPayload = JSON.parse(String(inspectRun.stdout || '')) as {
      inspection: {
        worktrees: Array<{ repoName: string; slot: { slotId: string } | null; poolWorktree: { poolWorktreeId: string } | null }>;
        events: Array<{ eventType: string; level: string; message: string; payloadJson: string | null }>;
      };
    };
    expect(inspectPayload.inspection.worktrees).toHaveLength(2);
    expect(inspectPayload.inspection.worktrees.every((worktree) => worktree.slot && worktree.poolWorktree)).toBe(true);
    expect(inspectPayload.inspection.events.at(-1)?.eventType).toBe('RUN_RETAINED');
    expect(inspectPayload.inspection.events.at(-1)?.level).toBe('WARN');
    expect(inspectPayload.inspection.events.at(-1)?.payloadJson).toBe('{"repoName":"henon-pub02"}');
  });
});
