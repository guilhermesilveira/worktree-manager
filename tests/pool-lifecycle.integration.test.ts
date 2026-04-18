import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { existsSync, lstatSync, realpathSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { initGitRepo, makeTempDir, removeTempDir } from './testUtils.js';

let tempDir = '';
let appDir = '';
const repoRoot = fileURLToPath(new URL('..', import.meta.url));

beforeEach(() => {
  tempDir = makeTempDir('wtm-pool-integration-');
  appDir = join(tempDir, 'app-home');
});

afterEach(() => {
  removeTempDir(tempDir);
});

function run(command: string, args: string[], cwd: string): string {
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf-8',
    stdio: ['ignore', 'pipe', 'pipe'],
    env: {
      ...process.env,
      WORKTREE_MANAGER_HOME: appDir,
    },
  });
  if ((result.status ?? 1) !== 0) {
    throw new Error(String(result.stderr || result.stdout || `${command} failed`));
  }
  return String(result.stdout || '').trim();
}

function runCli(args: string[], cwd = tempDir): string {
  return run('node', [join(repoRoot, 'dist', 'cli.js'), ...args], cwd);
}

describe('integration-test pooled lifecycle flow', () => {
  it('allocates new pooled worktrees when busy, then reuses released ones later', () => {
    const mainRepo = join(tempDir, 'henon');
    const pubRepo = join(tempDir, 'henon-pub02');
    initGitRepo(mainRepo, { 'README.md': '# henon\n' }, 'main');
    initGitRepo(pubRepo, { 'README.md': '# pub02\n' }, 'main');

    expect(JSON.parse(runCli(['register', 'demo', join(tempDir, 'runs'), '--json'])).project.nickname).toBe('demo');
    expect(JSON.parse(runCli(['add', 'demo', 'henon', mainRepo, '--primary', '--json'])).repository.isPrimary).toBe(true);
    expect(JSON.parse(runCli(['add', 'demo', 'henon-pub02', pubRepo, '--json'])).repository.name).toBe('henon-pub02');

    const firstRun = JSON.parse(runCli(['new-tree', 'demo', '--consumer', 'agent-1', '--json'])) as {
      run: { runId: string; workspaceRoot: string };
      worktrees: Array<{ worktreePath: string }>;
    };
    const firstMainPath = firstRun.worktrees[0]!.worktreePath;
    const firstMainPoolPath = realpathSync(firstMainPath);
    expect(lstatSync(firstMainPath).isSymbolicLink()).toBe(true);

    const secondRun = JSON.parse(runCli(['new-tree', 'demo', '--consumer', 'agent-2', '--json'])) as {
      run: { runId: string };
      worktrees: Array<{ worktreePath: string }>;
    };
    const secondMainPoolPath = realpathSync(secondRun.worktrees[0]!.worktreePath);
    expect(secondMainPoolPath).not.toBe(firstMainPoolPath);

    const poolsWhileBusy = JSON.parse(runCli(['list-pool-worktrees', 'demo', '--json'])) as {
      poolWorktrees: Array<{ repoName: string; state: string }>;
    };
    expect(poolsWhileBusy.poolWorktrees.filter((entry) => entry.repoName === 'henon')).toHaveLength(2);
    expect(poolsWhileBusy.poolWorktrees.every((entry) => entry.state === 'BUSY')).toBe(true);

    const promoted = JSON.parse(runCli(['promote', firstRun.run.runId, 'henon-pub02', '--consumer', 'agent-1', '--json'])) as {
      worktree: { worktreePath: string };
    };
    expect(existsSync(promoted.worktree.worktreePath)).toBe(true);
    expect(lstatSync(promoted.worktree.worktreePath).isSymbolicLink()).toBe(true);

    const released = JSON.parse(runCli(['release-tree', firstRun.run.runId, '--cleanup', '--json'])) as {
      slots: Array<{ state: string }>;
    };
    expect(released.slots.every((entry) => entry.state === 'FREE')).toBe(true);

    const poolStatsAfterRelease = JSON.parse(runCli(['pool-stats', 'demo', '--json'])) as {
      stats: { byRepo: Array<{ repoName: string; free: number; busy: number }> };
    };
    expect(poolStatsAfterRelease.stats.byRepo.find((entry) => entry.repoName === 'henon')?.free).toBe(1);
    expect(poolStatsAfterRelease.stats.byRepo.find((entry) => entry.repoName === 'henon')?.busy).toBe(1);
    expect(poolStatsAfterRelease.stats.byRepo.find((entry) => entry.repoName === 'henon-pub02')?.free).toBe(1);

    const thirdRun = JSON.parse(runCli(['new-tree', 'demo', '--consumer', 'agent-3', '--json'])) as {
      run: { runId: string };
      worktrees: Array<{ worktreePath: string }>;
    };
    expect(realpathSync(thirdRun.worktrees[0]!.worktreePath)).toBe(firstMainPoolPath);

    const inspectedPools = JSON.parse(runCli(['list-pool-worktrees', 'demo', '--json'])) as {
      poolWorktrees: Array<{ poolWorktreeId: string; repoName: string; state: string; poolPath: string; currentConsumerId: string | null }>;
    };
    const reusedMainPool = inspectedPools.poolWorktrees.find((entry) => entry.repoName === 'henon' && entry.currentConsumerId === 'agent-3');
    expect(reusedMainPool).toBeTruthy();

    const inspectPool = JSON.parse(runCli(['inspect-pool-worktree', reusedMainPool!.poolWorktreeId, '--json'])) as {
      inspection: {
        poolWorktree: { repoName: string };
        runs: Array<{ runId: string }>;
      };
    };
    expect(inspectPool.inspection.poolWorktree.repoName).toBe('henon');
    expect(inspectPool.inspection.runs.some((entry) => entry.runId === firstRun.run.runId)).toBe(true);
    expect(inspectPool.inspection.runs.some((entry) => entry.runId === thirdRun.run.runId)).toBe(true);

    expect(JSON.parse(runCli(['release-tree', secondRun.run.runId, '--cleanup', '--json'])).slots).toHaveLength(1);
    expect(JSON.parse(runCli(['release-tree', thirdRun.run.runId, '--cleanup', '--json'])).slots).toHaveLength(1);

    const gcResult = JSON.parse(runCli(['gc-pool', 'demo', '--older-than-hours', '0', '--json'])) as {
      removed: Array<{ poolWorktreeId: string }>;
      skipped: string[];
    };
    expect(gcResult.skipped).toEqual([]);
    expect(gcResult.removed).toHaveLength(3);

    const poolsAfterGc = JSON.parse(runCli(['list-pool-worktrees', 'demo', '--json'])) as {
      poolWorktrees: Array<unknown>;
    };
    expect(poolsAfterGc.poolWorktrees).toHaveLength(0);
  });
});
