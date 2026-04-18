import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { existsSync, lstatSync, readFileSync, realpathSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { initGitRepo, makeTempDir, removeTempDir, writeFile } from './testUtils.js';

let tempDir = '';
let appDir = '';
const repoRoot = fileURLToPath(new URL('..', import.meta.url));

beforeEach(() => {
  tempDir = makeTempDir('wtm-integration-');
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

describe('integration-test CLI flow', () => {
  it('runs register -> add -> new-tree -> push-tree and then reuses the pooled worktree after purge', () => {
    const barePath = join(tempDir, 'origin.git');
    const seedRepo = join(tempDir, 'seed');
    const verifyMain = join(tempDir, 'verify-main');
    const verifyRun = join(tempDir, 'verify-run');

    initGitRepo(seedRepo, { 'README.md': '# integration test\n' }, 'main');
    run('git', ['init', '--bare', barePath], tempDir);
    run('git', ['remote', 'add', 'origin', barePath], seedRepo);
    run('git', ['push', '-u', 'origin', 'main'], seedRepo);
    run('git', ['remote', 'set-head', 'origin', 'main'], seedRepo);

    const registerJson = JSON.parse(runCli(['register', 'demo', join(tempDir, 'runs'), '--json'])) as {
      project: { nickname: string; baseDir: string };
    };
    expect(registerJson.project.nickname).toBe('demo');

    const addJson = JSON.parse(runCli(['add', 'demo', 'main', seedRepo, '--primary', '--json'])) as {
      repository: { isPrimary: boolean; defaultBranch: string };
    };
    expect(addJson.repository.isPrimary).toBe(true);
    expect(addJson.repository.defaultBranch).toBe('main');

    const runJson = JSON.parse(runCli(['new-tree', 'demo', '--json'])) as {
      run: { runId: string; workspaceRoot: string };
      worktrees: Array<{ worktreePath: string; branchName: string }>;
    };
    expect(runJson.worktrees).toHaveLength(1);
    expect(runJson.worktrees[0]!.worktreePath).toBe(join(runJson.run.workspaceRoot, 'repos', 'main'));
    expect(lstatSync(runJson.worktrees[0]!.worktreePath).isSymbolicLink()).toBe(true);

    const worktreePath = runJson.worktrees[0]!.worktreePath;
    const firstPoolPath = realpathSync(worktreePath);
    const branchName = runJson.worktrees[0]!.branchName;
    writeFile(join(worktreePath, 'note.txt'), 'hello from integration test\n');
    run('git', ['config', 'user.name', 'Integration Tests'], worktreePath);
    run('git', ['config', 'user.email', 'integration@example.com'], worktreePath);

    const pushJson = JSON.parse(runCli(['push-tree', runJson.run.runId, '--json'])) as {
      run: { status: string };
      repositories: Array<{ targetBranch: string; pushed: boolean; commitCreated: boolean }>;
    };
    expect(pushJson.run.status).toBe('PUSHED');
    expect(pushJson.repositories[0]!.targetBranch).toBe(branchName);
    expect(pushJson.repositories[0]!.pushed).toBe(true);
    expect(pushJson.repositories[0]!.commitCreated).toBe(true);

    run('git', ['clone', '--branch', 'main', barePath, verifyMain], tempDir);
    expect(existsSync(join(verifyMain, 'note.txt'))).toBe(false);
    expect(readFileSync(join(verifyMain, 'README.md'), 'utf-8')).toContain('# integration test');

    run('git', ['clone', '--branch', branchName, barePath, verifyRun], tempDir);
    expect(existsSync(join(verifyRun, 'note.txt'))).toBe(true);
    expect(readFileSync(join(verifyRun, 'note.txt'), 'utf-8')).toContain('integration test');

    const purgeJson = JSON.parse(runCli(['purge-tree', 'demo', '--force', '--json'])) as {
      purgedRunIds: string[];
      skippedRunIds: string[];
    };
    expect(purgeJson.purgedRunIds).toEqual([runJson.run.runId]);
    expect(purgeJson.skippedRunIds).toEqual([]);

    const slotsJson = JSON.parse(runCli(['list-slots', 'demo', '--json'])) as {
      slots: Array<{ slotPath: string; state: string }>;
    };
    expect(slotsJson.slots).toHaveLength(0);

    const secondRunJson = JSON.parse(runCli(['new-tree', 'demo', '--json'])) as {
      worktrees: Array<{ worktreePath: string }>;
    };
    expect(realpathSync(secondRunJson.worktrees[0]!.worktreePath)).toBe(firstPoolPath);
  });
});
