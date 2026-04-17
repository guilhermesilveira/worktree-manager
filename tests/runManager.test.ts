import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

import { getRun, getRunWorktree, listRunWorktrees, upsertProject, upsertRepository } from '../src/db.js';
import { createNewTree, promoteRunRepository, purgeTrees, pushRunTree } from '../src/runManager.js';
import { makeTempDir, removeTempDir, initGitRepo, writeFile } from './testUtils.js';

let tempDir = '';

beforeEach(() => {
  tempDir = makeTempDir('wtm-run-');
  process.env.WORKTREE_MANAGER_HOME = join(tempDir, 'app-home');
});

afterEach(() => {
  delete process.env.WORKTREE_MANAGER_HOME;
  removeTempDir(tempDir);
});

function run(command: string, args: string[], cwd: string): void {
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf-8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  if ((result.status ?? 1) !== 0) {
    throw new Error(String(result.stderr || result.stdout || `${command} failed`));
  }
}

describe('runManager', () => {
  it('creates a new run with the primary repository worktree by default', () => {
    const baseDir = join(tempDir, 'runs');
    const repoPath = join(tempDir, 'henon');
    initGitRepo(repoPath, { 'README.md': '# henon\n' });
    upsertProject('henon', baseDir);
    upsertRepository({
      nickname: 'henon',
      name: 'henon',
      localPath: repoPath,
      remoteUrl: '',
      defaultBranch: 'main',
      isPrimary: true,
    });

    const result = createNewTree('henon', []);

    expect(result.worktrees).toHaveLength(1);
    expect(result.worktrees[0]?.repoName).toBe('henon');
    expect(existsSync(result.worktrees[0]!.worktreePath)).toBe(true);
    expect(existsSync(join(result.run.workspaceRoot, 'run.json'))).toBe(true);
  });

  it('promotes an extra repository into an existing run', () => {
    const baseDir = join(tempDir, 'runs');
    const mainRepo = join(tempDir, 'henon');
    const auxRepo = join(tempDir, 'henon-pub02');
    initGitRepo(mainRepo, { 'README.md': '# henon\n' });
    initGitRepo(auxRepo, { 'README.md': '# pub02\n' });
    upsertProject('henon', baseDir);
    upsertRepository({
      nickname: 'henon',
      name: 'henon',
      localPath: mainRepo,
      remoteUrl: '',
      defaultBranch: 'main',
      isPrimary: true,
    });
    upsertRepository({
      nickname: 'henon',
      name: 'henon-pub02',
      localPath: auxRepo,
      remoteUrl: '',
      defaultBranch: 'main',
      isPrimary: false,
    });

    const created = createNewTree('henon', []);
    const promoted = promoteRunRepository(created.run.runId, 'henon-pub02');

    expect(promoted.worktree.repoName).toBe('henon-pub02');
    expect(listRunWorktrees(created.run.runId)).toHaveLength(2);
    expect(existsSync(promoted.worktree.worktreePath)).toBe(true);
  });

  it('purges inactive runs and removes their workspace directories', () => {
    const baseDir = join(tempDir, 'runs');
    const repoPath = join(tempDir, 'henon');
    initGitRepo(repoPath, { 'README.md': '# henon\n' });
    upsertProject('henon', baseDir);
    upsertRepository({
      nickname: 'henon',
      name: 'henon',
      localPath: repoPath,
      remoteUrl: '',
      defaultBranch: 'main',
      isPrimary: true,
    });

    const created = createNewTree('henon', []);
    const purgeBeforeForce = purgeTrees('henon', false);
    expect(purgeBeforeForce.skippedRunIds).toEqual([created.run.runId]);

    const purgeAfterForce = purgeTrees('henon', true);
    expect(purgeAfterForce.purgedRunIds).toEqual([created.run.runId]);
    expect(getRun(created.run.runId)).toBeNull();
    expect(existsSync(created.run.workspaceRoot)).toBe(false);
  });

  it('push-tree commits and pushes to the run branch without updating main', () => {
    const baseDir = join(tempDir, 'runs');
    const barePath = join(tempDir, 'origin.git');
    const seedRepo = join(tempDir, 'seed');
    initGitRepo(seedRepo, { 'README.md': '# henon\n' });
    run('git', ['init', '--bare', barePath], tempDir);
    run('git', ['remote', 'add', 'origin', barePath], seedRepo);
    run('git', ['push', '-u', 'origin', 'main'], seedRepo);
    run('git', ['remote', 'set-head', 'origin', 'main'], seedRepo);

    upsertProject('henon', baseDir);
    upsertRepository({
      nickname: 'henon',
      name: 'henon',
      localPath: seedRepo,
      remoteUrl: barePath,
      defaultBranch: 'main',
      isPrimary: true,
    });

    const created = createNewTree('henon', []);
    const worktree = getRunWorktree(created.run.runId, 'henon');
    writeFile(join(worktree!.worktreePath, 'new.txt'), 'hello\n');
    run('git', ['config', 'user.name', 'Worktree Manager Tests'], worktree!.worktreePath);
    run('git', ['config', 'user.email', 'tests@example.com'], worktree!.worktreePath);

    const pushed = pushRunTree(created.run.runId);

    expect(pushed.run.status).toBe('PUSHED');
    expect(pushed.repositories[0]?.commitCreated).toBe(true);
    expect(pushed.repositories[0]?.targetBranch).toBe(worktree?.branchName);

    const mainClonePath = join(tempDir, 'verify-main-clone');
    run('git', ['clone', '--branch', 'main', barePath, mainClonePath], tempDir);
    expect(existsSync(join(mainClonePath, 'new.txt'))).toBe(false);

    const runClonePath = join(tempDir, 'verify-run-clone');
    run('git', ['clone', '--branch', worktree!.branchName, barePath, runClonePath], tempDir);
    expect(existsSync(join(runClonePath, 'new.txt'))).toBe(true);
  });
});
