import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { existsSync, lstatSync, mkdirSync, realpathSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

import { createRun, getRun, getRunWorktree, getSlot, listConsumerSlots, listPoolWorktrees, listRuns, listRunSlots, listRunWorktrees, listSlots, updateRunStatus, upsertProject, upsertRepository } from '../src/db.js';
import { cleanupConsumerRuns, cleanupConsumerSlots, cleanupOneSlot, createNewTree, gcPoolWorktrees, promoteRunRepository, purgeFailedTrees, purgeTrees, pushRunTree, releaseRunTree } from '../src/runManager.js';
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
    expect(result.worktrees[0]!.worktreePath).toBe(join(result.run.workspaceRoot, 'repos', 'henon'));
    expect(lstatSync(result.worktrees[0]!.worktreePath).isSymbolicLink()).toBe(true);
    expect(listRunSlots(result.run.runId)).toHaveLength(1);
    expect(listSlots('henon')).toHaveLength(1);
    expect(listPoolWorktrees('henon')).toHaveLength(1);
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
    expect(listRunSlots(created.run.runId)).toHaveLength(2);
    expect(existsSync(promoted.worktree.worktreePath)).toBe(true);
  });

  it('records consumer-to-slot mappings when a consumer id is provided', () => {
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

    const result = createNewTree('henon', [], 'agent-17');

    expect(listConsumerSlots('agent-17')).toHaveLength(1);
    expect(listSlots('henon')[0]?.currentConsumerId).toBe('agent-17');
    expect(listRunSlots(result.run.runId)[0]?.repoName).toBe('henon');
  });

  it('reuses a cleaned free slot for a later run of the same repository', () => {
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

    const first = createNewTree('henon', [], 'agent-1');
    const firstPoolPath = realpathSync(first.worktrees[0]!.worktreePath);

    const released = releaseRunTree(first.run.runId, true);
    expect(released.slots[0]?.state).toBe('FREE');

    const second = createNewTree('henon', [], 'agent-2');

    expect(realpathSync(second.worktrees[0]!.worktreePath)).toBe(firstPoolPath);
    expect(listSlots('henon')).toHaveLength(2);
    expect(listPoolWorktrees('henon')).toHaveLength(1);
    expect(listConsumerSlots('agent-2')).toHaveLength(1);
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
    expect(listSlots('henon')).toHaveLength(0);
    expect(listPoolWorktrees('henon')).toHaveLength(1);
    expect(listPoolWorktrees('henon')[0]?.state).toBe('FREE');
    expect(existsSync(created.run.workspaceRoot)).toBe(false);
  });

  it('purges only failed runs and leaves other runs alone', () => {
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

    const active = createNewTree('henon', []);
    const failedRoot = join(baseDir, 'run-failed-manual');
    mkdirSync(failedRoot, { recursive: true });
    const failed = createRun({ runId: 'run-failed-manual', nickname: 'henon', workspaceRoot: failedRoot });
    updateRunStatus(failed.runId, 'FAILED');

    const purged = purgeFailedTrees('henon');

    expect(purged.purgedRunIds).toEqual([failed.runId]);
    expect(purged.skippedRunIds).toEqual([active.run.runId]);
    expect(getRun(failed.runId)).toBeNull();
    expect(getRun(active.run.runId)?.status).toBe('ACTIVE');
    expect(existsSync(failedRoot)).toBe(false);
    expect(existsSync(active.run.workspaceRoot)).toBe(true);
  });

  it('removes failed allocation run records before returning the error', () => {
    const baseDir = join(tempDir, 'runs');
    const repoPath = join(tempDir, 'missing-repo');
    upsertProject('henon', baseDir);
    upsertRepository({
      nickname: 'henon',
      name: 'henon',
      localPath: repoPath,
      remoteUrl: '',
      defaultBranch: 'main',
      isPrimary: true,
    });

    expect(() => createNewTree('henon', [])).toThrow();

    expect(listRuns('henon')).toHaveLength(0);
    expect(listPoolWorktrees('henon')).toHaveLength(0);
    expect(listSlots('henon')).toHaveLength(0);
    expect(purgeFailedTrees('henon').purgedRunIds).toEqual([]);
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

  it('cleanup-consumer resets assigned slots back to free', () => {
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

    const created = createNewTree('henon', [], 'agent-55');
    writeFile(join(created.worktrees[0]!.worktreePath, 'temp.txt'), 'leftover\n');

    const cleaned = cleanupConsumerSlots('agent-55');

    expect(cleaned.slots).toHaveLength(1);
    expect(cleaned.slots[0]?.state).toBe('FREE');
    expect(listConsumerSlots('agent-55')).toHaveLength(0);
  });

  it('cleanup-consumer safe mode releases only remotely represented clean runs', () => {
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

    const cleanRun = createNewTree('henon', [], 'agent-safe');
    const dirtyRun = createNewTree('henon', [], 'agent-safe');
    writeFile(join(dirtyRun.worktrees[0]!.worktreePath, 'temp.txt'), 'leftover\n');

    const cleaned = cleanupConsumerRuns('agent-safe', true);

    expect(cleaned.safe).toBe(true);
    expect(cleaned.released.map((entry) => entry.runId)).toEqual([cleanRun.run.runId]);
    expect(cleaned.skipped).toHaveLength(1);
    expect(cleaned.skipped[0]?.runId).toBe(dirtyRun.run.runId);
    expect(cleaned.skipped[0]?.reason).toContain('uncommitted changes');
    expect(getRun(cleanRun.run.runId)?.status).toBe('RELEASED');
    expect(getRun(dirtyRun.run.runId)?.status).toBe('ACTIVE');
    expect(listConsumerSlots('agent-safe')).toHaveLength(1);
  });

  it('cleanup-slot marks a slot free when reset succeeds', () => {
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
    writeFile(join(created.worktrees[0]!.worktreePath, 'temp.txt'), 'leftover\n');

    const slotId = listRunSlots(created.run.runId)[0]!.slotId;
    const cleaned = cleanupOneSlot(slotId);

    expect(cleaned.state).toBe('FREE');
    expect(getSlot(slotId)?.poolWorktreeId).toBe(created.worktrees[0]!.poolWorktreeId);
  });

  it('gc-pool removes stale free pooled worktrees while keeping newer ones', () => {
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
    const poolPath = realpathSync(created.worktrees[0]!.worktreePath);
    releaseRunTree(created.run.runId, true);

    const removed = gcPoolWorktrees(0, 'henon');

    expect(removed.removed).toHaveLength(1);
    expect(removed.skipped).toEqual([]);
    expect(listPoolWorktrees('henon')).toHaveLength(0);
    expect(existsSync(poolPath)).toBe(false);
  });
});
