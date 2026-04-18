import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { addRunSlot, addRunWorktree, assignConsumerSlot, createPoolWorktree, createRun, createSlot, getPoolStats, getProject, getSlotStats, inspectPoolWorktree, inspectSlot, listConsumerSlots, listPoolWorktrees, listRepositories, listRunSlots, listSlots, listStaleFreePoolWorktrees, upsertProject, upsertRepository } from '../src/db.js';
import { makeTempDir, removeTempDir } from './testUtils.js';

let tempDir = '';

beforeEach(() => {
  tempDir = makeTempDir('wtm-db-');
  process.env.WORKTREE_MANAGER_HOME = tempDir;
});

afterEach(() => {
  delete process.env.WORKTREE_MANAGER_HOME;
  removeTempDir(tempDir);
});

describe('db', () => {
  it('stores and loads a project', () => {
    const project = upsertProject('henon', '/tmp/runs/henon');

    expect(project.nickname).toBe('henon');
    expect(project.baseDir).toBe('/tmp/runs/henon');
    expect(getProject('henon')?.baseDir).toBe('/tmp/runs/henon');
  });

  it('stores and lists repositories in name order', () => {
    upsertProject('henon', '/tmp/runs/henon');
    upsertRepository({
      nickname: 'henon',
      name: 'zeta',
      localPath: '/tmp/zeta',
      remoteUrl: 'https://example.com/zeta.git',
      defaultBranch: 'main',
      isPrimary: false,
    });
    upsertRepository({
      nickname: 'henon',
      name: 'alpha',
      localPath: '/tmp/alpha',
      remoteUrl: 'https://example.com/alpha.git',
      defaultBranch: 'master',
      isPrimary: true,
    });

    const repositories = listRepositories('henon');
    expect(repositories.map((repository) => repository.name)).toEqual(['alpha', 'zeta']);
    expect(repositories[0]?.defaultBranch).toBe('master');
    expect(repositories[0]?.isPrimary).toBe(true);
  });

  it('updates repository rows in place for the same nickname and name', () => {
    upsertProject('henon', '/tmp/runs/henon');
    const first = upsertRepository({
      nickname: 'henon',
      name: 'henon',
      localPath: '/tmp/henon',
      remoteUrl: '',
      defaultBranch: 'main',
      isPrimary: true,
    });
    const second = upsertRepository({
      nickname: 'henon',
      name: 'henon',
      localPath: '/tmp/henon-next',
      remoteUrl: 'https://example.com/henon.git',
      defaultBranch: 'trunk',
      isPrimary: true,
    });

    expect(second.id).toBe(first.id);
    expect(listRepositories('henon')[0]?.localPath).toBe('/tmp/henon-next');
    expect(listRepositories('henon')[0]?.defaultBranch).toBe('trunk');
  });

  it('stores slot rows plus run and consumer mappings', () => {
    upsertProject('henon', '/tmp/runs/henon');
    createRun({
      runId: 'run-123',
      nickname: 'henon',
      workspaceRoot: '/tmp/runs/henon/run-123',
    });

    const slot = createSlot({
      slotId: 'slot-001',
      nickname: 'henon',
      repoName: 'henon',
      slotPath: '/tmp/runs/henon/run-123/repos/henon',
      poolWorktreeId: 'pool-001',
      state: 'BUSY',
      currentConsumerId: 'agent-7',
    });
    createPoolWorktree({
      poolWorktreeId: 'pool-001',
      nickname: 'henon',
      repoName: 'henon',
      poolPath: '/tmp/pool/henon/pool-001',
      state: 'BUSY',
      currentConsumerId: 'agent-7',
    });
    addRunSlot({
      runId: 'run-123',
      slotId: slot.slotId,
      repoName: 'henon',
      poolWorktreeId: 'pool-001',
    });
    assignConsumerSlot('agent-7', slot.slotId);

    expect(listSlots('henon')).toHaveLength(1);
    expect(listSlots('henon')[0]?.currentConsumerId).toBe('agent-7');
    expect(listSlots('henon')[0]?.poolWorktreeId).toBe('pool-001');
    expect(listRunSlots('run-123').map((runSlot) => runSlot.slotId)).toEqual(['slot-001']);
    expect(listConsumerSlots('agent-7').map((consumerSlot) => consumerSlot.slotId)).toEqual(['slot-001']);
    expect(listPoolWorktrees('henon').map((pool) => pool.poolWorktreeId)).toEqual(['pool-001']);
  });

  it('inspects slots and aggregates slot stats for scheduler reads', () => {
    upsertProject('henon', '/tmp/runs/henon');
    createRun({
      runId: 'run-123',
      nickname: 'henon',
      workspaceRoot: '/tmp/runs/henon/run-123',
    });
    createSlot({
      slotId: 'slot-001',
      nickname: 'henon',
      repoName: 'henon',
      slotPath: '/tmp/runs/henon/run-123/repos/henon',
      poolWorktreeId: 'pool-001',
      state: 'BUSY',
      currentConsumerId: 'agent-7',
    });
    createPoolWorktree({
      poolWorktreeId: 'pool-001',
      nickname: 'henon',
      repoName: 'henon',
      poolPath: '/tmp/pool/henon/pool-001',
      state: 'BUSY',
      currentConsumerId: 'agent-7',
    });
    addRunSlot({
      runId: 'run-123',
      slotId: 'slot-001',
      repoName: 'henon',
      poolWorktreeId: 'pool-001',
    });
    addRunWorktree({
      runId: 'run-123',
      repoName: 'henon',
      worktreePath: '/tmp/runs/henon/run-123/repos/henon',
      branchName: 'wt/run-123-henon',
      poolWorktreeId: 'pool-001',
      isPrimary: true,
    });
    assignConsumerSlot('agent-7', 'slot-001');
    createSlot({
      slotId: 'slot-002',
      nickname: 'henon',
      repoName: 'henon-pub02',
      slotPath: '/tmp/runs/henon/run-123/repos/henon-pub02',
      poolWorktreeId: 'pool-002',
      state: 'FREE',
    });
    createPoolWorktree({
      poolWorktreeId: 'pool-002',
      nickname: 'henon',
      repoName: 'henon-pub02',
      poolPath: '/tmp/pool/henon-pub02/pool-002',
      state: 'FREE',
    });

    const inspection = inspectSlot('slot-001');
    const stats = getSlotStats('henon');

    expect(inspection?.slot.slotId).toBe('slot-001');
    expect(inspection?.slot.poolWorktreeId).toBe('pool-001');
    expect(inspection?.poolWorktree?.poolWorktreeId).toBe('pool-001');
    expect(inspection?.runs[0]?.runId).toBe('run-123');
    expect(inspection?.runs[0]?.poolWorktreeId).toBe('pool-001');
    expect(inspection?.runs[0]?.branchName).toBe('wt/run-123-henon');
    expect(inspection?.consumers[0]?.consumerId).toBe('agent-7');
    expect(stats.total).toBe(2);
    expect(stats.byNickname[0]?.nickname).toBe('henon');
    expect(stats.byNickname[0]?.busy).toBe(1);
    expect(stats.byNickname[0]?.free).toBe(1);
    expect(stats.byRepo.map((entry) => entry.repoName)).toEqual(['henon', 'henon-pub02']);
  });

  it('inspects pooled worktrees and aggregates pool stats', () => {
    upsertProject('henon', '/tmp/runs/henon');
    createRun({
      runId: 'run-123',
      nickname: 'henon',
      workspaceRoot: '/tmp/runs/henon/run-123',
    });
    createPoolWorktree({
      poolWorktreeId: 'pool-001',
      nickname: 'henon',
      repoName: 'henon',
      poolPath: '/tmp/pool/henon/pool-001',
      state: 'BUSY',
      currentConsumerId: 'agent-7',
    });
    createSlot({
      slotId: 'slot-001',
      nickname: 'henon',
      repoName: 'henon',
      slotPath: '/tmp/runs/henon/run-123/repos/henon',
      poolWorktreeId: 'pool-001',
      state: 'BUSY',
      currentConsumerId: 'agent-7',
    });
    addRunSlot({
      runId: 'run-123',
      slotId: 'slot-001',
      repoName: 'henon',
      poolWorktreeId: 'pool-001',
    });
    addRunWorktree({
      runId: 'run-123',
      repoName: 'henon',
      worktreePath: '/tmp/runs/henon/run-123/repos/henon',
      branchName: 'wt/run-123-henon',
      poolWorktreeId: 'pool-001',
      isPrimary: true,
    });
    createPoolWorktree({
      poolWorktreeId: 'pool-002',
      nickname: 'henon',
      repoName: 'henon-pub02',
      poolPath: '/tmp/pool/henon-pub02/pool-002',
      state: 'FREE',
    });

    const inspection = inspectPoolWorktree('pool-001');
    const stats = getPoolStats('henon');
    const stale = listStaleFreePoolWorktrees(new Date(Date.now() + 60_000).toISOString(), 'henon');

    expect(inspection?.poolWorktree.poolWorktreeId).toBe('pool-001');
    expect(inspection?.slots[0]?.slotId).toBe('slot-001');
    expect(inspection?.runs[0]?.runId).toBe('run-123');
    expect(stats.total).toBe(2);
    expect(stats.byNickname[0]?.busy).toBe(1);
    expect(stats.byNickname[0]?.free).toBe(1);
    expect(stale.map((entry) => entry.poolWorktreeId)).toEqual(['pool-002']);
  });
});
