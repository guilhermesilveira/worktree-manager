import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { getProject, listRepositories, upsertProject, upsertRepository } from '../src/db.js';
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
});
