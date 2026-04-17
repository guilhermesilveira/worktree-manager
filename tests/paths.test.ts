import { afterEach, describe, expect, it } from 'vitest';

import { resolveAppDir, resolveDatabasePath } from '../src/paths.js';
import { makeTempDir, removeTempDir } from './testUtils.js';

const tempDirs: string[] = [];

afterEach(() => {
  delete process.env.WORKTREE_MANAGER_HOME;
  for (const path of tempDirs.splice(0)) {
    removeTempDir(path);
  }
});

describe('paths', () => {
  it('uses the override app dir when WORKTREE_MANAGER_HOME is set', () => {
    const tempDir = makeTempDir('wtm-paths-');
    tempDirs.push(tempDir);
    process.env.WORKTREE_MANAGER_HOME = tempDir;

    expect(resolveAppDir()).toBe(tempDir);
    expect(resolveDatabasePath()).toBe(`${tempDir}/worktree-manager.sqlite`);
  });
});

