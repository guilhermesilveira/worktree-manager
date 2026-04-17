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
  return spawnSync('node', ['dist/cli.js', ...args], {
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
});
