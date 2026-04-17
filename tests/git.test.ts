import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { join } from 'node:path';
import { mkdirSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

import { assertGitRepo, detectDefaultBranch, detectRemoteUrl } from '../src/git.js';
import { makeTempDir, removeTempDir, initGitRepo } from './testUtils.js';

let tempDir = '';

beforeEach(() => {
  tempDir = makeTempDir('wtm-git-');
});

afterEach(() => {
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

describe('git helpers', () => {
  it('rejects non-git directories', () => {
    const repoPath = join(tempDir, 'not-git');
    mkdirSync(repoPath, { recursive: true });
    expect(() => assertGitRepo(repoPath)).toThrow(/Not a git repository/);
  });

  it('detects the default branch and remote url', () => {
    const repoPath = join(tempDir, 'repo');
    const barePath = join(tempDir, 'origin.git');
    initGitRepo(repoPath, { 'README.md': '# hello\n' }, 'main');
    run('git', ['init', '--bare', barePath], tempDir);
    run('git', ['remote', 'add', 'origin', barePath], repoPath);
    run('git', ['push', '-u', 'origin', 'main'], repoPath);
    run('git', ['remote', 'set-head', 'origin', 'main'], repoPath);

    expect(detectDefaultBranch(repoPath)).toBe('main');
    expect(detectRemoteUrl(repoPath)).toBe(barePath);
  });

  it('falls back to main when git commands cannot infer a branch', () => {
    const repoPath = join(tempDir, 'repo-no-origin');
    initGitRepo(repoPath, { 'README.md': '# hello\n' }, 'main');

    expect(detectDefaultBranch(repoPath)).toBe('main');
    expect(detectRemoteUrl(repoPath)).toBe('');
  });
});

