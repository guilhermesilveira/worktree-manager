import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { spawnSync } from 'node:child_process';

export function makeTempDir(prefix: string): string {
  return mkdtempSync(join(tmpdir(), prefix));
}

export function removeTempDir(path: string): void {
  rmSync(path, { recursive: true, force: true });
}

export function writeFile(path: string, content: string): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content, 'utf-8');
}

function run(command: string, args: string[], cwd: string): string {
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf-8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  if ((result.status ?? 1) !== 0) {
    throw new Error(String(result.stderr || result.stdout || `${command} failed`));
  }
  return String(result.stdout || '').trim();
}

export function initGitRepo(repoPath: string, files: Record<string, string>, branch = 'main'): void {
  mkdirSync(repoPath, { recursive: true });
  run('git', ['init', '-b', branch], repoPath);
  run('git', ['config', 'user.name', 'Worktree Manager Tests'], repoPath);
  run('git', ['config', 'user.email', 'tests@example.com'], repoPath);
  for (const [relativePath, content] of Object.entries(files)) {
    writeFile(join(repoPath, relativePath), content);
  }
  run('git', ['add', '.'], repoPath);
  run('git', ['commit', '-m', 'init'], repoPath);
}

