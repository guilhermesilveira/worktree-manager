import { existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';

function runGitRaw(repoPath: string, args: string[]): { status: number; stdout: string; stderr: string } {
  const result = spawnSync('git', args, {
    cwd: repoPath,
    encoding: 'utf-8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  return {
    status: Number(result.status ?? 1),
    stdout: String(result.stdout || '').trim(),
    stderr: String(result.stderr || '').trim(),
  };
}

function runGit(repoPath: string, args: string[]): string {
  const result = runGitRaw(repoPath, args);
  if (result.status !== 0) {
    throw new Error(result.stderr || `git ${args.join(' ')} failed in ${repoPath}`);
  }
  return result.stdout;
}

function tryGit(repoPath: string, args: string[]): string {
  const result = runGitRaw(repoPath, args);
  if (result.status !== 0) {
    return '';
  }
  return result.stdout;
}

export function assertGitRepo(repoPath: string): void {
  if (!existsSync(join(repoPath, '.git'))) {
    throw new Error(`Not a git repository: ${repoPath}`);
  }
}

export function detectRemoteUrl(repoPath: string): string {
  return tryGit(repoPath, ['remote', 'get-url', 'origin']);
}

export function detectDefaultBranch(repoPath: string): string {
  const remoteHead = tryGit(repoPath, ['symbolic-ref', 'refs/remotes/origin/HEAD']);
  const match = /refs\/remotes\/origin\/(.+)$/.exec(remoteHead);
  if (match?.[1]) return match[1];

  const localHead = tryGit(repoPath, ['branch', '--show-current']);
  if (localHead) return localHead;

  return 'main';
}

export function gitWorktreePrune(repoPath: string): void {
  tryGit(repoPath, ['worktree', 'prune']);
}

export function gitDeleteBranchIfExists(repoPath: string, branchName: string): void {
  const hasBranch = runGitRaw(repoPath, ['show-ref', '--verify', '--quiet', `refs/heads/${branchName}`]);
  if (hasBranch.status === 0) {
    runGit(repoPath, ['branch', '-D', branchName]);
  }
}

export function gitWorktreeAdd(repoPath: string, worktreePath: string, branchName: string): void {
  runGit(repoPath, ['worktree', 'add', '-B', branchName, worktreePath, 'HEAD']);
}

export function gitSyncWorktreeToBranch(worktreePath: string, defaultBranch: string): void {
  const fetch = runGitRaw(worktreePath, ['fetch', 'origin']);
  if (fetch.status === 0) {
    const remoteRef = `origin/${defaultBranch}`;
    const hasRemoteBranch = runGitRaw(worktreePath, ['rev-parse', '--verify', '--quiet', remoteRef]);
    if (hasRemoteBranch.status === 0) {
      runGit(worktreePath, ['reset', '--hard', remoteRef]);
      runGit(worktreePath, ['branch', '--set-upstream-to', remoteRef]);
    } else {
      runGit(worktreePath, ['reset', '--hard', defaultBranch]);
    }
    runGit(worktreePath, ['clean', '-fd']);
    return;
  }

  runGit(worktreePath, ['reset', '--hard', defaultBranch]);
  runGit(worktreePath, ['clean', '-fd']);
}

export function gitHasUncommittedChanges(worktreePath: string): boolean {
  const result = runGitRaw(worktreePath, ['status', '--porcelain']);
  if (result.status !== 0) {
    throw new Error(result.stderr || `git status failed in ${worktreePath}`);
  }
  return result.stdout.length > 0;
}

export function gitCommitAll(worktreePath: string, message: string): boolean {
  if (!gitHasUncommittedChanges(worktreePath)) return false;
  runGit(worktreePath, ['add', '-A']);
  runGit(worktreePath, ['commit', '-m', message]);
  return true;
}

export function gitRebaseOntoRemoteBranch(worktreePath: string, branchName: string): void {
  const fetch = runGitRaw(worktreePath, ['fetch', 'origin']);
  if (fetch.status !== 0) return;

  const remoteRef = `origin/${branchName}`;
  const hasRemoteBranch = runGitRaw(worktreePath, ['rev-parse', '--verify', '--quiet', remoteRef]);
  if (hasRemoteBranch.status !== 0) return;

  const rebase = runGitRaw(worktreePath, ['rebase', remoteRef]);
  if (rebase.status === 0) return;
  runGitRaw(worktreePath, ['rebase', '--abort']);
  throw new Error(rebase.stderr || `git rebase ${remoteRef} failed in ${worktreePath}`);
}

export function gitPushHeadToBranch(worktreePath: string, branchName: string): boolean {
  runGit(worktreePath, ['push', 'origin', `HEAD:${branchName}`]);
  return true;
}

export function gitBranchExistsOnRemote(worktreePath: string, branchName: string): boolean {
  const result = runGitRaw(worktreePath, ['rev-parse', '--verify', '--quiet', `origin/${branchName}`]);
  return result.status === 0;
}

export function gitWorktreeRemove(repoPath: string, worktreePath: string): void {
  runGit(repoPath, ['worktree', 'remove', '--force', worktreePath]);
}
