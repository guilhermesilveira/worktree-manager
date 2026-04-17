import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

import { addRunWorktree, createRun, deleteRun, getPrimaryRepository, getProject, getRepository, getRun, getRunWorktree, listRepositories, listRunWorktrees, listRuns, updateRunStatus } from './db.js';
import { assertGitRepo, gitBranchExistsOnRemote, gitCommitAll, gitDeleteBranchIfExists, gitHasUncommittedChanges, gitPushHeadToBranch, gitRebaseOntoRemoteBranch, gitSyncWorktreeToBranch, gitWorktreeAdd, gitWorktreePrune, gitWorktreeRemove } from './git.js';
import type { PushTreeRepoResult, PushTreeResult, RepositoryRow, RunRow, RunWorktreeRow } from './types.js';

function sanitizeSegment(value: string): string {
  return String(value || '').trim().replace(/[^a-zA-Z0-9._-]+/g, '-').replace(/^-+|-+$/g, '') || 'repo';
}

function createRunId(): string {
  return `run-${new Date().toISOString().replaceAll(':', '').replaceAll('.', '').replaceAll('-', '').slice(0, 15)}-${randomUUID().slice(0, 8)}`;
}

function branchNameForRun(runId: string, repoName: string): string {
  return `wt/${sanitizeSegment(runId)}-${sanitizeSegment(repoName)}`;
}

function manifestPath(workspaceRoot: string): string {
  return join(workspaceRoot, 'run.json');
}

function writeRunManifest(run: RunRow, worktrees: RunWorktreeRow[]): void {
  writeFileSync(manifestPath(run.workspaceRoot), `${JSON.stringify({ run, worktrees }, null, 2)}\n`, 'utf-8');
}

function resolveSelectedRepositories(nickname: string, selectedNames: string[]): RepositoryRow[] {
  const cleanedNames = selectedNames.map((name) => String(name || '').trim()).filter(Boolean);
  if (cleanedNames.length === 0) {
    const primaryRepository = getPrimaryRepository(nickname);
    if (!primaryRepository) {
      throw new Error(`No primary repository configured for nickname: ${nickname}`);
    }
    return [primaryRepository];
  }

  return cleanedNames.map((name) => {
    const repository = getRepository(nickname, name);
    if (!repository) {
      throw new Error(`Unknown repository ${name} for nickname ${nickname}`);
    }
    return repository;
  });
}

function createWorktreeForRepository(run: RunRow, repository: RepositoryRow): RunWorktreeRow {
  assertGitRepo(repository.localPath);
  gitWorktreePrune(repository.localPath);

  const branchName = branchNameForRun(run.runId, repository.name);
  const worktreePath = join(run.workspaceRoot, 'repos', repository.name);
  if (existsSync(worktreePath)) {
    rmSync(worktreePath, { recursive: true, force: true });
  }
  gitDeleteBranchIfExists(repository.localPath, branchName);
  mkdirSync(join(run.workspaceRoot, 'repos'), { recursive: true });
  gitWorktreeAdd(repository.localPath, worktreePath, branchName);
  gitSyncWorktreeToBranch(worktreePath, repository.defaultBranch);

  return addRunWorktree({
    runId: run.runId,
    repoName: repository.name,
    worktreePath,
    branchName,
    isPrimary: repository.isPrimary,
  });
}

export function createNewTree(nickname: string, selectedRepoNames: string[]): { run: RunRow; worktrees: RunWorktreeRow[] } {
  const project = getProject(nickname);
  if (!project) {
    throw new Error(`Unknown nickname: ${nickname}`);
  }

  const selectedRepositories = resolveSelectedRepositories(nickname, selectedRepoNames);
  mkdirSync(project.baseDir, { recursive: true });
  const runId = createRunId();
  const workspaceRoot = join(project.baseDir, runId);
  mkdirSync(workspaceRoot, { recursive: true });

  const run = createRun({
    runId,
    nickname,
    workspaceRoot,
  });

  try {
    const worktrees = selectedRepositories.map((repository) => createWorktreeForRepository(run, repository));
    writeRunManifest(run, worktrees);
    return { run, worktrees };
  } catch (error: unknown) {
    updateRunStatus(run.runId, 'FAILED');
    throw error;
  }
}

export function promoteRunRepository(runId: string, repoName: string): { run: RunRow; worktree: RunWorktreeRow } {
  const run = getRun(runId);
  if (!run) {
    throw new Error(`Unknown run: ${runId}`);
  }
  const existing = getRunWorktree(runId, repoName);
  if (existing) {
    return { run, worktree: existing };
  }
  const repository = getRepository(run.nickname, repoName);
  if (!repository) {
    throw new Error(`Unknown repository ${repoName} for nickname ${run.nickname}`);
  }
  const worktree = createWorktreeForRepository(run, repository);
  writeRunManifest(run, listRunWorktrees(runId));
  return { run, worktree };
}

export function pushRunTree(runId: string): PushTreeResult {
  const run = getRun(runId);
  if (!run) {
    throw new Error(`Unknown run: ${runId}`);
  }
  const worktrees = listRunWorktrees(runId);
  if (worktrees.length === 0) {
    throw new Error(`Run ${runId} has no writable worktrees`);
  }

  const repositoryRows = listRepositories(run.nickname);
  const repositoryByName = new Map(repositoryRows.map((repository) => [repository.name, repository]));
  const results: PushTreeRepoResult[] = [];

  try {
    for (const worktree of worktrees) {
      const repository = repositoryByName.get(worktree.repoName);
      if (!repository) {
        throw new Error(`Missing repository metadata for ${worktree.repoName}`);
      }

      const commitCreated = gitHasUncommittedChanges(worktree.worktreePath)
        ? gitCommitAll(worktree.worktreePath, `worktree-manager: sync ${worktree.repoName} from ${run.runId}`)
        : false;

      if (gitBranchExistsOnRemote(worktree.worktreePath, worktree.branchName)) {
        gitRebaseOntoRemoteBranch(worktree.worktreePath, worktree.branchName);
      }
      const pushed = gitPushHeadToBranch(worktree.worktreePath, worktree.branchName);

      results.push({
        repoName: worktree.repoName,
        worktreePath: worktree.worktreePath,
        branchName: worktree.branchName,
        targetBranch: worktree.branchName,
        commitCreated,
        pushed,
      });
    }

    const pushedRun = updateRunStatus(runId, 'PUSHED');
    writeRunManifest(pushedRun, worktrees);
    return { run: pushedRun, repositories: results };
  } catch (error: unknown) {
    const failedRun = updateRunStatus(runId, 'FAILED');
    writeRunManifest(failedRun, worktrees);
    throw error;
  }
}

export function purgeTrees(nickname?: string, force = false): { purgedRunIds: string[]; skippedRunIds: string[] } {
  const runs = listRuns(nickname);
  const purgedRunIds: string[] = [];
  const skippedRunIds: string[] = [];

  for (const run of runs) {
    if (run.status === 'ACTIVE' && !force) {
      skippedRunIds.push(run.runId);
      continue;
    }

    const worktrees = listRunWorktrees(run.runId);
    for (const worktree of worktrees) {
      const repository = getRepository(run.nickname, worktree.repoName);
      if (repository) {
        try {
          gitWorktreeRemove(repository.localPath, worktree.worktreePath);
        } catch {
          rmSync(worktree.worktreePath, { recursive: true, force: true });
        }
      } else {
        rmSync(worktree.worktreePath, { recursive: true, force: true });
      }
    }

    rmSync(run.workspaceRoot, { recursive: true, force: true });
    deleteRun(run.runId);
    purgedRunIds.push(run.runId);
  }

  return { purgedRunIds, skippedRunIds };
}
