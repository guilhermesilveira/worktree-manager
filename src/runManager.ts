import { existsSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { randomUUID } from 'node:crypto';

import { addRunEvent, addRunSlot, addRunWorktree, assignConsumerSlot, clearConsumerSlot, clearPoolWorktreeConsumer, createPoolWorktree, createRun, createSlot, deletePoolWorktree, deleteRun, deleteSlot, findReusablePoolWorktree, getPrimaryRepository, getProject, getPoolWorktree, getRepository, getRun, getRunWorktree, getSlot, inspectSlot, listConsumerSlots, listPoolWorktrees, listRepositories, listRunSlots, listRunWorktrees, listRuns, listStaleFreePoolWorktrees, updatePoolWorktreeState, updateRunStatus, updateSlotState, assignPoolWorktree } from './db.js';
import { assertGitRepo, gitBranchExistsOnRemote, gitCommitAll, gitDeleteBranchIfExists, gitHasAnyChanges, gitHasUncommittedChanges, gitPrepareWorktreeForRun, gitPushHeadToBranch, gitRebaseOntoRemoteBranch, gitResetWorktreeToDefault, gitSafeToRelease, gitWorktreeAddDetached, gitWorktreePrune, gitWorktreeRemove } from './git.js';
import type { CleanupConsumerResult, CleanupConsumerRunsResult, CleanupSlotResult, GcPoolResult, PoolWorktreeRow, PushTreeRepoResult, PushTreeResult, ReleaseTreeResult, RepositoryRow, RunRow, RunWorktreeRow, SlotRow, SlotState } from './types.js';

function sanitizeSegment(value: string): string {
  return String(value || '').trim().replace(/[^a-zA-Z0-9._-]+/g, '-').replace(/^-+|-+$/g, '') || 'repo';
}

function createRunId(): string {
  return `run-${new Date().toISOString().replaceAll(':', '').replaceAll('.', '').replaceAll('-', '').slice(0, 15)}-${randomUUID().slice(0, 8)}`;
}

function createSlotId(): string {
  return `slot-${randomUUID().slice(0, 8)}`;
}

function createPoolWorktreeId(): string {
  return `pool-${randomUUID().slice(0, 8)}`;
}

function olderThanIso(hours: number): string {
  return new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
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

function recordRunEvent(runId: string, eventType: string, message: string, payload?: unknown, level: 'INFO' | 'WARN' | 'ERROR' = 'INFO'): void {
  addRunEvent({
    runId,
    eventType,
    level,
    message,
    payloadJson: payload === undefined ? null : JSON.stringify(payload),
  });
}

function slotPathForRun(workspaceRoot: string, repoName: string): string {
  return join(workspaceRoot, 'repos', repoName);
}

function poolPathForProject(baseDir: string, repoName: string, poolWorktreeId: string): string {
  return join(baseDir, 'pool', repoName, poolWorktreeId);
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

function recreateSymlink(linkPath: string, targetPath: string): void {
  rmSync(linkPath, { recursive: true, force: true });
  mkdirSync(dirname(linkPath), { recursive: true });
  symlinkSync(targetPath, linkPath, 'dir');
}

function removePoolWorktree(repository: RepositoryRow, poolWorktree: PoolWorktreeRow): void {
  try {
    gitWorktreeRemove(repository.localPath, poolWorktree.poolPath);
  } catch {
    rmSync(poolWorktree.poolPath, { recursive: true, force: true });
  }
  deletePoolWorktree(poolWorktree.poolWorktreeId);
}

function ensurePoolWorktreeForRepository(run: RunRow, repository: RepositoryRow, consumerId?: string): PoolWorktreeRow {
  const project = getProject(run.nickname);
  if (!project) {
    throw new Error(`Unknown nickname: ${run.nickname}`);
  }

  const reusablePool = findReusablePoolWorktree(run.nickname, repository.name);
  if (reusablePool && existsSync(reusablePool.poolPath)) {
    return assignPoolWorktree(reusablePool.poolWorktreeId, consumerId);
  }
  if (reusablePool && !existsSync(reusablePool.poolPath)) {
    updatePoolWorktreeState(reusablePool.poolWorktreeId, 'BROKEN');
    clearPoolWorktreeConsumer(reusablePool.poolWorktreeId);
  }

  assertGitRepo(repository.localPath);
  gitWorktreePrune(repository.localPath);

  const poolWorktreeId = createPoolWorktreeId();
  const poolPath = poolPathForProject(project.baseDir, repository.name, poolWorktreeId);
  rmSync(poolPath, { recursive: true, force: true });
  mkdirSync(join(project.baseDir, 'pool', repository.name), { recursive: true });
  try {
    gitWorktreeAddDetached(repository.localPath, poolPath);
  } catch (error: unknown) {
    rmSync(poolPath, { recursive: true, force: true });
    throw error;
  }

  return createPoolWorktree({
    poolWorktreeId,
    nickname: run.nickname,
    repoName: repository.name,
    poolPath,
    state: 'BUSY',
    currentConsumerId: consumerId,
  });
}

function createRunSlotForRepository(run: RunRow, repository: RepositoryRow, consumerId?: string): { slot: SlotRow; poolWorktree: PoolWorktreeRow; worktree: RunWorktreeRow } {
  const poolWorktree = ensurePoolWorktreeForRepository(run, repository, consumerId);
  const branchName = branchNameForRun(run.runId, repository.name);
  const slotId = createSlotId();
  const slotPath = slotPathForRun(run.workspaceRoot, repository.name);
  try {
    gitPrepareWorktreeForRun(poolWorktree.poolPath, branchName, repository.defaultBranch);
    mkdirSync(join(run.workspaceRoot, 'repos'), { recursive: true });
    recreateSymlink(slotPath, poolWorktree.poolPath);

    const slot = createSlot({
      slotId,
      nickname: run.nickname,
      repoName: repository.name,
      slotPath,
      poolWorktreeId: poolWorktree.poolWorktreeId,
      state: 'BUSY',
      currentConsumerId: consumerId,
    });
    if (consumerId) {
      assignConsumerSlot(consumerId, slot.slotId);
    }
    addRunSlot({
      runId: run.runId,
      slotId: slot.slotId,
      repoName: repository.name,
      poolWorktreeId: poolWorktree.poolWorktreeId,
    });

    const worktree = addRunWorktree({
      runId: run.runId,
      repoName: repository.name,
      worktreePath: slot.slotPath,
      branchName,
      poolWorktreeId: poolWorktree.poolWorktreeId,
      isPrimary: repository.isPrimary,
    });

    return { slot, poolWorktree, worktree };
  } catch (error: unknown) {
    rmSync(slotPath, { recursive: true, force: true });
    clearConsumerSlot(slotId);
    deleteSlot(slotId);
    removePoolWorktree(repository, poolWorktree);
    throw error;
  }
}

function cleanupAssignedPool(repository: RepositoryRow, slot: SlotRow, poolWorktree: PoolWorktreeRow, branchName?: string): CleanupSlotResult {
  try {
    gitResetWorktreeToDefault(poolWorktree.poolPath, repository.defaultBranch);
    if (branchName) {
      gitDeleteBranchIfExists(repository.localPath, branchName);
    }
    rmSync(slot.slotPath, { recursive: true, force: true });
    clearConsumerSlot(slot.slotId);
    clearPoolWorktreeConsumer(poolWorktree.poolWorktreeId);
    const cleanedSlot = updateSlotState(slot.slotId, 'FREE');
    updatePoolWorktreeState(poolWorktree.poolWorktreeId, 'FREE');
    return {
      slotId: cleanedSlot.slotId,
      repoName: cleanedSlot.repoName,
      slotPath: cleanedSlot.slotPath,
      state: 'FREE',
    };
  } catch {
    rmSync(slot.slotPath, { recursive: true, force: true });
    clearConsumerSlot(slot.slotId);
    clearPoolWorktreeConsumer(poolWorktree.poolWorktreeId);
    updatePoolWorktreeState(poolWorktree.poolWorktreeId, 'BROKEN');
    const brokenSlot = updateSlotState(slot.slotId, 'BROKEN');
    return {
      slotId: brokenSlot.slotId,
      repoName: brokenSlot.repoName,
      slotPath: brokenSlot.slotPath,
      state: brokenSlot.state,
    };
  }
}

function releaseAssignedPoolWithoutCleanup(slot: SlotRow, poolWorktree: PoolWorktreeRow): CleanupSlotResult {
  let nextState: SlotState = 'DIRTY';
  try {
    nextState = gitHasAnyChanges(poolWorktree.poolPath) ? 'DIRTY' : 'FREE';
  } catch {
    nextState = 'BROKEN';
  }
  rmSync(slot.slotPath, { recursive: true, force: true });
  clearConsumerSlot(slot.slotId);
  clearPoolWorktreeConsumer(poolWorktree.poolWorktreeId);
  updatePoolWorktreeState(poolWorktree.poolWorktreeId, nextState);
  const updatedSlot = updateSlotState(slot.slotId, nextState);
  return {
    slotId: updatedSlot.slotId,
    repoName: updatedSlot.repoName,
    slotPath: updatedSlot.slotPath,
    state: updatedSlot.state,
  };
}

export function createNewTree(nickname: string, selectedRepoNames: string[], consumerId?: string): { run: RunRow; worktrees: RunWorktreeRow[] } {
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
    const worktrees = selectedRepositories.map((repository) => createRunSlotForRepository(run, repository, consumerId).worktree);
    recordRunEvent(run.runId, 'RUN_CREATED', `Allocated ${worktrees.length} writable repo(s)`, {
      consumerId: consumerId || null,
      repositories: worktrees.map((worktree) => ({ repoName: worktree.repoName, branchName: worktree.branchName, poolWorktreeId: worktree.poolWorktreeId })),
    });
    writeRunManifest(run, worktrees);
    return { run, worktrees };
  } catch (error: unknown) {
    updateRunStatus(run.runId, 'FAILED');
    recordRunEvent(run.runId, 'RUN_CREATE_FAILED', String(error instanceof Error ? error.message : error), undefined, 'ERROR');
    purgeFailedRun(run);
    throw error;
  }
}

export function promoteRunRepository(runId: string, repoName: string, consumerId?: string): { run: RunRow; worktree: RunWorktreeRow } {
  const run = getRun(runId);
  if (!run) {
    throw new Error(`Unknown run: ${runId}`);
  }
  const existing = getRunWorktree(runId, repoName);
  if (existing) {
    recordRunEvent(run.runId, 'RUN_PROMOTE_SKIPPED', `Writable repo ${repoName} was already attached`, {
      repoName,
      branchName: existing.branchName,
      poolWorktreeId: existing.poolWorktreeId,
    });
    return { run, worktree: existing };
  }
  const repository = getRepository(run.nickname, repoName);
  if (!repository) {
    throw new Error(`Unknown repository ${repoName} for nickname ${run.nickname}`);
  }
  const { worktree } = createRunSlotForRepository(run, repository, consumerId);
  recordRunEvent(run.runId, 'RUN_PROMOTED', `Promoted ${repoName} into this run`, {
    consumerId: consumerId || null,
    repoName: worktree.repoName,
    branchName: worktree.branchName,
    poolWorktreeId: worktree.poolWorktreeId,
  });
  writeRunManifest(run, listRunWorktrees(runId));
  return { run, worktree };
}

export function releaseRunTree(runId: string, cleanup = false): ReleaseTreeResult {
  const run = getRun(runId);
  if (!run) {
    throw new Error(`Unknown run: ${runId}`);
  }

  const worktrees = listRunWorktrees(runId);
  const slots = worktrees.map((worktree) => {
    const repository = getRepository(run.nickname, worktree.repoName);
    const slot = getSlot(listRunSlots(runId).find((entry) => entry.repoName === worktree.repoName)?.slotId || '');
    const poolWorktree = getPoolWorktree(worktree.poolWorktreeId);
    if (!repository || !slot || !poolWorktree) {
      throw new Error(`Missing pool or slot metadata for ${worktree.repoName}`);
    }
    return cleanup
      ? cleanupAssignedPool(repository, slot, poolWorktree, worktree.branchName)
      : releaseAssignedPoolWithoutCleanup(slot, poolWorktree);
  });

  const releasedRun = updateRunStatus(runId, 'RELEASED');
  recordRunEvent(runId, cleanup ? 'RUN_RELEASED_CLEAN' : 'RUN_RELEASED', `Released ${slots.length} slot(s)`, {
    cleanup,
    slots,
  }, slots.some((slot) => slot.state !== 'FREE') ? 'WARN' : 'INFO');

  return { run: releasedRun, slots };
}

export function cleanupOneSlot(slotId: string): CleanupSlotResult {
  const slot = getSlot(slotId);
  if (!slot) {
    throw new Error(`Unknown slot: ${slotId}`);
  }
  const repository = getRepository(slot.nickname, slot.repoName);
  const poolWorktree = getPoolWorktree(slot.poolWorktreeId);
  if (!repository || !poolWorktree) {
    clearConsumerSlot(slotId);
    if (poolWorktree) {
      clearPoolWorktreeConsumer(poolWorktree.poolWorktreeId);
      updatePoolWorktreeState(poolWorktree.poolWorktreeId, 'BROKEN');
    }
    const brokenSlot = updateSlotState(slotId, 'BROKEN');
    return {
      slotId: brokenSlot.slotId,
      repoName: brokenSlot.repoName,
      slotPath: brokenSlot.slotPath,
      state: brokenSlot.state,
    };
  }
  return cleanupAssignedPool(repository, slot, poolWorktree);
}

export function cleanupConsumerSlots(consumerId: string): CleanupConsumerResult {
  const consumerSlots = listConsumerSlots(consumerId);
  const slots = consumerSlots.map((consumerSlot) => cleanupOneSlot(consumerSlot.slotId));
  return { consumerId, slots };
}

function listCurrentConsumerRunIds(consumerId: string): string[] {
  const runIds = new Set<string>();
  for (const consumerSlot of listConsumerSlots(consumerId)) {
    const inspection = inspectSlot(consumerSlot.slotId);
    for (const run of inspection?.runs || []) {
      if (run.status === 'ACTIVE' || run.status === 'PUSHED') {
        runIds.add(run.runId);
      }
    }
  }
  return [...runIds].sort();
}

function safeReleaseReason(run: RunRow): string | null {
  for (const worktree of listRunWorktrees(run.runId)) {
    const repository = getRepository(run.nickname, worktree.repoName);
    const poolWorktree = getPoolWorktree(worktree.poolWorktreeId);
    if (!repository || !poolWorktree) {
      return `missing repository or pool metadata for ${worktree.repoName}`;
    }
    const safety = gitSafeToRelease(poolWorktree.poolPath, worktree.branchName, repository.defaultBranch);
    if (!safety.safe) {
      return `${worktree.repoName}: ${safety.reason}`;
    }
  }
  return null;
}

export function cleanupConsumerRuns(consumerId: string, safe = false): CleanupConsumerRunsResult {
  const released: CleanupConsumerRunsResult['released'] = [];
  const skipped: CleanupConsumerRunsResult['skipped'] = [];

  for (const runId of listCurrentConsumerRunIds(consumerId)) {
    const run = getRun(runId);
    if (!run) {
      skipped.push({ runId, status: 'PURGED', reason: 'run disappeared before cleanup' });
      continue;
    }
    if (safe) {
      const reason = safeReleaseReason(run);
      if (reason) {
        skipped.push({ runId, status: run.status, reason });
        recordRunEvent(run.runId, 'RUN_CLEANUP_CONSUMER_SKIPPED', `Skipped cleanup-consumer ${consumerId}: ${reason}`, { consumerId, safe }, 'WARN');
        continue;
      }
    }

    const result = releaseRunTree(run.runId, true);
    recordRunEvent(run.runId, 'RUN_CLEANUP_CONSUMER_RELEASED', `Released by cleanup-consumer ${consumerId}`, { consumerId, safe });
    released.push({ runId: result.run.runId, status: result.run.status, slots: result.slots });
  }

  return { consumerId, safe, released, skipped };
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
      const poolWorktree = getPoolWorktree(worktree.poolWorktreeId);
      if (!repository || !poolWorktree) {
        throw new Error(`Missing repository or pool metadata for ${worktree.repoName}`);
      }

      const commitCreated = gitHasUncommittedChanges(poolWorktree.poolPath)
        ? gitCommitAll(poolWorktree.poolPath, `worktree-manager: sync ${worktree.repoName} from ${run.runId}`)
        : false;

      if (gitBranchExistsOnRemote(poolWorktree.poolPath, worktree.branchName)) {
        gitRebaseOntoRemoteBranch(poolWorktree.poolPath, worktree.branchName);
      }
      const pushed = gitPushHeadToBranch(poolWorktree.poolPath, worktree.branchName);

      results.push({
        repoName: worktree.repoName,
        worktreePath: worktree.worktreePath,
        branchName: worktree.branchName,
        targetBranch: worktree.branchName,
        commitCreated,
        pushed,
      });
      recordRunEvent(runId, 'RUN_PUSH_REPOSITORY', `Pushed ${worktree.repoName} to ${worktree.branchName}`, {
        repoName: worktree.repoName,
        branchName: worktree.branchName,
        commitCreated,
        pushed,
      });
    }

    const pushedRun = updateRunStatus(runId, 'PUSHED');
    recordRunEvent(runId, 'RUN_PUSHED', `Pushed ${results.length} repo branch(es)`, { repositories: results });
    writeRunManifest(pushedRun, worktrees);
    return { run: pushedRun, repositories: results };
  } catch (error: unknown) {
    const failedRun = updateRunStatus(runId, 'FAILED');
    recordRunEvent(runId, 'RUN_PUSH_FAILED', String(error instanceof Error ? error.message : error), undefined, 'ERROR');
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

    const runSlots = listRunSlots(run.runId);
    releaseRunTree(run.runId, true);
    for (const runSlot of runSlots) {
      deleteSlot(runSlot.slotId);
    }
    rmSync(run.workspaceRoot, { recursive: true, force: true });
    deleteRun(run.runId);
    purgedRunIds.push(run.runId);
  }

  return { purgedRunIds, skippedRunIds };
}

function purgeFailedRun(run: RunRow): void {
  const runSlots = listRunSlots(run.runId);
  for (const worktree of listRunWorktrees(run.runId)) {
    const repository = getRepository(run.nickname, worktree.repoName);
    const poolWorktree = getPoolWorktree(worktree.poolWorktreeId);
    const slot = getSlot(runSlots.find((entry) => entry.repoName === worktree.repoName)?.slotId || '');
    if (repository && poolWorktree) {
      removePoolWorktree(repository, poolWorktree);
    } else if (poolWorktree) {
      rmSync(poolWorktree.poolPath, { recursive: true, force: true });
      deletePoolWorktree(poolWorktree.poolWorktreeId);
    }
    if (slot) {
      clearConsumerSlot(slot.slotId);
      deleteSlot(slot.slotId);
    }
  }
  for (const runSlot of runSlots) {
    deleteSlot(runSlot.slotId);
  }
  rmSync(run.workspaceRoot, { recursive: true, force: true });
  deleteRun(run.runId);
}

export function purgeFailedTrees(nickname?: string): { purgedRunIds: string[]; skippedRunIds: string[] } {
  const purgedRunIds: string[] = [];
  const skippedRunIds: string[] = [];

  for (const run of listRuns(nickname)) {
    if (run.status !== 'FAILED') {
      skippedRunIds.push(run.runId);
      continue;
    }
    purgeFailedRun(run);
    purgedRunIds.push(run.runId);
  }

  return { purgedRunIds, skippedRunIds };
}

export function gcPoolWorktrees(olderThanHours: number, nickname?: string): GcPoolResult {
  if (!Number.isFinite(olderThanHours) || olderThanHours < 0) {
    throw new Error(`olderThanHours must be a non-negative number: ${olderThanHours}`);
  }

  const poolsToRemove = olderThanHours === 0
    ? listPoolWorktrees(nickname).filter((poolWorktree) => poolWorktree.state === 'FREE')
    : listStaleFreePoolWorktrees(olderThanIso(olderThanHours), nickname);
  const removed: GcPoolResult['removed'] = [];
  const skipped: string[] = [];

  for (const poolWorktree of poolsToRemove) {
    const repository = getRepository(poolWorktree.nickname, poolWorktree.repoName);
    if (!repository) {
      skipped.push(poolWorktree.poolWorktreeId);
      continue;
    }

    removePoolWorktree(repository, poolWorktree);
    removed.push({
      poolWorktreeId: poolWorktree.poolWorktreeId,
      repoName: poolWorktree.repoName,
      poolPath: poolWorktree.poolPath,
    });
  }

  return { removed, skipped };
}
