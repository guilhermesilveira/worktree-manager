#!/usr/bin/env node

import { existsSync } from 'node:fs';

import { Command } from 'commander';

import { getPoolStats, getProject, getSlotStats, inspectPoolWorktree, inspectSlot, listPoolWorktrees, listRepositories, listSlots, upsertProject, upsertRepository } from './db.js';
import { assertGitRepo, detectDefaultBranch, detectRemoteUrl } from './git.js';
import { resolveAbsolutePath, resolveDatabasePath } from './paths.js';
import { cleanupConsumerSlots, cleanupOneSlot, createNewTree, gcPoolWorktrees, promoteRunRepository, purgeTrees, pushRunTree, releaseRunTree } from './runManager.js';
import { searchAcrossRepos } from './search.js';

function requiredText(value: string, label: string): string {
  const text = String(value || '').trim();
  if (!text) throw new Error(`Missing ${label}`);
  return text;
}

function writeJson(data: unknown): void {
  process.stdout.write(`${JSON.stringify(data, null, 2)}\n`);
}

const program = new Command();

program
  .name('worktree-manager')
  .description('Manage related repositories and transient multi-repo worktree flows.')
  .version('0.2.0');

program
  .command('register')
  .description('Register or update a project nickname and its run base directory')
  .argument('<nickname>')
  .argument('<baseDir>')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string, baseDirArg: string, options: { json?: boolean }) => {
    const nickname = requiredText(nicknameArg, 'nickname');
    const baseDir = resolveAbsolutePath(requiredText(baseDirArg, 'baseDir'));
    const project = upsertProject(nickname, baseDir);
    if (options.json) {
      writeJson({ project, dbPath: resolveDatabasePath() });
      return;
    }
    process.stdout.write(`registered ${project.nickname} -> ${project.baseDir}\n`);
  });

program
  .command('add')
  .description('Add or update one repository under a project nickname')
  .argument('<nickname>')
  .argument('<repoName>')
  .argument('<repoPath>')
  .option('--remote <remoteUrl>', 'Override detected remote URL')
  .option('--branch <defaultBranch>', 'Override detected default branch')
  .option('--primary', 'Mark this repository as the primary default for new-tree')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string, repoNameArg: string, repoPathArg: string, options: {
    remote?: string;
    branch?: string;
    primary?: boolean;
    json?: boolean;
  }) => {
    const nickname = requiredText(nicknameArg, 'nickname');
    const project = getProject(nickname);
    if (!project) {
      throw new Error(`Unknown nickname: ${nickname}`);
    }

    const repoName = requiredText(repoNameArg, 'repoName');
    const repoPath = resolveAbsolutePath(requiredText(repoPathArg, 'repoPath'));
    if (!existsSync(repoPath)) {
      throw new Error(`Repository path does not exist: ${repoPath}`);
    }
    assertGitRepo(repoPath);

    const remoteUrl = String(options.remote || '').trim() || detectRemoteUrl(repoPath);
    const defaultBranch = String(options.branch || '').trim() || detectDefaultBranch(repoPath);

    const repository = upsertRepository({
      nickname,
      name: repoName,
      localPath: repoPath,
      remoteUrl,
      defaultBranch,
      isPrimary: options.primary === true,
    });

    if (options.json) {
      writeJson({ repository, dbPath: resolveDatabasePath() });
      return;
    }

    process.stdout.write(`added ${repository.nickname}/${repository.name} -> ${repository.localPath}${repository.isPrimary ? ' [primary]' : ''}\n`);
  });

program
  .command('list')
  .description('List repositories registered for a nickname')
  .argument('<nickname>')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string, options: { json?: boolean }) => {
    const nickname = requiredText(nicknameArg, 'nickname');
    const project = getProject(nickname);
    if (!project) {
      throw new Error(`Unknown nickname: ${nickname}`);
    }
    const repositories = listRepositories(nickname);
    if (options.json) {
      writeJson({ project, repositories, dbPath: resolveDatabasePath() });
      return;
    }
    if (repositories.length === 0) {
      process.stdout.write(`${nickname}: no repositories registered\n`);
      return;
    }
    for (const repository of repositories) {
      process.stdout.write(`${repository.name}\t${repository.localPath}\t${repository.defaultBranch}${repository.isPrimary ? '\tprimary' : ''}\n`);
    }
  });

program
  .command('search')
  .description('Search across all repositories registered for a nickname using rg')
  .argument('<nickname>')
  .argument('<pattern>')
  .option('--repo <name...>', 'Restrict search to selected repositories')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string, patternArg: string, options: { repo?: string[]; json?: boolean }) => {
    const nickname = requiredText(nicknameArg, 'nickname');
    const pattern = requiredText(patternArg, 'pattern');
    const repositories = listRepositories(nickname);
    if (repositories.length === 0) {
      throw new Error(`No repositories registered for nickname: ${nickname}`);
    }

    const selectedNames = new Set((options.repo || []).map((name) => String(name || '').trim()).filter(Boolean));
    const selectedRepos = selectedNames.size === 0
      ? repositories
      : repositories.filter((repository) => selectedNames.has(repository.name));
    if (selectedRepos.length === 0) {
      throw new Error(`No repositories matched the requested selection for ${nickname}`);
    }

    const result = searchAcrossRepos(nickname, selectedRepos.map((repository) => repository.localPath), pattern);
    if (options.json) {
      writeJson(result);
      return;
    }

    if (result.stdout) process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    process.exitCode = result.exitCode === 1 ? 0 : result.exitCode;
  });

program
  .command('new-tree')
  .description('Create a transient run workspace with one or more writable worktrees')
  .argument('<nickname>')
  .option('--repo <name...>', 'Create writable worktrees for these repositories; defaults to the primary repository')
  .option('--consumer <consumerId>', 'Optional consumer/agent id to associate with allocated slots')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string, options: { repo?: string[]; consumer?: string; json?: boolean }) => {
    const nickname = requiredText(nicknameArg, 'nickname');
    const result = createNewTree(nickname, options.repo || [], options.consumer);
    if (options.json) {
      writeJson(result);
      return;
    }
    process.stdout.write(`${result.run.runId}\t${result.run.workspaceRoot}\n`);
    for (const worktree of result.worktrees) {
      process.stdout.write(`${worktree.repoName}\t${worktree.worktreePath}\t${worktree.branchName}\n`);
    }
  });

program
  .command('promote')
  .description('Promote a related repository into a writable worktree for a run')
  .argument('<runId>')
  .argument('<repoName>')
  .option('--consumer <consumerId>', 'Optional consumer/agent id to associate with the promoted slot')
  .option('--json', 'Emit JSON output')
  .action((runIdArg: string, repoNameArg: string, options: { consumer?: string; json?: boolean }) => {
    const runId = requiredText(runIdArg, 'runId');
    const repoName = requiredText(repoNameArg, 'repoName');
    const result = promoteRunRepository(runId, repoName, options.consumer);
    if (options.json) {
      writeJson(result);
      return;
    }
    process.stdout.write(`${result.run.runId}\t${result.worktree.repoName}\t${result.worktree.worktreePath}\n`);
  });

program
  .command('list-slots')
  .description('List current slot rows and usage state')
  .argument('[nickname]')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string | undefined, options: { json?: boolean }) => {
    const nickname = String(nicknameArg || '').trim() || undefined;
    const slots = listSlots(nickname);
    if (options.json) {
      writeJson({ slots, dbPath: resolveDatabasePath() });
      return;
    }
    if (slots.length === 0) {
      process.stdout.write(`${nickname || 'all'}: no slots\n`);
      return;
    }
    for (const slot of slots) {
      process.stdout.write(`${slot.slotId}\t${slot.nickname}\t${slot.repoName}\t${slot.state}\t${slot.currentConsumerId || '-'}\t${slot.slotPath}\n`);
    }
  });

program
  .command('list-pool-worktrees')
  .description('List pooled reusable worktrees and their current state')
  .argument('[nickname]')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string | undefined, options: { json?: boolean }) => {
    const nickname = String(nicknameArg || '').trim() || undefined;
    const poolWorktrees = listPoolWorktrees(nickname);
    if (options.json) {
      writeJson({ poolWorktrees, dbPath: resolveDatabasePath() });
      return;
    }
    if (poolWorktrees.length === 0) {
      process.stdout.write(`${nickname || 'all'}: no pooled worktrees\n`);
      return;
    }
    for (const poolWorktree of poolWorktrees) {
      process.stdout.write(`${poolWorktree.poolWorktreeId}\t${poolWorktree.nickname}\t${poolWorktree.repoName}\t${poolWorktree.state}\t${poolWorktree.currentConsumerId || '-'}\t${poolWorktree.poolPath}\n`);
    }
  });

program
  .command('inspect-slot')
  .description('Show one slot with its run bindings and current consumer assignment')
  .argument('<slotId>')
  .option('--json', 'Emit JSON output')
  .action((slotIdArg: string, options: { json?: boolean }) => {
    const slotId = requiredText(slotIdArg, 'slotId');
    const inspection = inspectSlot(slotId);
    if (!inspection) {
      throw new Error(`Unknown slot: ${slotId}`);
    }
    if (options.json) {
      writeJson({ inspection, dbPath: resolveDatabasePath() });
      return;
    }
    process.stdout.write(`${inspection.slot.slotId}\t${inspection.slot.nickname}\t${inspection.slot.repoName}\t${inspection.slot.state}\t${inspection.slot.currentConsumerId || '-'}\n`);
    for (const run of inspection.runs) {
      process.stdout.write(`run\t${run.runId}\t${run.status}\t${run.repoName}\t${run.branchName || '-'}\t${run.workspaceRoot}\n`);
    }
  });

program
  .command('inspect-pool-worktree')
  .description('Show one pooled worktree with its slot links and run usage')
  .argument('<poolWorktreeId>')
  .option('--json', 'Emit JSON output')
  .action((poolWorktreeIdArg: string, options: { json?: boolean }) => {
    const poolWorktreeId = requiredText(poolWorktreeIdArg, 'poolWorktreeId');
    const inspection = inspectPoolWorktree(poolWorktreeId);
    if (!inspection) {
      throw new Error(`Unknown pooled worktree: ${poolWorktreeId}`);
    }
    if (options.json) {
      writeJson({ inspection, dbPath: resolveDatabasePath() });
      return;
    }
    process.stdout.write(`${inspection.poolWorktree.poolWorktreeId}\t${inspection.poolWorktree.nickname}\t${inspection.poolWorktree.repoName}\t${inspection.poolWorktree.state}\t${inspection.poolWorktree.currentConsumerId || '-'}\n`);
    for (const slot of inspection.slots) {
      process.stdout.write(`slot\t${slot.slotId}\t${slot.state}\t${slot.currentConsumerId || '-'}\t${slot.slotPath}\n`);
    }
    for (const run of inspection.runs) {
      process.stdout.write(`run\t${run.runId}\t${run.status}\t${run.repoName}\t${run.branchName || '-'}\t${run.workspaceRoot}\n`);
    }
  });

program
  .command('slot-stats')
  .description('Show aggregated slot counts for scheduler capacity decisions')
  .argument('[nickname]')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string | undefined, options: { json?: boolean }) => {
    const nickname = String(nicknameArg || '').trim() || undefined;
    const stats = getSlotStats(nickname);
    if (options.json) {
      writeJson({ stats, dbPath: resolveDatabasePath() });
      return;
    }
    process.stdout.write(`total=${stats.total}\n`);
    for (const state of stats.byState) {
      process.stdout.write(`state\t${state.state}\t${state.total}\n`);
    }
    for (const group of stats.byNickname) {
      process.stdout.write(`nickname\t${group.nickname}\ttotal=${group.total}\tfree=${group.free}\tbusy=${group.busy}\tdirty=${group.dirty}\tbroken=${group.broken}\n`);
    }
  });

program
  .command('pool-stats')
  .description('Show aggregated pooled worktree counts')
  .argument('[nickname]')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string | undefined, options: { json?: boolean }) => {
    const nickname = String(nicknameArg || '').trim() || undefined;
    const stats = getPoolStats(nickname);
    if (options.json) {
      writeJson({ stats, dbPath: resolveDatabasePath() });
      return;
    }
    process.stdout.write(`total=${stats.total}\n`);
    for (const state of stats.byState) {
      process.stdout.write(`state\t${state.state}\t${state.total}\n`);
    }
    for (const group of stats.byNickname) {
      process.stdout.write(`nickname\t${group.nickname}\ttotal=${group.total}\tfree=${group.free}\tbusy=${group.busy}\tdirty=${group.dirty}\tbroken=${group.broken}\n`);
    }
  });

program
  .command('push-tree')
  .description('Commit and push every writable worktree attached to a run to its run branch')
  .argument('<runId>')
  .option('--json', 'Emit JSON output')
  .action((runIdArg: string, options: { json?: boolean }) => {
    const runId = requiredText(runIdArg, 'runId');
    const result = pushRunTree(runId);
    if (options.json) {
      writeJson(result);
      return;
    }
    process.stdout.write(`${result.run.runId}\t${result.run.status}\n`);
    for (const repository of result.repositories) {
      process.stdout.write(`${repository.repoName}\tbranch=${repository.targetBranch}\tcommit=${repository.commitCreated}\tpushed=${repository.pushed}\n`);
    }
  });

program
  .command('release-tree')
  .description('Release a run’s slots for reuse, optionally cleaning them back to the default branch first')
  .argument('<runId>')
  .option('--cleanup', 'Reset slots to their default branch and remove local leftovers before releasing them')
  .option('--json', 'Emit JSON output')
  .action((runIdArg: string, options: { cleanup?: boolean; json?: boolean }) => {
    const runId = requiredText(runIdArg, 'runId');
    const result = releaseRunTree(runId, options.cleanup === true);
    if (options.json) {
      writeJson(result);
      return;
    }
    process.stdout.write(`${result.run.runId}\tslots=${result.slots.length}\n`);
    for (const slot of result.slots) {
      process.stdout.write(`${slot.slotId}\t${slot.repoName}\t${slot.state}\t${slot.slotPath}\n`);
    }
  });

program
  .command('cleanup-slot')
  .description('Reset one slot back to a reusable default-branch state')
  .argument('<slotId>')
  .option('--json', 'Emit JSON output')
  .action((slotIdArg: string, options: { json?: boolean }) => {
    const slotId = requiredText(slotIdArg, 'slotId');
    const result = cleanupOneSlot(slotId);
    if (options.json) {
      writeJson(result);
      return;
    }
    process.stdout.write(`${result.slotId}\t${result.repoName}\t${result.state}\t${result.slotPath}\n`);
  });

program
  .command('cleanup-consumer')
  .description('Reset every slot currently assigned to one consumer/agent')
  .argument('<consumerId>')
  .option('--json', 'Emit JSON output')
  .action((consumerIdArg: string, options: { json?: boolean }) => {
    const consumerId = requiredText(consumerIdArg, 'consumerId');
    const result = cleanupConsumerSlots(consumerId);
    if (options.json) {
      writeJson(result);
      return;
    }
    process.stdout.write(`${result.consumerId}\tslots=${result.slots.length}\n`);
    for (const slot of result.slots) {
      process.stdout.write(`${slot.slotId}\t${slot.repoName}\t${slot.state}\t${slot.slotPath}\n`);
    }
  });

program
  .command('gc-pool')
  .description('Remove free pooled worktrees that have been unused longer than the requested threshold')
  .argument('[nickname]')
  .requiredOption('--older-than-hours <hours>', 'Only remove free pooled worktrees unused longer than this many hours')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string | undefined, options: { olderThanHours: string; json?: boolean }) => {
    const nickname = String(nicknameArg || '').trim() || undefined;
    const olderThanHours = Number(options.olderThanHours);
    const result = gcPoolWorktrees(olderThanHours, nickname);
    if (options.json) {
      writeJson(result);
      return;
    }
    process.stdout.write(`removed=${result.removed.length} skipped=${result.skipped.length}\n`);
  });

program
  .command('purge-tree')
  .description('Remove run workspaces and unregister them from the local database')
  .argument('[nickname]')
  .option('--force', 'Also purge active runs')
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string | undefined, options: { force?: boolean; json?: boolean }) => {
    const nickname = String(nicknameArg || '').trim() || undefined;
    const result = purgeTrees(nickname, options.force === true);
    if (options.json) {
      writeJson(result);
      return;
    }
    process.stdout.write(`purged=${result.purgedRunIds.length} skipped=${result.skippedRunIds.length}\n`);
  });

try {
  await program.parseAsync(process.argv);
} catch (error: unknown) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
}
