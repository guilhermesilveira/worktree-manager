#!/usr/bin/env node

import { existsSync } from 'node:fs';

import { Command } from 'commander';

import { getProject, listRepositories, upsertProject, upsertRepository } from './db.js';
import { assertGitRepo, detectDefaultBranch, detectRemoteUrl } from './git.js';
import { resolveAbsolutePath, resolveDatabasePath } from './paths.js';
import { createNewTree, promoteRunRepository, purgeTrees, pushRunTree } from './runManager.js';
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
  .option('--json', 'Emit JSON output')
  .action((nicknameArg: string, options: { repo?: string[]; json?: boolean }) => {
    const nickname = requiredText(nicknameArg, 'nickname');
    const result = createNewTree(nickname, options.repo || []);
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
  .option('--json', 'Emit JSON output')
  .action((runIdArg: string, repoNameArg: string, options: { json?: boolean }) => {
    const runId = requiredText(runIdArg, 'runId');
    const repoName = requiredText(repoNameArg, 'repoName');
    const result = promoteRunRepository(runId, repoName);
    if (options.json) {
      writeJson(result);
      return;
    }
    process.stdout.write(`${result.run.runId}\t${result.worktree.repoName}\t${result.worktree.worktreePath}\n`);
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
