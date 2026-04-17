import { existsSync, mkdirSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

import { resolveAppDir, resolveDatabasePath } from './paths.js';
import type { ProjectRow, RepositoryRow, RunRow, RunStatus, RunWorktreeRow } from './types.js';

function nowIso(): string {
  return new Date().toISOString();
}

function mapProjectRow(row: { nickname: string; base_dir: string; created_at: string; updated_at: string }): ProjectRow {
  return {
    nickname: row.nickname,
    baseDir: row.base_dir,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapRepositoryRow(row: {
  id: number;
  nickname: string;
  name: string;
  local_path: string;
  remote_url: string;
  default_branch: string;
  is_primary: number;
  created_at: string;
  updated_at: string;
}): RepositoryRow {
  return {
    id: row.id,
    nickname: row.nickname,
    name: row.name,
    localPath: row.local_path,
    remoteUrl: row.remote_url,
    defaultBranch: row.default_branch,
    isPrimary: row.is_primary === 1,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapRunRow(row: {
  run_id: string;
  nickname: string;
  workspace_root: string;
  status: string;
  created_at: string;
  updated_at: string;
  finished_at: string | null;
}): RunRow {
  return {
    runId: row.run_id,
    nickname: row.nickname,
    workspaceRoot: row.workspace_root,
    status: row.status as RunStatus,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    finishedAt: row.finished_at,
  };
}

function mapRunWorktreeRow(row: {
  id: number;
  run_id: string;
  repo_name: string;
  worktree_path: string;
  branch_name: string;
  is_primary: number;
  created_at: string;
  updated_at: string;
}): RunWorktreeRow {
  return {
    id: row.id,
    runId: row.run_id,
    repoName: row.repo_name,
    worktreePath: row.worktree_path,
    branchName: row.branch_name,
    isPrimary: row.is_primary === 1,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function openDb(): DatabaseSync {
  const appDir = resolveAppDir();
  if (!existsSync(appDir)) {
    mkdirSync(appDir, { recursive: true });
  }
  const db = new DatabaseSync(resolveDatabasePath());
  db.exec('PRAGMA journal_mode = WAL;');
  db.exec('PRAGMA busy_timeout = 5000;');
  db.exec('PRAGMA foreign_keys = ON;');
  db.exec(`
    CREATE TABLE IF NOT EXISTS projects (
      nickname TEXT PRIMARY KEY,
      base_dir TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS repositories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nickname TEXT NOT NULL,
      name TEXT NOT NULL,
      local_path TEXT NOT NULL,
      remote_url TEXT NOT NULL DEFAULT '',
      default_branch TEXT NOT NULL DEFAULT 'main',
      is_primary INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(nickname, name),
      FOREIGN KEY(nickname) REFERENCES projects(nickname) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS runs (
      run_id TEXT PRIMARY KEY,
      nickname TEXT NOT NULL,
      workspace_root TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      finished_at TEXT,
      FOREIGN KEY(nickname) REFERENCES projects(nickname) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS run_worktrees (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id TEXT NOT NULL,
      repo_name TEXT NOT NULL,
      worktree_path TEXT NOT NULL,
      branch_name TEXT NOT NULL,
      is_primary INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(run_id, repo_name),
      FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
    );
  `);

  const repositoryColumns = db.prepare('PRAGMA table_info(repositories)').all() as Array<{ name: string }>;
  const hasPrimaryColumn = repositoryColumns.some((column) => column.name === 'is_primary');
  if (!hasPrimaryColumn) {
    db.exec('ALTER TABLE repositories ADD COLUMN is_primary INTEGER NOT NULL DEFAULT 0;');
  }

  return db;
}

export function upsertProject(nickname: string, baseDir: string): ProjectRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    INSERT INTO projects (nickname, base_dir, created_at, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(nickname) DO UPDATE SET
      base_dir = excluded.base_dir,
      updated_at = excluded.updated_at
  `).run(nickname, baseDir, timestamp, timestamp);

  const row = db.prepare(`
    SELECT nickname, base_dir, created_at, updated_at
    FROM projects
    WHERE nickname = ?
  `).get(nickname) as { nickname: string; base_dir: string; created_at: string; updated_at: string };

  return mapProjectRow(row);
}

export function getProject(nickname: string): ProjectRow | null {
  const db = openDb();
  const row = db.prepare(`
    SELECT nickname, base_dir, created_at, updated_at
    FROM projects
    WHERE nickname = ?
  `).get(nickname) as { nickname: string; base_dir: string; created_at: string; updated_at: string } | undefined;
  return row ? mapProjectRow(row) : null;
}

function countRepositories(db: DatabaseSync, nickname: string): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS total
    FROM repositories
    WHERE nickname = ?
  `).get(nickname) as { total: number };
  return Number(row.total || 0);
}

export function upsertRepository(input: {
  nickname: string;
  name: string;
  localPath: string;
  remoteUrl: string;
  defaultBranch: string;
  isPrimary: boolean;
}): RepositoryRow {
  const db = openDb();
  const timestamp = nowIso();
  const shouldBePrimary = input.isPrimary || countRepositories(db, input.nickname) === 0;

  if (shouldBePrimary) {
    db.prepare('UPDATE repositories SET is_primary = 0 WHERE nickname = ?').run(input.nickname);
  }

  db.prepare(`
    INSERT INTO repositories (
      nickname,
      name,
      local_path,
      remote_url,
      default_branch,
      is_primary,
      created_at,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(nickname, name) DO UPDATE SET
      local_path = excluded.local_path,
      remote_url = excluded.remote_url,
      default_branch = excluded.default_branch,
      is_primary = excluded.is_primary,
      updated_at = excluded.updated_at
  `).run(
    input.nickname,
    input.name,
    input.localPath,
    input.remoteUrl,
    input.defaultBranch,
    shouldBePrimary ? 1 : 0,
    timestamp,
    timestamp,
  );

  const row = db.prepare(`
    SELECT id, nickname, name, local_path, remote_url, default_branch, is_primary, created_at, updated_at
    FROM repositories
    WHERE nickname = ? AND name = ?
  `).get(input.nickname, input.name) as {
    id: number;
    nickname: string;
    name: string;
    local_path: string;
    remote_url: string;
    default_branch: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  };

  return mapRepositoryRow(row);
}

export function getRepository(nickname: string, name: string): RepositoryRow | null {
  const db = openDb();
  const row = db.prepare(`
    SELECT id, nickname, name, local_path, remote_url, default_branch, is_primary, created_at, updated_at
    FROM repositories
    WHERE nickname = ? AND name = ?
  `).get(nickname, name) as {
    id: number;
    nickname: string;
    name: string;
    local_path: string;
    remote_url: string;
    default_branch: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  } | undefined;
  return row ? mapRepositoryRow(row) : null;
}

export function getPrimaryRepository(nickname: string): RepositoryRow | null {
  const db = openDb();
  const row = db.prepare(`
    SELECT id, nickname, name, local_path, remote_url, default_branch, is_primary, created_at, updated_at
    FROM repositories
    WHERE nickname = ? AND is_primary = 1
    LIMIT 1
  `).get(nickname) as {
    id: number;
    nickname: string;
    name: string;
    local_path: string;
    remote_url: string;
    default_branch: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  } | undefined;
  return row ? mapRepositoryRow(row) : null;
}

export function listRepositories(nickname: string): RepositoryRow[] {
  const db = openDb();
  const rows = db.prepare(`
    SELECT id, nickname, name, local_path, remote_url, default_branch, is_primary, created_at, updated_at
    FROM repositories
    WHERE nickname = ?
    ORDER BY is_primary DESC, name ASC
  `).all(nickname) as Array<{
    id: number;
    nickname: string;
    name: string;
    local_path: string;
    remote_url: string;
    default_branch: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  }>;

  return rows.map(mapRepositoryRow);
}

export function createRun(input: {
  runId: string;
  nickname: string;
  workspaceRoot: string;
}): RunRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    INSERT INTO runs (run_id, nickname, workspace_root, status, created_at, updated_at, finished_at)
    VALUES (?, ?, ?, 'ACTIVE', ?, ?, NULL)
  `).run(input.runId, input.nickname, input.workspaceRoot, timestamp, timestamp);

  const row = db.prepare(`
    SELECT run_id, nickname, workspace_root, status, created_at, updated_at, finished_at
    FROM runs
    WHERE run_id = ?
  `).get(input.runId) as {
    run_id: string;
    nickname: string;
    workspace_root: string;
    status: string;
    created_at: string;
    updated_at: string;
    finished_at: string | null;
  };
  return mapRunRow(row);
}

export function getRun(runId: string): RunRow | null {
  const db = openDb();
  const row = db.prepare(`
    SELECT run_id, nickname, workspace_root, status, created_at, updated_at, finished_at
    FROM runs
    WHERE run_id = ?
  `).get(runId) as {
    run_id: string;
    nickname: string;
    workspace_root: string;
    status: string;
    created_at: string;
    updated_at: string;
    finished_at: string | null;
  } | undefined;
  return row ? mapRunRow(row) : null;
}

export function listRuns(nickname?: string): RunRow[] {
  const db = openDb();
  const rows = nickname
    ? db.prepare(`
        SELECT run_id, nickname, workspace_root, status, created_at, updated_at, finished_at
        FROM runs
        WHERE nickname = ?
        ORDER BY created_at ASC
      `).all(nickname)
    : db.prepare(`
        SELECT run_id, nickname, workspace_root, status, created_at, updated_at, finished_at
        FROM runs
        ORDER BY created_at ASC
      `).all();

  return (rows as Array<{
    run_id: string;
    nickname: string;
    workspace_root: string;
    status: string;
    created_at: string;
    updated_at: string;
    finished_at: string | null;
  }>).map(mapRunRow);
}

export function updateRunStatus(runId: string, status: RunStatus): RunRow {
  const db = openDb();
  const timestamp = nowIso();
  const finishedAt = status === 'ACTIVE' ? null : timestamp;
  db.prepare(`
    UPDATE runs
    SET status = ?, updated_at = ?, finished_at = ?
    WHERE run_id = ?
  `).run(status, timestamp, finishedAt, runId);

  const row = db.prepare(`
    SELECT run_id, nickname, workspace_root, status, created_at, updated_at, finished_at
    FROM runs
    WHERE run_id = ?
  `).get(runId) as {
    run_id: string;
    nickname: string;
    workspace_root: string;
    status: string;
    created_at: string;
    updated_at: string;
    finished_at: string | null;
  };
  return mapRunRow(row);
}

export function addRunWorktree(input: {
  runId: string;
  repoName: string;
  worktreePath: string;
  branchName: string;
  isPrimary: boolean;
}): RunWorktreeRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    INSERT INTO run_worktrees (
      run_id,
      repo_name,
      worktree_path,
      branch_name,
      is_primary,
      created_at,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, repo_name) DO UPDATE SET
      worktree_path = excluded.worktree_path,
      branch_name = excluded.branch_name,
      is_primary = excluded.is_primary,
      updated_at = excluded.updated_at
  `).run(
    input.runId,
    input.repoName,
    input.worktreePath,
    input.branchName,
    input.isPrimary ? 1 : 0,
    timestamp,
    timestamp,
  );

  const row = db.prepare(`
    SELECT id, run_id, repo_name, worktree_path, branch_name, is_primary, created_at, updated_at
    FROM run_worktrees
    WHERE run_id = ? AND repo_name = ?
  `).get(input.runId, input.repoName) as {
    id: number;
    run_id: string;
    repo_name: string;
    worktree_path: string;
    branch_name: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  };
  return mapRunWorktreeRow(row);
}

export function getRunWorktree(runId: string, repoName: string): RunWorktreeRow | null {
  const db = openDb();
  const row = db.prepare(`
    SELECT id, run_id, repo_name, worktree_path, branch_name, is_primary, created_at, updated_at
    FROM run_worktrees
    WHERE run_id = ? AND repo_name = ?
  `).get(runId, repoName) as {
    id: number;
    run_id: string;
    repo_name: string;
    worktree_path: string;
    branch_name: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  } | undefined;
  return row ? mapRunWorktreeRow(row) : null;
}

export function listRunWorktrees(runId: string): RunWorktreeRow[] {
  const db = openDb();
  const rows = db.prepare(`
    SELECT id, run_id, repo_name, worktree_path, branch_name, is_primary, created_at, updated_at
    FROM run_worktrees
    WHERE run_id = ?
    ORDER BY is_primary DESC, repo_name ASC
  `).all(runId) as Array<{
    id: number;
    run_id: string;
    repo_name: string;
    worktree_path: string;
    branch_name: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  }>;
  return rows.map(mapRunWorktreeRow);
}

export function deleteRun(runId: string): void {
  const db = openDb();
  db.prepare('DELETE FROM runs WHERE run_id = ?').run(runId);
}
