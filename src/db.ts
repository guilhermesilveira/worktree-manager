import { existsSync, mkdirSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

import { resolveAppDir, resolveDatabasePath } from './paths.js';
import type { ConsumerSlotRow, PoolStats, PoolWorktreeInspection, PoolWorktreeInspectionRun, PoolWorktreeInspectionSlot, PoolWorktreeRow, ProjectRow, RepositoryRow, RunEventLevel, RunEventRow, RunInspection, RunInspectionWorktree, RunListItem, RunRow, RunSlotRow, RunStatus, RunWorktreeRow, SlotInspection, SlotInspectionRun, SlotRow, SlotState, SlotStats, SlotStatsByNickname, SlotStatsByRepo, SlotStateCount } from './types.js';

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

function mapRunListItem(row: {
  run_id: string;
  nickname: string;
  workspace_root: string;
  status: string;
  created_at: string;
  updated_at: string;
  finished_at: string | null;
  repo_count: number;
  event_count: number;
  latest_event_type: string | null;
  latest_event_level: string | null;
  latest_event_message: string | null;
  latest_event_at: string | null;
}): RunListItem {
  return {
    runId: row.run_id,
    nickname: row.nickname,
    workspaceRoot: row.workspace_root,
    status: row.status as RunStatus,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    finishedAt: row.finished_at,
    repoCount: Number(row.repo_count || 0),
    eventCount: Number(row.event_count || 0),
    latestEventType: row.latest_event_type,
    latestEventLevel: row.latest_event_level as RunEventLevel | null,
    latestEventMessage: row.latest_event_message,
    latestEventAt: row.latest_event_at,
  };
}

function mapRunWorktreeRow(row: {
  id: number;
  run_id: string;
  repo_name: string;
  worktree_path: string;
  branch_name: string;
  pool_worktree_id: string;
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
    poolWorktreeId: row.pool_worktree_id,
    isPrimary: row.is_primary === 1,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapRunEventRow(row: {
  id: number;
  run_id: string;
  event_type: string;
  level: string;
  message: string;
  payload_json: string | null;
  created_at: string;
}): RunEventRow {
  return {
    id: row.id,
    runId: row.run_id,
    eventType: row.event_type,
    level: row.level as RunEventLevel,
    message: row.message,
    payloadJson: row.payload_json,
    createdAt: row.created_at,
  };
}

function mapSlotRow(row: {
  slot_id: string;
  nickname: string;
  repo_name: string;
  slot_path: string;
  pool_worktree_id: string;
  state: string;
  current_consumer_id: string | null;
  created_at: string;
  updated_at: string;
  last_used_at: string;
}): SlotRow {
  return {
    slotId: row.slot_id,
    nickname: row.nickname,
    repoName: row.repo_name,
    slotPath: row.slot_path,
    poolWorktreeId: row.pool_worktree_id,
    state: row.state as SlotState,
    currentConsumerId: row.current_consumer_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    lastUsedAt: row.last_used_at,
  };
}

function mapRunSlotRow(row: {
  id: number;
  run_id: string;
  slot_id: string;
  repo_name: string;
  pool_worktree_id: string;
  created_at: string;
}): RunSlotRow {
  return {
    id: row.id,
    runId: row.run_id,
    slotId: row.slot_id,
    repoName: row.repo_name,
    poolWorktreeId: row.pool_worktree_id,
    createdAt: row.created_at,
  };
}

function mapPoolWorktreeRow(row: {
  pool_worktree_id: string;
  nickname: string;
  repo_name: string;
  pool_path: string;
  state: string;
  current_consumer_id: string | null;
  created_at: string;
  updated_at: string;
  last_used_at: string;
}): PoolWorktreeRow {
  return {
    poolWorktreeId: row.pool_worktree_id,
    nickname: row.nickname,
    repoName: row.repo_name,
    poolPath: row.pool_path,
    state: row.state as SlotState,
    currentConsumerId: row.current_consumer_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    lastUsedAt: row.last_used_at,
  };
}

function mapPoolWorktreeInspectionSlot(row: {
  slot_id: string;
  slot_path: string;
  state: string;
  current_consumer_id: string | null;
  created_at: string;
}): PoolWorktreeInspectionSlot {
  return {
    slotId: row.slot_id,
    slotPath: row.slot_path,
    state: row.state as SlotState,
    currentConsumerId: row.current_consumer_id,
    createdAt: row.created_at,
  };
}

function mapPoolWorktreeInspectionRun(row: {
  run_id: string;
  status: string;
  workspace_root: string;
  repo_name: string;
  branch_name: string | null;
  slot_id: string | null;
  slot_path: string | null;
  created_at: string;
}): PoolWorktreeInspectionRun {
  return {
    runId: row.run_id,
    status: row.status as RunStatus,
    workspaceRoot: row.workspace_root,
    repoName: row.repo_name,
    branchName: row.branch_name,
    slotId: row.slot_id,
    slotPath: row.slot_path,
    createdAt: row.created_at,
  };
}

function mapConsumerSlotRow(row: {
  id: number;
  consumer_id: string;
  slot_id: string;
  created_at: string;
  updated_at: string;
}): ConsumerSlotRow {
  return {
    id: row.id,
    consumerId: row.consumer_id,
    slotId: row.slot_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapSlotInspectionRun(row: {
  run_id: string;
  status: string;
  workspace_root: string;
  repo_name: string;
  pool_worktree_id: string;
  branch_name: string | null;
  is_primary: number | null;
  created_at: string;
}): SlotInspectionRun {
  return {
    runId: row.run_id,
    status: row.status as RunStatus,
    workspaceRoot: row.workspace_root,
    repoName: row.repo_name,
    poolWorktreeId: row.pool_worktree_id,
    branchName: row.branch_name,
    isPrimary: row.is_primary === null ? null : row.is_primary === 1,
    createdAt: row.created_at,
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
      pool_worktree_id TEXT NOT NULL DEFAULT '',
      is_primary INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(run_id, repo_name),
      FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS slots (
      slot_id TEXT PRIMARY KEY,
      nickname TEXT NOT NULL,
      repo_name TEXT NOT NULL,
      slot_path TEXT NOT NULL,
      pool_worktree_id TEXT NOT NULL DEFAULT '',
      state TEXT NOT NULL,
      current_consumer_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_used_at TEXT NOT NULL,
      FOREIGN KEY(nickname) REFERENCES projects(nickname) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS run_slots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id TEXT NOT NULL,
      slot_id TEXT NOT NULL,
      repo_name TEXT NOT NULL,
      pool_worktree_id TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL,
      UNIQUE(run_id, slot_id),
      UNIQUE(run_id, repo_name),
      FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE,
      FOREIGN KEY(slot_id) REFERENCES slots(slot_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS consumer_slots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      consumer_id TEXT NOT NULL,
      slot_id TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(consumer_id, slot_id),
      FOREIGN KEY(slot_id) REFERENCES slots(slot_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS pool_worktrees (
      pool_worktree_id TEXT PRIMARY KEY,
      nickname TEXT NOT NULL,
      repo_name TEXT NOT NULL,
      pool_path TEXT NOT NULL,
      state TEXT NOT NULL,
      current_consumer_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_used_at TEXT NOT NULL,
      FOREIGN KEY(nickname) REFERENCES projects(nickname) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS run_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      level TEXT NOT NULL,
      message TEXT NOT NULL,
      payload_json TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
    );
  `);

  const repositoryColumns = db.prepare('PRAGMA table_info(repositories)').all() as Array<{ name: string }>;
  const hasPrimaryColumn = repositoryColumns.some((column) => column.name === 'is_primary');
  if (!hasPrimaryColumn) {
    db.exec('ALTER TABLE repositories ADD COLUMN is_primary INTEGER NOT NULL DEFAULT 0;');
  }

  const runWorktreeColumns = db.prepare('PRAGMA table_info(run_worktrees)').all() as Array<{ name: string }>;
  if (!runWorktreeColumns.some((column) => column.name === 'pool_worktree_id')) {
    db.exec("ALTER TABLE run_worktrees ADD COLUMN pool_worktree_id TEXT NOT NULL DEFAULT '';");
  }

  const slotColumns = db.prepare('PRAGMA table_info(slots)').all() as Array<{ name: string }>;
  if (!slotColumns.some((column) => column.name === 'pool_worktree_id')) {
    db.exec("ALTER TABLE slots ADD COLUMN pool_worktree_id TEXT NOT NULL DEFAULT '';");
  }

  const runSlotColumns = db.prepare('PRAGMA table_info(run_slots)').all() as Array<{ name: string }>;
  if (!runSlotColumns.some((column) => column.name === 'pool_worktree_id')) {
    db.exec("ALTER TABLE run_slots ADD COLUMN pool_worktree_id TEXT NOT NULL DEFAULT '';");
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

export function listRunSummaries(nickname?: string): RunListItem[] {
  const db = openDb();
  const rows = nickname
    ? db.prepare(`
        SELECT
          runs.run_id,
          runs.nickname,
          runs.workspace_root,
          runs.status,
          runs.created_at,
          runs.updated_at,
          runs.finished_at,
          COUNT(DISTINCT run_worktrees.id) AS repo_count,
          COUNT(DISTINCT run_events.id) AS event_count,
          (
            SELECT event_type FROM run_events re
            WHERE re.run_id = runs.run_id
            ORDER BY re.created_at DESC, re.id DESC
            LIMIT 1
          ) AS latest_event_type,
          (
            SELECT level FROM run_events re
            WHERE re.run_id = runs.run_id
            ORDER BY re.created_at DESC, re.id DESC
            LIMIT 1
          ) AS latest_event_level,
          (
            SELECT message FROM run_events re
            WHERE re.run_id = runs.run_id
            ORDER BY re.created_at DESC, re.id DESC
            LIMIT 1
          ) AS latest_event_message,
          (
            SELECT created_at FROM run_events re
            WHERE re.run_id = runs.run_id
            ORDER BY re.created_at DESC, re.id DESC
            LIMIT 1
          ) AS latest_event_at
        FROM runs
        LEFT JOIN run_worktrees ON run_worktrees.run_id = runs.run_id
        LEFT JOIN run_events ON run_events.run_id = runs.run_id
        WHERE runs.nickname = ?
        GROUP BY runs.run_id
        ORDER BY runs.created_at DESC
      `).all(nickname)
    : db.prepare(`
        SELECT
          runs.run_id,
          runs.nickname,
          runs.workspace_root,
          runs.status,
          runs.created_at,
          runs.updated_at,
          runs.finished_at,
          COUNT(DISTINCT run_worktrees.id) AS repo_count,
          COUNT(DISTINCT run_events.id) AS event_count,
          (
            SELECT event_type FROM run_events re
            WHERE re.run_id = runs.run_id
            ORDER BY re.created_at DESC, re.id DESC
            LIMIT 1
          ) AS latest_event_type,
          (
            SELECT level FROM run_events re
            WHERE re.run_id = runs.run_id
            ORDER BY re.created_at DESC, re.id DESC
            LIMIT 1
          ) AS latest_event_level,
          (
            SELECT message FROM run_events re
            WHERE re.run_id = runs.run_id
            ORDER BY re.created_at DESC, re.id DESC
            LIMIT 1
          ) AS latest_event_message,
          (
            SELECT created_at FROM run_events re
            WHERE re.run_id = runs.run_id
            ORDER BY re.created_at DESC, re.id DESC
            LIMIT 1
          ) AS latest_event_at
        FROM runs
        LEFT JOIN run_worktrees ON run_worktrees.run_id = runs.run_id
        LEFT JOIN run_events ON run_events.run_id = runs.run_id
        GROUP BY runs.run_id
        ORDER BY runs.created_at DESC
      `).all();

  return (rows as Array<{
    run_id: string;
    nickname: string;
    workspace_root: string;
    status: string;
    created_at: string;
    updated_at: string;
    finished_at: string | null;
    repo_count: number;
    event_count: number;
    latest_event_type: string | null;
    latest_event_level: string | null;
    latest_event_message: string | null;
    latest_event_at: string | null;
  }>).map(mapRunListItem);
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

export function addRunEvent(input: {
  runId: string;
  eventType: string;
  level?: RunEventLevel;
  message: string;
  payloadJson?: string | null;
}): RunEventRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    INSERT INTO run_events (run_id, event_type, level, message, payload_json, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(
    input.runId,
    input.eventType,
    input.level || 'INFO',
    input.message,
    input.payloadJson || null,
    timestamp,
  );
  const row = db.prepare(`
    SELECT id, run_id, event_type, level, message, payload_json, created_at
    FROM run_events
    WHERE id = last_insert_rowid()
  `).get() as {
    id: number;
    run_id: string;
    event_type: string;
    level: string;
    message: string;
    payload_json: string | null;
    created_at: string;
  };
  return mapRunEventRow(row);
}

export function listRunEvents(runId: string): RunEventRow[] {
  const db = openDb();
  const rows = db.prepare(`
    SELECT id, run_id, event_type, level, message, payload_json, created_at
    FROM run_events
    WHERE run_id = ?
    ORDER BY created_at ASC, id ASC
  `).all(runId) as Array<{
    id: number;
    run_id: string;
    event_type: string;
    level: string;
    message: string;
    payload_json: string | null;
    created_at: string;
  }>;
  return rows.map(mapRunEventRow);
}

export function addRunWorktree(input: {
  runId: string;
  repoName: string;
  worktreePath: string;
  branchName: string;
  poolWorktreeId: string;
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
      pool_worktree_id,
      is_primary,
      created_at,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, repo_name) DO UPDATE SET
      worktree_path = excluded.worktree_path,
      branch_name = excluded.branch_name,
      pool_worktree_id = excluded.pool_worktree_id,
      is_primary = excluded.is_primary,
      updated_at = excluded.updated_at
  `).run(
    input.runId,
    input.repoName,
    input.worktreePath,
    input.branchName,
    input.poolWorktreeId,
    input.isPrimary ? 1 : 0,
    timestamp,
    timestamp,
  );

  const row = db.prepare(`
    SELECT id, run_id, repo_name, worktree_path, branch_name, pool_worktree_id, is_primary, created_at, updated_at
    FROM run_worktrees
    WHERE run_id = ? AND repo_name = ?
  `).get(input.runId, input.repoName) as {
    id: number;
    run_id: string;
    repo_name: string;
    worktree_path: string;
    branch_name: string;
    pool_worktree_id: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  };
  return mapRunWorktreeRow(row);
}

export function getRunWorktree(runId: string, repoName: string): RunWorktreeRow | null {
  const db = openDb();
  const row = db.prepare(`
    SELECT id, run_id, repo_name, worktree_path, branch_name, pool_worktree_id, is_primary, created_at, updated_at
    FROM run_worktrees
    WHERE run_id = ? AND repo_name = ?
  `).get(runId, repoName) as {
    id: number;
    run_id: string;
    repo_name: string;
    worktree_path: string;
    branch_name: string;
    pool_worktree_id: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  } | undefined;
  return row ? mapRunWorktreeRow(row) : null;
}

export function listRunWorktrees(runId: string): RunWorktreeRow[] {
  const db = openDb();
  const rows = db.prepare(`
    SELECT id, run_id, repo_name, worktree_path, branch_name, pool_worktree_id, is_primary, created_at, updated_at
    FROM run_worktrees
    WHERE run_id = ?
    ORDER BY is_primary DESC, repo_name ASC
  `).all(runId) as Array<{
    id: number;
    run_id: string;
    repo_name: string;
    worktree_path: string;
    branch_name: string;
    pool_worktree_id: string;
    is_primary: number;
    created_at: string;
    updated_at: string;
  }>;
  return rows.map(mapRunWorktreeRow);
}

export function inspectRun(runId: string): RunInspection | null {
  const run = getRun(runId);
  if (!run) return null;
  const worktrees = listRunWorktrees(runId).map((worktree): RunInspectionWorktree => {
    const runSlot = listRunSlots(runId).find((entry) => entry.repoName === worktree.repoName);
    const slot = runSlot ? getSlot(runSlot.slotId) : null;
    const poolWorktree = getPoolWorktree(worktree.poolWorktreeId);
    return {
      ...worktree,
      slot,
      poolWorktree,
    };
  });
  const events = listRunEvents(runId);
  return { run, worktrees, events };
}

export function deleteRun(runId: string): void {
  const db = openDb();
  db.prepare('DELETE FROM runs WHERE run_id = ?').run(runId);
}

export function createSlot(input: {
  slotId: string;
  nickname: string;
  repoName: string;
  slotPath: string;
  poolWorktreeId: string;
  state: SlotState;
  currentConsumerId?: string;
}): SlotRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    INSERT INTO slots (
      slot_id,
      nickname,
      repo_name,
      slot_path,
      pool_worktree_id,
      state,
      current_consumer_id,
      created_at,
      updated_at,
      last_used_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(slot_id) DO UPDATE SET
      nickname = excluded.nickname,
      repo_name = excluded.repo_name,
      slot_path = excluded.slot_path,
      pool_worktree_id = excluded.pool_worktree_id,
      state = excluded.state,
      current_consumer_id = excluded.current_consumer_id,
      updated_at = excluded.updated_at,
      last_used_at = excluded.last_used_at
  `).run(
    input.slotId,
    input.nickname,
    input.repoName,
    input.slotPath,
    input.poolWorktreeId,
    input.state,
    input.currentConsumerId ?? null,
    timestamp,
    timestamp,
    timestamp,
  );

  const row = db.prepare(`
    SELECT slot_id, nickname, repo_name, slot_path, pool_worktree_id, state, current_consumer_id, created_at, updated_at, last_used_at
    FROM slots
    WHERE slot_id = ?
  `).get(input.slotId) as {
    slot_id: string;
    nickname: string;
    repo_name: string;
    slot_path: string;
    pool_worktree_id: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  };
  return mapSlotRow(row);
}

export function createPoolWorktree(input: {
  poolWorktreeId: string;
  nickname: string;
  repoName: string;
  poolPath: string;
  state: SlotState;
  currentConsumerId?: string;
}): PoolWorktreeRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    INSERT INTO pool_worktrees (
      pool_worktree_id,
      nickname,
      repo_name,
      pool_path,
      state,
      current_consumer_id,
      created_at,
      updated_at,
      last_used_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(pool_worktree_id) DO UPDATE SET
      nickname = excluded.nickname,
      repo_name = excluded.repo_name,
      pool_path = excluded.pool_path,
      state = excluded.state,
      current_consumer_id = excluded.current_consumer_id,
      updated_at = excluded.updated_at,
      last_used_at = excluded.last_used_at
  `).run(
    input.poolWorktreeId,
    input.nickname,
    input.repoName,
    input.poolPath,
    input.state,
    input.currentConsumerId ?? null,
    timestamp,
    timestamp,
    timestamp,
  );

  const row = db.prepare(`
    SELECT pool_worktree_id, nickname, repo_name, pool_path, state, current_consumer_id, created_at, updated_at, last_used_at
    FROM pool_worktrees
    WHERE pool_worktree_id = ?
  `).get(input.poolWorktreeId) as {
    pool_worktree_id: string;
    nickname: string;
    repo_name: string;
    pool_path: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  };
  return mapPoolWorktreeRow(row);
}

export function getPoolWorktree(poolWorktreeId: string): PoolWorktreeRow | null {
  const db = openDb();
  const row = db.prepare(`
    SELECT pool_worktree_id, nickname, repo_name, pool_path, state, current_consumer_id, created_at, updated_at, last_used_at
    FROM pool_worktrees
    WHERE pool_worktree_id = ?
  `).get(poolWorktreeId) as {
    pool_worktree_id: string;
    nickname: string;
    repo_name: string;
    pool_path: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  } | undefined;
  return row ? mapPoolWorktreeRow(row) : null;
}

export function listPoolWorktrees(nickname?: string): PoolWorktreeRow[] {
  const db = openDb();
  const rows = nickname
    ? db.prepare(`
        SELECT pool_worktree_id, nickname, repo_name, pool_path, state, current_consumer_id, created_at, updated_at, last_used_at
        FROM pool_worktrees
        WHERE nickname = ?
        ORDER BY nickname ASC, repo_name ASC, pool_worktree_id ASC
      `).all(nickname)
    : db.prepare(`
        SELECT pool_worktree_id, nickname, repo_name, pool_path, state, current_consumer_id, created_at, updated_at, last_used_at
        FROM pool_worktrees
        ORDER BY nickname ASC, repo_name ASC, pool_worktree_id ASC
      `).all();

  return (rows as Array<{
    pool_worktree_id: string;
    nickname: string;
    repo_name: string;
    pool_path: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  }>).map(mapPoolWorktreeRow);
}

export function inspectPoolWorktree(poolWorktreeId: string): PoolWorktreeInspection | null {
  const poolWorktree = getPoolWorktree(poolWorktreeId);
  if (!poolWorktree) {
    return null;
  }
  const db = openDb();
  const slotRows = db.prepare(`
    SELECT slot_id, slot_path, state, current_consumer_id, created_at
    FROM slots
    WHERE pool_worktree_id = ?
    ORDER BY created_at DESC, slot_id DESC
  `).all(poolWorktreeId) as Array<{
    slot_id: string;
    slot_path: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
  }>;
  const runRows = db.prepare(`
    SELECT
      runs.run_id,
      runs.status,
      runs.workspace_root,
      run_worktrees.repo_name,
      run_worktrees.branch_name,
      slots.slot_id,
      slots.slot_path,
      run_worktrees.created_at
    FROM run_worktrees
    JOIN runs ON runs.run_id = run_worktrees.run_id
    LEFT JOIN slots
      ON slots.pool_worktree_id = run_worktrees.pool_worktree_id
      AND slots.repo_name = run_worktrees.repo_name
    WHERE run_worktrees.pool_worktree_id = ?
    ORDER BY run_worktrees.created_at DESC, runs.run_id DESC
  `).all(poolWorktreeId) as Array<{
    run_id: string;
    status: string;
    workspace_root: string;
    repo_name: string;
    branch_name: string | null;
    slot_id: string | null;
    slot_path: string | null;
    created_at: string;
  }>;

  return {
    poolWorktree,
    slots: slotRows.map(mapPoolWorktreeInspectionSlot),
    runs: runRows.map(mapPoolWorktreeInspectionRun),
  };
}

export function findReusablePoolWorktree(nickname: string, repoName: string): PoolWorktreeRow | null {
  const db = openDb();
  const row = db.prepare(`
    SELECT pool_worktree_id, nickname, repo_name, pool_path, state, current_consumer_id, created_at, updated_at, last_used_at
    FROM pool_worktrees
    WHERE nickname = ? AND repo_name = ? AND state = 'FREE'
    ORDER BY last_used_at ASC, pool_worktree_id ASC
    LIMIT 1
  `).get(nickname, repoName) as {
    pool_worktree_id: string;
    nickname: string;
    repo_name: string;
    pool_path: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  } | undefined;
  return row ? mapPoolWorktreeRow(row) : null;
}

export function listStaleFreePoolWorktrees(olderThanIso: string, nickname?: string): PoolWorktreeRow[] {
  const db = openDb();
  const rows = nickname
    ? db.prepare(`
        SELECT pool_worktree_id, nickname, repo_name, pool_path, state, current_consumer_id, created_at, updated_at, last_used_at
        FROM pool_worktrees
        WHERE nickname = ? AND state = 'FREE' AND last_used_at < ?
        ORDER BY last_used_at ASC, pool_worktree_id ASC
      `).all(nickname, olderThanIso)
    : db.prepare(`
        SELECT pool_worktree_id, nickname, repo_name, pool_path, state, current_consumer_id, created_at, updated_at, last_used_at
        FROM pool_worktrees
        WHERE state = 'FREE' AND last_used_at < ?
        ORDER BY last_used_at ASC, pool_worktree_id ASC
      `).all(olderThanIso);

  return (rows as Array<{
    pool_worktree_id: string;
    nickname: string;
    repo_name: string;
    pool_path: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  }>).map(mapPoolWorktreeRow);
}

export function getSlot(slotId: string): SlotRow | null {
  const db = openDb();
  const row = db.prepare(`
    SELECT slot_id, nickname, repo_name, slot_path, pool_worktree_id, state, current_consumer_id, created_at, updated_at, last_used_at
    FROM slots
    WHERE slot_id = ?
  `).get(slotId) as {
    slot_id: string;
    nickname: string;
    repo_name: string;
    slot_path: string;
    pool_worktree_id: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  } | undefined;
  return row ? mapSlotRow(row) : null;
}

export function listSlots(nickname?: string): SlotRow[] {
  const db = openDb();
  const rows = nickname
    ? db.prepare(`
        SELECT slot_id, nickname, repo_name, slot_path, pool_worktree_id, state, current_consumer_id, created_at, updated_at, last_used_at
        FROM slots
        WHERE nickname = ?
        ORDER BY nickname ASC, repo_name ASC, slot_id ASC
      `).all(nickname)
    : db.prepare(`
        SELECT slot_id, nickname, repo_name, slot_path, pool_worktree_id, state, current_consumer_id, created_at, updated_at, last_used_at
        FROM slots
        ORDER BY nickname ASC, repo_name ASC, slot_id ASC
      `).all();

  return (rows as Array<{
    slot_id: string;
    nickname: string;
    repo_name: string;
    slot_path: string;
    pool_worktree_id: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  }>).map(mapSlotRow);
}

export function inspectSlot(slotId: string): SlotInspection | null {
  const slot = getSlot(slotId);
  if (!slot) {
    return null;
  }
  const db = openDb();
  const poolWorktree = getPoolWorktree(slot.poolWorktreeId);
  const runRows = db.prepare(`
    SELECT
      runs.run_id,
      runs.status,
      runs.workspace_root,
      run_slots.repo_name,
      run_slots.pool_worktree_id,
      run_worktrees.branch_name,
      run_worktrees.is_primary,
      run_slots.created_at
    FROM run_slots
    JOIN runs ON runs.run_id = run_slots.run_id
    LEFT JOIN run_worktrees
      ON run_worktrees.run_id = run_slots.run_id
      AND run_worktrees.repo_name = run_slots.repo_name
    WHERE run_slots.slot_id = ?
    ORDER BY run_slots.created_at DESC, runs.run_id DESC
  `).all(slotId) as Array<{
    run_id: string;
    status: string;
    workspace_root: string;
    repo_name: string;
    pool_worktree_id: string;
    branch_name: string | null;
    is_primary: number | null;
    created_at: string;
  }>;

  const consumerRows = db.prepare(`
    SELECT id, consumer_id, slot_id, created_at, updated_at
    FROM consumer_slots
    WHERE slot_id = ?
    ORDER BY updated_at DESC, id DESC
  `).all(slotId) as Array<{
    id: number;
    consumer_id: string;
    slot_id: string;
    created_at: string;
    updated_at: string;
  }>;

  return {
    slot,
    poolWorktree,
    runs: runRows.map(mapSlotInspectionRun),
    consumers: consumerRows.map(mapConsumerSlotRow),
  };
}

function mapSlotStateCount(row: { state: string; total: number }): SlotStateCount {
  return {
    state: row.state as SlotState,
    total: Number(row.total || 0),
  };
}

function mapSlotStatsByNickname(row: {
  nickname: string;
  total: number;
  free: number;
  busy: number;
  dirty: number;
  broken: number;
}): SlotStatsByNickname {
  return {
    nickname: row.nickname,
    total: Number(row.total || 0),
    free: Number(row.free || 0),
    busy: Number(row.busy || 0),
    dirty: Number(row.dirty || 0),
    broken: Number(row.broken || 0),
  };
}

function mapSlotStatsByRepo(row: {
  nickname: string;
  repo_name: string;
  total: number;
  free: number;
  busy: number;
  dirty: number;
  broken: number;
}): SlotStatsByRepo {
  return {
    nickname: row.nickname,
    repoName: row.repo_name,
    total: Number(row.total || 0),
    free: Number(row.free || 0),
    busy: Number(row.busy || 0),
    dirty: Number(row.dirty || 0),
    broken: Number(row.broken || 0),
  };
}

export function getSlotStats(nickname?: string): SlotStats {
  const db = openDb();
  const filterClause = nickname ? 'WHERE nickname = ?' : '';
  const params = nickname ? [nickname] : [];

  const totalRow = db.prepare(`
    SELECT COUNT(*) AS total
    FROM slots
    ${filterClause}
  `).get(...params) as { total: number };

  const byStateRows = db.prepare(`
    SELECT state, COUNT(*) AS total
    FROM slots
    ${filterClause}
    GROUP BY state
    ORDER BY state ASC
  `).all(...params) as Array<{ state: string; total: number }>;

  const byNicknameRows = db.prepare(`
    SELECT
      nickname,
      COUNT(*) AS total,
      SUM(CASE WHEN state = 'FREE' THEN 1 ELSE 0 END) AS free,
      SUM(CASE WHEN state = 'BUSY' THEN 1 ELSE 0 END) AS busy,
      SUM(CASE WHEN state = 'DIRTY' THEN 1 ELSE 0 END) AS dirty,
      SUM(CASE WHEN state = 'BROKEN' THEN 1 ELSE 0 END) AS broken
    FROM slots
    ${filterClause}
    GROUP BY nickname
    ORDER BY nickname ASC
  `).all(...params) as Array<{
    nickname: string;
    total: number;
    free: number;
    busy: number;
    dirty: number;
    broken: number;
  }>;

  const byRepoRows = db.prepare(`
    SELECT
      nickname,
      repo_name,
      COUNT(*) AS total,
      SUM(CASE WHEN state = 'FREE' THEN 1 ELSE 0 END) AS free,
      SUM(CASE WHEN state = 'BUSY' THEN 1 ELSE 0 END) AS busy,
      SUM(CASE WHEN state = 'DIRTY' THEN 1 ELSE 0 END) AS dirty,
      SUM(CASE WHEN state = 'BROKEN' THEN 1 ELSE 0 END) AS broken
    FROM slots
    ${filterClause}
    GROUP BY nickname, repo_name
    ORDER BY nickname ASC, repo_name ASC
  `).all(...params) as Array<{
    nickname: string;
    repo_name: string;
    total: number;
    free: number;
    busy: number;
    dirty: number;
    broken: number;
  }>;

  return {
    total: Number(totalRow.total || 0),
    byState: byStateRows.map(mapSlotStateCount),
    byNickname: byNicknameRows.map(mapSlotStatsByNickname),
    byRepo: byRepoRows.map(mapSlotStatsByRepo),
  };
}

export function getPoolStats(nickname?: string): PoolStats {
  const db = openDb();
  const filterClause = nickname ? 'WHERE nickname = ?' : '';
  const params = nickname ? [nickname] : [];

  const totalRow = db.prepare(`
    SELECT COUNT(*) AS total
    FROM pool_worktrees
    ${filterClause}
  `).get(...params) as { total: number };

  const byStateRows = db.prepare(`
    SELECT state, COUNT(*) AS total
    FROM pool_worktrees
    ${filterClause}
    GROUP BY state
    ORDER BY state ASC
  `).all(...params) as Array<{ state: string; total: number }>;

  const byNicknameRows = db.prepare(`
    SELECT
      nickname,
      COUNT(*) AS total,
      SUM(CASE WHEN state = 'FREE' THEN 1 ELSE 0 END) AS free,
      SUM(CASE WHEN state = 'BUSY' THEN 1 ELSE 0 END) AS busy,
      SUM(CASE WHEN state = 'DIRTY' THEN 1 ELSE 0 END) AS dirty,
      SUM(CASE WHEN state = 'BROKEN' THEN 1 ELSE 0 END) AS broken
    FROM pool_worktrees
    ${filterClause}
    GROUP BY nickname
    ORDER BY nickname ASC
  `).all(...params) as Array<{
    nickname: string;
    total: number;
    free: number;
    busy: number;
    dirty: number;
    broken: number;
  }>;

  const byRepoRows = db.prepare(`
    SELECT
      nickname,
      repo_name,
      COUNT(*) AS total,
      SUM(CASE WHEN state = 'FREE' THEN 1 ELSE 0 END) AS free,
      SUM(CASE WHEN state = 'BUSY' THEN 1 ELSE 0 END) AS busy,
      SUM(CASE WHEN state = 'DIRTY' THEN 1 ELSE 0 END) AS dirty,
      SUM(CASE WHEN state = 'BROKEN' THEN 1 ELSE 0 END) AS broken
    FROM pool_worktrees
    ${filterClause}
    GROUP BY nickname, repo_name
    ORDER BY nickname ASC, repo_name ASC
  `).all(...params) as Array<{
    nickname: string;
    repo_name: string;
    total: number;
    free: number;
    busy: number;
    dirty: number;
    broken: number;
  }>;

  return {
    total: Number(totalRow.total || 0),
    byState: byStateRows.map(mapSlotStateCount),
    byNickname: byNicknameRows.map(mapSlotStatsByNickname),
    byRepo: byRepoRows.map(mapSlotStatsByRepo),
  };
}

export function updateSlotState(slotId: string, state: SlotState): SlotRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    UPDATE slots
    SET state = ?, updated_at = ?, last_used_at = ?
    WHERE slot_id = ?
  `).run(state, timestamp, timestamp, slotId);

  const row = db.prepare(`
    SELECT slot_id, nickname, repo_name, slot_path, pool_worktree_id, state, current_consumer_id, created_at, updated_at, last_used_at
    FROM slots
    WHERE slot_id = ?
  `).get(slotId) as {
    slot_id: string;
    nickname: string;
    repo_name: string;
    slot_path: string;
    pool_worktree_id: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  };
  return mapSlotRow(row);
}

export function updatePoolWorktreeState(poolWorktreeId: string, state: SlotState): PoolWorktreeRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    UPDATE pool_worktrees
    SET state = ?, updated_at = ?, last_used_at = ?
    WHERE pool_worktree_id = ?
  `).run(state, timestamp, timestamp, poolWorktreeId);

  const row = db.prepare(`
    SELECT pool_worktree_id, nickname, repo_name, pool_path, state, current_consumer_id, created_at, updated_at, last_used_at
    FROM pool_worktrees
    WHERE pool_worktree_id = ?
  `).get(poolWorktreeId) as {
    pool_worktree_id: string;
    nickname: string;
    repo_name: string;
    pool_path: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  };
  return mapPoolWorktreeRow(row);
}

export function assignPoolWorktree(poolWorktreeId: string, consumerId?: string): PoolWorktreeRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    UPDATE pool_worktrees
    SET state = 'BUSY', current_consumer_id = ?, updated_at = ?, last_used_at = ?
    WHERE pool_worktree_id = ?
  `).run(consumerId ?? null, timestamp, timestamp, poolWorktreeId);

  const row = db.prepare(`
    SELECT pool_worktree_id, nickname, repo_name, pool_path, state, current_consumer_id, created_at, updated_at, last_used_at
    FROM pool_worktrees
    WHERE pool_worktree_id = ?
  `).get(poolWorktreeId) as {
    pool_worktree_id: string;
    nickname: string;
    repo_name: string;
    pool_path: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  };
  return mapPoolWorktreeRow(row);
}

export function clearPoolWorktreeConsumer(poolWorktreeId: string): void {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    UPDATE pool_worktrees
    SET current_consumer_id = NULL, updated_at = ?, last_used_at = ?
    WHERE pool_worktree_id = ?
  `).run(timestamp, timestamp, poolWorktreeId);
}

export function assignSlot(slotId: string, consumerId?: string): SlotRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    UPDATE slots
    SET state = 'BUSY', current_consumer_id = ?, updated_at = ?, last_used_at = ?
    WHERE slot_id = ?
  `).run(consumerId ?? null, timestamp, timestamp, slotId);

  const row = db.prepare(`
    SELECT slot_id, nickname, repo_name, slot_path, pool_worktree_id, state, current_consumer_id, created_at, updated_at, last_used_at
    FROM slots
    WHERE slot_id = ?
  `).get(slotId) as {
    slot_id: string;
    nickname: string;
    repo_name: string;
    slot_path: string;
    pool_worktree_id: string;
    state: string;
    current_consumer_id: string | null;
    created_at: string;
    updated_at: string;
    last_used_at: string;
  };
  return mapSlotRow(row);
}

export function addRunSlot(input: {
  runId: string;
  slotId: string;
  repoName: string;
  poolWorktreeId: string;
}): RunSlotRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    INSERT INTO run_slots (run_id, slot_id, repo_name, pool_worktree_id, created_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(run_id, slot_id) DO UPDATE SET
      repo_name = excluded.repo_name,
      pool_worktree_id = excluded.pool_worktree_id
  `).run(input.runId, input.slotId, input.repoName, input.poolWorktreeId, timestamp);

  const row = db.prepare(`
    SELECT id, run_id, slot_id, repo_name, pool_worktree_id, created_at
    FROM run_slots
    WHERE run_id = ? AND slot_id = ?
  `).get(input.runId, input.slotId) as {
    id: number;
    run_id: string;
    slot_id: string;
    repo_name: string;
    pool_worktree_id: string;
    created_at: string;
  };
  return mapRunSlotRow(row);
}

export function listRunSlots(runId: string): RunSlotRow[] {
  const db = openDb();
  const rows = db.prepare(`
    SELECT id, run_id, slot_id, repo_name, pool_worktree_id, created_at
    FROM run_slots
    WHERE run_id = ?
    ORDER BY repo_name ASC, slot_id ASC
  `).all(runId) as Array<{
    id: number;
    run_id: string;
    slot_id: string;
    repo_name: string;
    pool_worktree_id: string;
    created_at: string;
  }>;
  return rows.map(mapRunSlotRow);
}

export function assignConsumerSlot(consumerId: string, slotId: string): ConsumerSlotRow {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare(`
    INSERT INTO consumer_slots (consumer_id, slot_id, created_at, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(slot_id) DO UPDATE SET
      consumer_id = excluded.consumer_id,
      updated_at = excluded.updated_at
  `).run(consumerId, slotId, timestamp, timestamp);
  db.prepare(`
    UPDATE slots
    SET current_consumer_id = ?, updated_at = ?, last_used_at = ?
    WHERE slot_id = ?
  `).run(consumerId, timestamp, timestamp, slotId);

  const row = db.prepare(`
    SELECT id, consumer_id, slot_id, created_at, updated_at
    FROM consumer_slots
    WHERE slot_id = ?
  `).get(slotId) as {
    id: number;
    consumer_id: string;
    slot_id: string;
    created_at: string;
    updated_at: string;
  };
  return mapConsumerSlotRow(row);
}

export function listConsumerSlots(consumerId: string): ConsumerSlotRow[] {
  const db = openDb();
  const rows = db.prepare(`
    SELECT id, consumer_id, slot_id, created_at, updated_at
    FROM consumer_slots
    WHERE consumer_id = ?
    ORDER BY slot_id ASC
  `).all(consumerId) as Array<{
    id: number;
    consumer_id: string;
    slot_id: string;
    created_at: string;
    updated_at: string;
  }>;
  return rows.map(mapConsumerSlotRow);
}

export function clearConsumerSlot(slotId: string): void {
  const db = openDb();
  const timestamp = nowIso();
  db.prepare('DELETE FROM consumer_slots WHERE slot_id = ?').run(slotId);
  db.prepare(`
    UPDATE slots
    SET current_consumer_id = NULL, updated_at = ?, last_used_at = ?
    WHERE slot_id = ?
  `).run(timestamp, timestamp, slotId);
}

export function deletePoolWorktree(poolWorktreeId: string): void {
  const db = openDb();
  db.prepare('DELETE FROM pool_worktrees WHERE pool_worktree_id = ?').run(poolWorktreeId);
}

export function deleteSlot(slotId: string): void {
  const db = openDb();
  db.prepare('DELETE FROM slots WHERE slot_id = ?').run(slotId);
}
