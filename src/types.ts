export interface ProjectRow {
  nickname: string;
  baseDir: string;
  createdAt: string;
  updatedAt: string;
}

export interface RepositoryRow {
  id: number;
  nickname: string;
  name: string;
  localPath: string;
  remoteUrl: string;
  defaultBranch: string;
  isPrimary: boolean;
  createdAt: string;
  updatedAt: string;
}

export type RunStatus = 'ACTIVE' | 'PUSHED' | 'FAILED' | 'RELEASED' | 'PURGED';
export type SlotState = 'FREE' | 'BUSY' | 'DIRTY' | 'BROKEN';
export type RunEventLevel = 'INFO' | 'WARN' | 'ERROR';

export interface RunEventRow {
  id: number;
  runId: string;
  eventType: string;
  level: RunEventLevel;
  message: string;
  payloadJson: string | null;
  createdAt: string;
}

export interface RunRow {
  runId: string;
  nickname: string;
  workspaceRoot: string;
  status: RunStatus;
  createdAt: string;
  updatedAt: string;
  finishedAt: string | null;
}

export interface RunListItem extends RunRow {
  repoCount: number;
  eventCount: number;
  latestEventType: string | null;
  latestEventLevel: RunEventLevel | null;
  latestEventMessage: string | null;
  latestEventAt: string | null;
}

export interface RunWorktreeRow {
  id: number;
  runId: string;
  repoName: string;
  worktreePath: string;
  branchName: string;
  poolWorktreeId: string;
  isPrimary: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface RunInspectionWorktree extends RunWorktreeRow {
  slot: SlotRow | null;
  poolWorktree: PoolWorktreeRow | null;
}

export interface RunInspection {
  run: RunRow;
  worktrees: RunInspectionWorktree[];
  events: RunEventRow[];
}

export interface SlotRow {
  slotId: string;
  nickname: string;
  repoName: string;
  slotPath: string;
  poolWorktreeId: string;
  state: SlotState;
  currentConsumerId: string | null;
  createdAt: string;
  updatedAt: string;
  lastUsedAt: string;
}

export interface RunSlotRow {
  id: number;
  runId: string;
  slotId: string;
  repoName: string;
  poolWorktreeId: string;
  createdAt: string;
}

export interface PoolWorktreeRow {
  poolWorktreeId: string;
  nickname: string;
  repoName: string;
  poolPath: string;
  state: SlotState;
  currentConsumerId: string | null;
  createdAt: string;
  updatedAt: string;
  lastUsedAt: string;
}

export interface PoolWorktreeInspectionSlot {
  slotId: string;
  slotPath: string;
  state: SlotState;
  currentConsumerId: string | null;
  createdAt: string;
}

export interface PoolWorktreeInspectionRun {
  runId: string;
  status: RunStatus;
  workspaceRoot: string;
  repoName: string;
  branchName: string | null;
  slotId: string | null;
  slotPath: string | null;
  createdAt: string;
}

export interface PoolWorktreeInspection {
  poolWorktree: PoolWorktreeRow;
  slots: PoolWorktreeInspectionSlot[];
  runs: PoolWorktreeInspectionRun[];
}

export interface ConsumerSlotRow {
  id: number;
  consumerId: string;
  slotId: string;
  createdAt: string;
  updatedAt: string;
}

export interface SlotInspectionRun {
  runId: string;
  status: RunStatus;
  workspaceRoot: string;
  repoName: string;
  poolWorktreeId: string;
  branchName: string | null;
  isPrimary: boolean | null;
  createdAt: string;
}

export interface SlotInspection {
  slot: SlotRow;
  poolWorktree: PoolWorktreeRow | null;
  runs: SlotInspectionRun[];
  consumers: ConsumerSlotRow[];
}

export interface SlotStateCount {
  state: SlotState;
  total: number;
}

export interface SlotStatsByNickname {
  nickname: string;
  total: number;
  free: number;
  busy: number;
  dirty: number;
  broken: number;
}

export interface SlotStatsByRepo {
  nickname: string;
  repoName: string;
  total: number;
  free: number;
  busy: number;
  dirty: number;
  broken: number;
}

export interface SlotStats {
  total: number;
  byState: SlotStateCount[];
  byNickname: SlotStatsByNickname[];
  byRepo: SlotStatsByRepo[];
}

export interface PoolStats {
  total: number;
  byState: SlotStateCount[];
  byNickname: SlotStatsByNickname[];
  byRepo: SlotStatsByRepo[];
}

export interface SearchResult {
  nickname: string;
  repos: string[];
  exitCode: number;
  stdout: string;
  stderr: string;
}

export interface PushTreeRepoResult {
  repoName: string;
  worktreePath: string;
  branchName: string;
  targetBranch: string;
  commitCreated: boolean;
  pushed: boolean;
}

export interface PushTreeResult {
  run: RunRow;
  repositories: PushTreeRepoResult[];
}

export interface ReleaseTreeSlotResult {
  slotId: string;
  repoName: string;
  slotPath: string;
  state: SlotState;
}

export interface ReleaseTreeResult {
  run: RunRow;
  slots: ReleaseTreeSlotResult[];
}

export interface CleanupSlotResult {
  slotId: string;
  repoName: string;
  slotPath: string;
  state: SlotState;
}

export interface CleanupConsumerResult {
  consumerId: string;
  slots: CleanupSlotResult[];
}

export interface CleanupConsumerRunResult {
  runId: string;
  status: RunStatus;
  slots: ReleaseTreeSlotResult[];
}

export interface CleanupConsumerSkippedRun {
  runId: string;
  status: RunStatus;
  reason: string;
}

export interface CleanupConsumerRunsResult {
  consumerId: string;
  safe: boolean;
  released: CleanupConsumerRunResult[];
  skipped: CleanupConsumerSkippedRun[];
}

export interface GcPoolWorktreeResult {
  poolWorktreeId: string;
  repoName: string;
  poolPath: string;
}

export interface GcPoolResult {
  removed: GcPoolWorktreeResult[];
  skipped: string[];
}
