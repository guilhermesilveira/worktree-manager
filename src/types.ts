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

export type RunStatus = 'ACTIVE' | 'PUSHED' | 'FAILED' | 'PURGED';

export interface RunRow {
  runId: string;
  nickname: string;
  workspaceRoot: string;
  status: RunStatus;
  createdAt: string;
  updatedAt: string;
  finishedAt: string | null;
}

export interface RunWorktreeRow {
  id: number;
  runId: string;
  repoName: string;
  worktreePath: string;
  branchName: string;
  isPrimary: boolean;
  createdAt: string;
  updatedAt: string;
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
