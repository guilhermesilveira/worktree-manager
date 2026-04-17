import { homedir } from 'node:os';
import { join, resolve } from 'node:path';

export function resolveAppDir(): string {
  const override = String(process.env.WORKTREE_MANAGER_HOME || '').trim();
  if (override) return resolve(override);
  return join(homedir(), '.worktree-manager');
}

export function resolveDatabasePath(): string {
  return join(resolveAppDir(), 'worktree-manager.sqlite');
}

export function resolveAbsolutePath(input: string): string {
  return resolve(String(input || '').trim());
}
