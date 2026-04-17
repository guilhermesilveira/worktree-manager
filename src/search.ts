import { spawnSync } from 'node:child_process';

import type { SearchResult } from './types.js';

export function searchAcrossRepos(nickname: string, repoPaths: string[], pattern: string): SearchResult {
  const args = [pattern, ...repoPaths];
  const result = spawnSync('rg', args, {
    encoding: 'utf-8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  return {
    nickname,
    repos: repoPaths,
    exitCode: Number(result.status ?? 1),
    stdout: String(result.stdout || ''),
    stderr: String(result.stderr || ''),
  };
}

