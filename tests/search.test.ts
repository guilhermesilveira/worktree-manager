import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { join } from 'node:path';

import { searchAcrossRepos } from '../src/search.js';
import { makeTempDir, removeTempDir, writeFile } from './testUtils.js';

let tempDir = '';

beforeEach(() => {
  tempDir = makeTempDir('wtm-search-');
});

afterEach(() => {
  removeTempDir(tempDir);
});

describe('searchAcrossRepos', () => {
  it('returns matches across repository roots', () => {
    const repoA = join(tempDir, 'repo-a');
    const repoB = join(tempDir, 'repo-b');
    writeFile(join(repoA, 'notes.txt'), 'alpha needle omega\n');
    writeFile(join(repoB, 'notes.txt'), 'beta only\n');

    const result = searchAcrossRepos('henon', [repoA, repoB], 'needle');

    expect(result.nickname).toBe('henon');
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('notes.txt');
    expect(result.stdout).toContain('needle');
  });

  it('returns exitCode 1 when there are no matches', () => {
    const repoA = join(tempDir, 'repo-a');
    writeFile(join(repoA, 'notes.txt'), 'alpha only\n');

    const result = searchAcrossRepos('henon', [repoA], 'needle');

    expect(result.exitCode).toBe(1);
    expect(result.stdout).toBe('');
  });
});

