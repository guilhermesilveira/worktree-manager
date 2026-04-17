# worktree-manager

Small CLI for managing groups of related repositories and transient multi-repo run workspaces.

Current feature set:

- register a project nickname and its run base directory
- add related repositories to that nickname
- mark one repository as the primary default target
- list registered repositories
- search across all repositories for a nickname with `rg`
- create transient run workspaces with writable worktrees
- promote extra repositories into a run as writable worktrees
- push each run worktree to its own run branch
- purge finished run workspaces

## Install

```bash
npm install
npm run build
```

Run locally during development:

```bash
npm run dev -- --help
```

Or after building:

```bash
node dist/cli.js --help
```

## Example

```bash
node dist/cli.js register henon /Users/guilhermesilveira/nobackup/code/runs/henon
node dist/cli.js add henon henon /Users/guilhermesilveira/nobackup/code/henon --primary
node dist/cli.js add henon henon-pub02 /Users/guilhermesilveira/nobackup/code/henon-pub02-completeness
node dist/cli.js list henon
node dist/cli.js search henon "kneading"
node dist/cli.js new-tree henon --json
node dist/cli.js promote run-... henon-pub02 --json
node dist/cli.js push-tree run-... --json
```

## Storage

The tool stores metadata in a local SQLite database at:

```text
~/.worktree-manager/worktree-manager.sqlite
```

This keeps the tool independent from ResearchUI for now.

## Workflow

`push-tree` pushes to per-run branches such as `wt/run-...-repo`, not to the repository's default branch.
That leaves the later merge/integration step to the caller or worker layer.

For tests or isolated runs, you can override the app directory:

```bash
WORKTREE_MANAGER_HOME=/tmp/worktree-manager-dev node dist/cli.js list henon
```
