# AI Agent Git Commit Guidelines

This document defines the strict requirements for AI agents performing version control operations in this repository.

## 1. Commit Frequency

- **Atomic Commits**: Agents **MUST** execute a `git commit` immediately after completing any discrete unit of work, bugfix, or feature step.
- **Verification Before Commit**: Never commit unverified or broken code. Always run `cargo check` and `cargo test` prior to executing a commit.
- **No Unstaged Leftovers**: Ensure all relevant modified files and new files (`src/`, documentation, tests) are staged with `git add` before committing.

## 2. Conventional Commits Standard

All commit messages **MUST** strictly follow the [Conventional Commits 1.0.0](https://www.conventionalcommits.org/) specification:

```text
<type>(<scope>): <short description>

[optional body]

[optional footer(s)]
```

### Allowed Types

| Type | Description | Example |
| :--- | :--- | :--- |
| `feat` | A new feature for the user or system | `feat(mesh): add outbound seed connection support` |
| `fix` | A bug fix | `fix(relay): correct backoff timer calculation` |
| `docs` | Documentation changes only | `docs(readme): update architecture diagram` |
| `style` | Formatting, missing semi-colons, no code logic change | `style(mesh): format code imports` |
| `refactor` | Code change that neither fixes a bug nor adds a feature | `refactor(mesh): extract unified handle_peer_stream handler` |
| `perf` | Code change that improves performance | `perf(dedup): optimize event ID set lookup` |
| `test` | Adding missing tests or correcting existing tests | `test(mesh): add two-node sync integration test` |
| `build` | Changes affecting build system or external dependencies | `build(deps): update tokio-tungstenite to 0.26` |
| `ci` | Changes to CI configuration files and scripts | `ci(github): add cargo test workflow` |
| `chore` | Maintenance tasks, git configuration, script updates | `chore(git): add commit-msg hook for conventional commits` |
| `revert` | Reverts a previous commit | `revert(relay): undo custom timeout setting` |

### Rules & Formatting

1. **Header Format**: `<type>(<scope>): <description>` (scope is optional but recommended).
2. **Lowercase**: Use lowercase for `<type>`, `<scope>`, and the first letter of `<description>`.
3. **No Trailing Period**: Do not place a period `.` at the end of the summary line.
4. **Imperative Mood**: Write the description in imperative present tense (e.g., `add` not `added`, `fix` not `fixed`).
5. **Length**: Keep the first line under 72 characters.
