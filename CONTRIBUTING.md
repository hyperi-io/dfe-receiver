# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description | Version Bump |
|------|-------------|--------------|
| `feat` | A new feature | Minor (0.X.0) |
| `fix` | A bug fix | Patch (0.0.X) |
| `docs` | Documentation only | None |
| `style` | Code style (formatting, semicolons, etc.) | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | Patch (0.0.X) |
| `test` | Adding or correcting tests | None |
| `build` | Changes to build system or dependencies | None |
| `ci` | Changes to CI configuration | None |
| `chore` | Other changes that don't modify src or test files | None |
| `revert` | Reverts a previous commit | Varies |

### Breaking Changes

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Examples

```
feat(auth): add OAuth2 support for Google login

fix(api): handle null response from upstream service

docs: update installation instructions for Windows

refactor(core)!: restructure module exports

BREAKING CHANGE: Named exports are now used instead of default exports.
```

### Scope

Scope is optional but recommended. Use it to indicate the area of the codebase
affected (e.g., `api`, `auth`, `core`, `cli`, `docs`).

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** (X.0.0): Breaking changes that require users to modify their code
- **MINOR** (0.X.0): New features that are backwards-compatible
- **PATCH** (0.0.X): Bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

## Developer Certificate of Origin

This project uses the Developer Certificate of Origin (DCO) to ensure that
contributors have the right to submit their contributions.

By making a contribution to this project, you certify that:

1. The contribution was created in whole or in part by you and you have the
   right to submit it under the license indicated in the file; or

2. The contribution is based upon previous work that, to the best of your
   knowledge, is covered under an appropriate open source license and you
   have the right under that license to submit that work with modifications;
   or

3. The contribution was provided directly to me by some other person who
   certified (1), (2) or (3) and you have not modified it.

4. You understand and agree that this project and the contribution are public
   and that a record of the contribution (including all personal information
   you submit with it, including your sign-off) is maintained indefinitely
   and may be redistributed consistent with this project or the license(s)
   involved.

## How to Sign Off Your Commits

You must sign off each commit to indicate your acceptance of the DCO. Combine
the signoff with your conventional commit message:

```
git commit --signoff -m "feat(auth): add two-factor authentication"
```

This produces:

```
feat(auth): add two-factor authentication

Signed-off-by: Your Name <your.email@example.com>
```

Make sure your Git configuration has your correct name and email:

```
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

## License for Contributions

All contributions to this project are licensed under the Functional Source
License, Version 1.1, ALv2 Future License (FSL-1.1-ALv2), the same license
that covers the project.

Each version of the software (including your contributions) will automatically
become available under the Apache License, Version 2.0 on the second
anniversary of its release.

## How to Contribute

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the commit message format above
3. **Sign off your commits** with the DCO
4. **Test your changes** to ensure they work as expected
5. **Submit a pull request** with a clear description of what you've done

### Pull Request Checklist

- [ ] Commits follow the conventional commit format
- [ ] All commits are signed off (DCO)
- [ ] Tests pass (if applicable)
- [ ] Documentation is updated (if applicable)

## Working with Kafka

[kcat](https://github.com/edenhill/kcat) (formerly kafkacat) is essential for
verifying that messages flow through dfe-receiver correctly.

```bash
# Start local Kafka (KRaft mode, no Zookeeper)
docker compose -f docker-compose.test.yaml up -d

# List topics and broker metadata
kcat -b localhost:9092 -L

# Watch a topic in real time as dfe-receiver routes to it
kcat -b localhost:9092 -t events -C -o end

# Send a test event and verify receipt
curl -s -X POST http://localhost:8080/ingest \
  -H 'Content-Type: application/json' \
  -d '{"level":"info","message":"test"}'

kcat -b localhost:9092 -t events -C -c 1

# Consume with partition and offset metadata
kcat -b localhost:9092 -t events -C -f 'P:%p O:%o T:%T\n%s\n'

# Produce directly to Kafka (useful for testing consumers in isolation)
echo '{"source":"manual","msg":"direct inject"}' | \
  kcat -b localhost:9092 -t events -P

# Drain and count all messages in a topic
kcat -b localhost:9092 -t events -C -e -q | wc -l
```

> **Install:** `apt install kcat` / `brew install kcat`
> Older packages may be named `kafkacat`.

For a full browser UI, start with the `ui` profile:

```bash
docker compose -f docker-compose.test.yaml --profile ui up -d
# Open http://localhost:8080 (Kafbat UI)
```

## CI/CD Workflow

When your pull request is merged to `main`:

1. **semantic-release** analyses commit messages since the last release
2. Determines the next version number based on commit types
3. Generates/updates the CHANGELOG
4. Creates a new GitHub release with release notes
5. Publishes the package (if applicable)

This happens automatically - no manual intervention required.

## Questions

If you have questions about contributing, please open an issue or contact us.
