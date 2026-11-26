---
sidebar_position: 6
title: Pre-commit
---

Scans staged changes for secrets with [gitleaks](https://github.com/gitleaks/gitleaks) before each commit.

```bash
pip install pre-commit
brew install gitleaks
pre-commit install
```

The hook only checks staged changes. To audit the whole repo:

```bash
gitleaks git . --redact -v   # git history
gitleaks dir . --redact -v   # files on disk, including ignored ones
```
