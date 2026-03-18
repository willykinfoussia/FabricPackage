# Publishing `fabrictools` to PyPI

This guide covers everything needed to publish and maintain `fabrictools` on the Python Package Index (PyPI), from first-time setup to fully-automated releases via GitHub Actions.

---

## Table of contents

1. [Prerequisites](#1-prerequisites)
2. [Prepare your PyPI account](#2-prepare-your-pypi-account)
3. [Configure the package metadata](#3-configure-the-package-metadata)
4. [Build the distribution](#4-build-the-distribution)
5. [Test on TestPyPI first](#5-test-on-testpypi-first)
6. [Publish to production PyPI](#6-publish-to-production-pypi)
7. [Versioning strategy](#7-versioning-strategy)
8. [Automate with GitHub Actions](#8-automate-with-github-actions)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

Install the build and upload tools locally:

```bash
pip install --upgrade build twine
```

Make sure you are in the repository root (where `pyproject.toml` lives):

```bash
ls pyproject.toml   # should exist
```

---

## 2. Prepare your PyPI account

### 2.1 Create accounts

- **Production PyPI:** <https://pypi.org/account/register/>
- **TestPyPI** (for test uploads): <https://test.pypi.org/account/register/>

### 2.2 Generate API tokens

Passwords are deprecated on PyPI. Use API tokens instead.

1. Log in to <https://pypi.org>
2. Go to **Account settings → API tokens → Add API token**
3. Give it a name (e.g. `fabrictools-publish`) and scope it to the `fabrictools` project (or to your whole account for the first upload)
4. Copy the token — it is shown **only once**

Repeat the same steps on <https://test.pypi.org> for your test token.

### 2.3 Store tokens securely

Create (or edit) `~/.pypirc`:

```ini
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
repository = https://upload.pypi.org/legacy/
username = __token__
password = pypi-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

> **Security:** never commit `.pypirc` to version control.  Add it to `.gitignore`.

---

## 3. Configure the package metadata

All metadata lives in `pyproject.toml`.  Before every release, verify:

| Field | Location | Notes |
|---|---|---|
| `version` | `[project] version` | Must be bumped before each upload (PyPI rejects re-uploads of the same version) |
| `name` | `[project] name` | Must be unique on PyPI |
| `description` | `[project] description` | One-line summary shown on PyPI |
| `authors` | `[project] authors` | Your name and email |
| `Homepage` | `[project.urls]` | Update to your actual repository URL |

---

## 4. Build the distribution

```bash
# Remove any previous build artefacts
rm -rf dist/ build/ *.egg-info

# Build both sdist (source) and wheel
python -m build
```

After a successful build, `dist/` will contain two files:

```
dist/
├── fabrictools-0.1.0.tar.gz      # source distribution
└── fabrictools-0.1.0-py3-none-any.whl   # wheel
```

Inspect the wheel contents before uploading:

```bash
twine check dist/*
```

---

## 5. Test on TestPyPI first

Always do a trial run on TestPyPI to catch metadata or packaging issues without polluting the production index.

```bash
twine upload --repository testpypi dist/*
```

Verify the upload at `https://test.pypi.org/project/fabrictools/`, then install from TestPyPI to confirm:

```bash
pip install --index-url https://test.pypi.org/simple/ fabrictools
```

---

## 6. Publish to production PyPI

When the TestPyPI upload looks correct:

```bash
twine upload dist/*
```

Your package is now available at `https://pypi.org/project/fabrictools/` and can be installed with:

```bash
pip install fabrictools
```

---

## 7. Versioning strategy

This project follows [Semantic Versioning](https://semver.org/):

```
MAJOR.MINOR.PATCH
  │     │     └── Bug fix, no API change
  │     └──────── New feature, backward-compatible
  └────────────── Breaking change
```

Steps for a new release:

1. Update `version` in `pyproject.toml`
2. Update `__version__` in `fabrictools/__init__.py` to match
3. Commit: `git commit -m "chore: bump version to X.Y.Z"`
4. Tag: `git tag vX.Y.Z`
5. Push: `git push origin main --tags`

---

## 8. Automate with GitHub Actions

Create the file `.github/workflows/publish.yml` in your repository to publish automatically whenever you push a version tag (`v*.*.*`).

```yaml
name: Publish to PyPI

on:
  push:
    tags:
      - "v*.*.*"

jobs:
  build-and-publish:
    name: Build and publish
    runs-on: ubuntu-latest
    environment: pypi           # optional: require manual approval in GitHub

    permissions:
      id-token: write           # required for Trusted Publishing (OIDC)

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install build tools
        run: pip install build twine

      - name: Build distributions
        run: python -m build

      - name: Check distributions
        run: twine check dist/*

      # Option A — Trusted Publishing (recommended, no secret needed)
      - name: Publish to PyPI via Trusted Publishing
        uses: pypa/gh-action-pypi-publish@release/v1

      # Option B — API token (comment out Option A and uncomment this block)
      # - name: Publish to PyPI via API token
      #   env:
      #     TWINE_USERNAME: __token__
      #     TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
      #   run: twine upload dist/*
```

### Option A — Trusted Publishing (recommended)

PyPI supports [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) via OIDC, which means **no API token secret is needed**.

Setup steps:
1. Go to your project on PyPI → **Manage → Publishing**
2. Add a new trusted publisher:
   - Owner: `your-org`
   - Repository: `fabrictools`
   - Workflow filename: `publish.yml`
   - Environment: `pypi` (or leave blank)
3. Commit the workflow file and push a tag — the action will publish automatically.

### Option B — API token secret

If you prefer token-based auth:
1. Go to your GitHub repository → **Settings → Secrets and variables → Actions**
2. Create a new secret named `PYPI_API_TOKEN` with the token value from PyPI
3. Use the commented-out block in the workflow above instead of Option A

---

## 9. Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `File already exists` | Version already uploaded to PyPI | Bump the version in `pyproject.toml` and rebuild |
| `Invalid distribution` | Wheel or sdist corrupted | Run `twine check dist/*` and rebuild |
| `403 Forbidden` | Wrong or expired token | Regenerate the API token on PyPI |
| `Package name not available` | Name taken on PyPI | Choose a different name in `pyproject.toml` |
| `The user ... isn't allowed to upload` | Token scoped to wrong project | Create a new token scoped to your project |
