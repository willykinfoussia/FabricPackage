"""Sync the **Version :** line in README.md with fabrictools/_version.py.

Run from repository root::

    python scripts/sync_readme_version.py

Check only (exit 1 if README is out of date)::

    python scripts/sync_readme_version.py --check
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_VERSION_FILE = _REPO_ROOT / "fabrictools" / "_version.py"
_README = _REPO_ROOT / "README.md"
_README_VERSION_LINE = re.compile(
    r"^(\*\*Version :\*\* )([\w.+-]+)(.*)$",
    re.MULTILINE,
)


def read_package_version() -> str:
    text = _VERSION_FILE.read_text(encoding="utf-8")
    m = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', text)
    if not m:
        print(f"Could not parse __version__ in {_VERSION_FILE}", file=sys.stderr)
        raise SystemExit(1)
    return m.group(1)


def readme_declared_version(readme: str) -> str | None:
    m = _README_VERSION_LINE.search(readme)
    return m.group(2) if m else None


def sync_readme_text(readme: str, version: str) -> tuple[str, bool]:
    m = _README_VERSION_LINE.search(readme)
    if not m:
        print(
            f"README: no line matching '**Version :** <version>...' in {_README}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    if m.group(2) == version:
        return readme, False
    new_readme, n = _README_VERSION_LINE.subn(
        lambda m2: f"{m2.group(1)}{version}{m2.group(3)}",
        readme,
        count=1,
    )
    if n != 1:
        raise SystemExit("Unexpected: version line replace count != 1")
    return new_readme, True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 if README version does not match _version.py (no writes).",
    )
    args = parser.parse_args()

    pkg_version = read_package_version()
    readme_text = _README.read_text(encoding="utf-8")
    current = readme_declared_version(readme_text)

    if current is None:
        raise SystemExit(1)

    if args.check:
        if current != pkg_version:
            print(
                f"README version {current!r} != fabrictools/_version.py {pkg_version!r}\n"
                f"Run: python scripts/sync_readme_version.py",
                file=sys.stderr,
            )
            raise SystemExit(1)
        print("README version matches fabrictools/_version.py:", pkg_version)
        return

    new_text, changed = sync_readme_text(readme_text, pkg_version)
    if changed:
        _README.write_text(new_text, encoding="utf-8", newline="\n")
        print(f"Updated {_README} to version {pkg_version}")
    else:
        print(f"README already at version {pkg_version}")


if __name__ == "__main__":
    main()
