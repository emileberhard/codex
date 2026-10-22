#!/usr/bin/env python3
# Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from codex_package.cli import parse_package_version, resolve_source_commit


class PackageVersionTest(unittest.TestCase):
    def test_accepts_release_prerelease_and_build_versions(self) -> None:
        for version in (
            "0.0.0",
            "1.2.3",
            "0.0.0-internal.deadbeef",
            "1.2.3-alpha.1+build.01",
            "18446744073709551615.0.0",
        ):
            with self.subTest(version=version):
                self.assertEqual(parse_package_version(version), version)

    def test_rejects_versions_the_runtime_cannot_parse(self) -> None:
        for version in (
            "",
            "1",
            "1.2",
            "1.2.3.4",
            "v1.2.3",
            "01.2.3",
            "1.02.3",
            "1.2.03",
            "1.2.3-",
            "1.2.3-alpha..1",
            "1.2.3-01",
            "1.2.3+",
            "1.2.3+build..1",
            "18446744073709551616.0.0",
        ):
            with self.subTest(version=version):
                with self.assertRaises(argparse.ArgumentTypeError):
                    parse_package_version(version)


class SourceProvenanceTest(unittest.TestCase):
    def test_source_commit_comes_from_a_clean_git_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", str(root)], check=True, capture_output=True)
            subprocess.run(
                ["git", "-C", str(root), "config", "user.email", "test@example.com"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(root), "config", "user.name", "Test"], check=True
            )
            tracked = root / "tracked"
            tracked.write_text("clean", encoding="utf-8")
            subprocess.run(["git", "-C", str(root), "add", "tracked"], check=True)
            subprocess.run(
                ["git", "-C", str(root), "commit", "-m", "test"],
                check=True,
                capture_output=True,
            )
            expected = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()

            with patch.dict(
                os.environ,
                {"CODEX_REPO_ROOT": str(root)},
                clear=False,
            ):
                os.environ.pop("CODEX_SOURCE_COMMIT", None)
                self.assertEqual(resolve_source_commit(), expected)
                os.environ["CODEX_SOURCE_COMMIT"] = "wrong"
                with self.assertRaisesRegex(RuntimeError, "does not match Git HEAD"):
                    resolve_source_commit()
                os.environ.pop("CODEX_SOURCE_COMMIT", None)
                tracked.write_text("dirty", encoding="utf-8")
                with self.assertRaisesRegex(RuntimeError, "source tree is dirty"):
                    resolve_source_commit()


if __name__ == "__main__":
    unittest.main()
