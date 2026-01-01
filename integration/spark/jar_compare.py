#!/usr/bin/env python3
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
JAR Comparison Tool

Compares two JAR files by extracting their contents and performing
recursive comparison of all files and directory structures.
"""

import argparse
import difflib
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

# Constants
MIN_PRINTABLE_CHAR = 32
MAX_PRINTABLE_CHAR = 126
TEXT_THRESHOLD = 0.7
MAX_DIFF_LINES = 50


class JarComparator:
    """Compare two JAR files recursively."""

    def __init__(self, jar1_path: str, jar2_path: str, verbose: bool = False):
        self.jar1_path = Path(jar1_path)
        self.jar2_path = Path(jar2_path)
        self.verbose = verbose
        self.temp_dir1: Optional[Path] = None
        self.temp_dir2: Optional[Path] = None

    def __enter__(self):
        """Context manager entry - create temp directories."""
        self.temp_dir1 = Path(tempfile.mkdtemp(prefix="jar1_"))
        self.temp_dir2 = Path(tempfile.mkdtemp(prefix="jar2_"))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup temp directories."""
        if self.temp_dir1 and self.temp_dir1.exists():
            shutil.rmtree(self.temp_dir1)
        if self.temp_dir2 and self.temp_dir2.exists():
            shutil.rmtree(self.temp_dir2)

    def extract_jar(self, jar_path: Path, extract_dir: Path) -> None:
        """Extract JAR file to specified directory."""
        if self.verbose:
            print(f"Extracting {jar_path} to {extract_dir}")

        try:
            with zipfile.ZipFile(jar_path, "r") as jar_file:
                jar_file.extractall(extract_dir)
        except zipfile.BadZipFile as e:
            msg = f"Invalid JAR file: {jar_path}"
            raise ValueError(msg) from e
        except OSError as e:
            msg = f"Failed to extract {jar_path}: {e}"
            raise RuntimeError(msg) from e

    def get_file_list(self, directory: Path) -> set[str]:
        """Get relative paths of all files in directory."""
        files = set()
        for root, _, filenames in os.walk(directory):
            for filename in filenames:
                full_path = Path(root) / filename
                rel_path = full_path.relative_to(directory)
                files.add(str(rel_path))
        return files

    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of file."""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except OSError:
            return "ERROR_READING_FILE"

    def is_text_file(self, file_path: Path) -> bool:
        """Check if file is likely a text file."""
        text_extensions = {
            ".txt",
            ".java",
            ".xml",
            ".properties",
            ".yml",
            ".yaml",
            ".json",
            ".md",
            ".rst",
            ".html",
            ".css",
            ".js",
            ".sql",
        }

        if file_path.suffix.lower() in text_extensions:
            return True

        # Check first few bytes for binary content
        try:
            with open(file_path, "rb") as f:
                chunk = f.read(1024)
                # Look for null bytes or high percentage of non-printable chars
                if b"\x00" in chunk:
                    return False
                printable = sum(
                    1 for b in chunk if MIN_PRINTABLE_CHAR <= b <= MAX_PRINTABLE_CHAR or b in (9, 10, 13)
                )
                return len(chunk) == 0 or printable / len(chunk) > TEXT_THRESHOLD
        except OSError:
            return False

    def compare_text_files(self, file1: Path, file2: Path) -> list[str]:
        """Compare two text files and return unified diff."""
        try:
            with open(file1, encoding="utf-8", errors="replace") as f1:
                lines1 = f1.readlines()
            with open(file2, encoding="utf-8", errors="replace") as f2:
                lines2 = f2.readlines()

            return list(
                difflib.unified_diff(
                    lines1, lines2, fromfile=str(file1.name), tofile=str(file2.name), lineterm=""
                )
            )
        except OSError as e:
            return [f"Error comparing text files: {e}"]

    def decompile_class_file(self, class_file: Path) -> Optional[list[str]]:
        """Decompile .class file using javap if available."""
        try:
            result = subprocess.run(  # noqa: S603
                ["javap", "-c", "-p", str(class_file)],  # noqa: S607
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if result.returncode == 0:
                return result.stdout.splitlines()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return None

    def compare_class_files(self, file1: Path, file2: Path) -> list[str]:
        """Compare two .class files using javap decompilation."""
        decomp1 = self.decompile_class_file(file1)
        decomp2 = self.decompile_class_file(file2)

        if decomp1 is None or decomp2 is None:
            # Fall back to checksum comparison
            checksum1 = self.calculate_checksum(file1)
            checksum2 = self.calculate_checksum(file2)
            if checksum1 != checksum2:
                return ["Binary difference detected (javap unavailable)"]
            return []

        return list(
            difflib.unified_diff(
                decomp1,
                decomp2,
                fromfile=f"{file1.name} (decompiled)",
                tofile=f"{file2.name} (decompiled)",
                lineterm="",
            )
        )

    def compare_files(self, rel_path: str) -> tuple[str, list[str]]:
        """Compare a single file between the two extracted JARs."""
        file1 = self.temp_dir1 / rel_path
        file2 = self.temp_dir2 / rel_path

        if not file1.exists() or not file2.exists():
            return "MISSING", ["File missing in one of the JARs"]

        # Check if files are identical
        checksum1 = self.calculate_checksum(file1)
        checksum2 = self.calculate_checksum(file2)

        if checksum1 == checksum2:
            return "IDENTICAL", []

        # Files differ - determine comparison method
        if rel_path.endswith(".class"):
            diff_lines = self.compare_class_files(file1, file2)
            return "CLASS_DIFF", diff_lines

        if self.is_text_file(file1):
            diff_lines = self.compare_text_files(file1, file2)
            return "TEXT_DIFF", diff_lines

        return "BINARY_DIFF", ["Binary files differ (checksum mismatch)"]

    def compare(self) -> dict:
        """Compare the two JAR files and return results."""
        # Validate input files
        if not self.jar1_path.exists():
            msg = f"JAR file not found: {self.jar1_path}"
            raise FileNotFoundError(msg)
        if not self.jar2_path.exists():
            msg = f"JAR file not found: {self.jar2_path}"
            raise FileNotFoundError(msg)

        # Extract both JARs
        self.extract_jar(self.jar1_path, self.temp_dir1)
        self.extract_jar(self.jar2_path, self.temp_dir2)

        # Get file lists
        files1 = self.get_file_list(self.temp_dir1)
        files2 = self.get_file_list(self.temp_dir2)

        # Identify file differences
        only_in_jar1 = files1 - files2
        only_in_jar2 = files2 - files1
        common_files = files1 & files2

        # Compare common files
        file_diffs = {}
        identical_count = 0

        for rel_path in sorted(common_files):
            if self.verbose:
                print(f"Comparing: {rel_path}")

            status, diff_lines = self.compare_files(rel_path)

            if status == "IDENTICAL":
                identical_count += 1
            else:
                file_diffs[rel_path] = {"status": status, "diff": diff_lines}

        return {
            "jar1_path": str(self.jar1_path),
            "jar2_path": str(self.jar2_path),
            "only_in_jar1": sorted(only_in_jar1),
            "only_in_jar2": sorted(only_in_jar2),
            "identical_files": identical_count,
            "different_files": file_diffs,
            "total_files_jar1": len(files1),
            "total_files_jar2": len(files2),
        }


def _print_header(results: dict) -> None:
    """Print comparison report header."""
    print("=" * 80)
    print("JAR COMPARISON REPORT")
    print("=" * 80)
    print(f"JAR 1: {results['jar1_path']}")
    print(f"JAR 2: {results['jar2_path']}")
    print()


def _print_summary(results: dict) -> None:
    """Print comparison summary statistics."""
    print("SUMMARY:")
    print(f"  Files in JAR 1: {results['total_files_jar1']}")
    print(f"  Files in JAR 2: {results['total_files_jar2']}")
    print(f"  Identical files: {results['identical_files']}")
    print(f"  Different files: {len(results['different_files'])}")
    print(f"  Only in JAR 1: {len(results['only_in_jar1'])}")
    print(f"  Only in JAR 2: {len(results['only_in_jar2'])}")
    print()


def _print_unique_files(results: dict) -> None:
    """Print files unique to each JAR."""
    if results["only_in_jar1"]:
        print("FILES ONLY IN JAR 1:")
        for file_path in results["only_in_jar1"]:
            print(f"  + {file_path}")
        print()

    if results["only_in_jar2"]:
        print("FILES ONLY IN JAR 2:")
        for file_path in results["only_in_jar2"]:
            print(f"  + {file_path}")
        print()


def _print_different_files(results: dict) -> None:
    """Print details of different files."""
    if not results["different_files"]:
        return

    print("DIFFERENT FILES:")
    for file_path, info in results["different_files"].items():
        print(f"\n--- {file_path} ({info['status']}) ---")
        if info["diff"] and len(info["diff"]) <= MAX_DIFF_LINES:
            for line in info["diff"]:
                print(line)
        elif info["diff"]:
            print(f"  (Diff too large: {len(info['diff'])} lines)")
        else:
            print("  No detailed diff available")


def _print_result(results: dict) -> None:
    """Print final comparison result."""
    print("\n" + "=" * 80)

    is_identical = (
        len(results["only_in_jar1"]) == 0
        and len(results["only_in_jar2"]) == 0
        and len(results["different_files"]) == 0
    )

    if is_identical:
        print("RESULT: JARs are IDENTICAL")
    else:
        print("RESULT: JARs are DIFFERENT")


def print_comparison_report(results: dict) -> None:
    """Print a formatted comparison report."""
    _print_header(results)
    _print_summary(results)
    _print_unique_files(results)
    _print_different_files(results)
    _print_result(results)


def main():
    parser = argparse.ArgumentParser(
        description="Compare two JAR files recursively",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python jar_compare.py app-v1.jar app-v2.jar
  python jar_compare.py --verbose lib1.jar lib2.jar
        """,
    )

    parser.add_argument("jar1", help="First JAR file to compare")
    parser.add_argument("jar2", help="Second JAR file to compare")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()

    try:
        with JarComparator(args.jar1, args.jar2, args.verbose) as comparator:
            results = comparator.compare()
            print_comparison_report(results)

            # Exit with non-zero code if JARs are different
            is_different = (
                len(results["only_in_jar1"]) > 0
                or len(results["only_in_jar2"]) > 0
                or len(results["different_files"]) > 0
            )

            sys.exit(1 if is_different else 0)

    except (FileNotFoundError, ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
