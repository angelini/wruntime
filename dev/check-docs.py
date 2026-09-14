#!/usr/bin/env python3
"""Validate local Markdown links, anchors, and fenced TOML snippets."""

from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from collections import Counter
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parent.parent
LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
HEADING_RE = re.compile(r"^ {0,3}(#{1,6})\s+(.+?)\s*#*\s*$")
EXPLICIT_ID_RE = re.compile(r"\bid=[\"']([^\"']+)[\"']")
FENCE_RE = re.compile(r"^```([^\s`]*)\s*$")
HTML_TAG_RE = re.compile(r"<[^>]+>")
PUNCT_RE = re.compile(r"[^\w\- ]", re.UNICODE)


def markdown_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard", "--", "*.md"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return sorted(
        path
        for path in {ROOT / name for name in result.stdout.splitlines()}
        if path.is_file()
    )


def slug(text: str) -> str:
    text = HTML_TAG_RE.sub("", text)
    text = text.replace("`", "").strip().lower()
    text = PUNCT_RE.sub("", text)
    return text.replace(" ", "-")


def anchors(path: Path) -> set[str]:
    found: set[str] = set()
    seen: Counter[str] = Counter()
    in_fence = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        for explicit in EXPLICIT_ID_RE.findall(line):
            found.add(explicit)
        match = HEADING_RE.match(line)
        if not match:
            continue
        base = slug(match.group(2))
        if not base:
            continue
        count = seen[base]
        seen[base] += 1
        found.add(base if count == 0 else f"{base}-{count}")
    return found


def destination(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("<") and ">" in raw:
        return raw[1 : raw.index(">")]
    return raw.split(maxsplit=1)[0]


def validate_links(files: list[Path]) -> list[str]:
    errors: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}
    for source in files:
        text = source.read_text(encoding="utf-8")
        for line_number, line in enumerate(text.splitlines(), 1):
            for raw in LINK_RE.findall(line):
                target = destination(raw)
                parsed = urlsplit(target)
                if parsed.scheme or target.startswith("//"):
                    continue
                path_text = unquote(parsed.path)
                fragment = unquote(parsed.fragment)
                if not path_text:
                    resolved = source
                elif path_text.startswith("/"):
                    resolved = ROOT / path_text.lstrip("/")
                else:
                    resolved = (source.parent / path_text).resolve()
                try:
                    resolved.relative_to(ROOT)
                except ValueError:
                    errors.append(
                        f"{source.relative_to(ROOT)}:{line_number}: link escapes repository: {target}"
                    )
                    continue
                if not resolved.exists():
                    errors.append(
                        f"{source.relative_to(ROOT)}:{line_number}: missing link target: {target}"
                    )
                    continue
                if fragment and resolved.is_file() and resolved.suffix.lower() == ".md":
                    available = anchor_cache.setdefault(resolved, anchors(resolved))
                    if fragment not in available:
                        errors.append(
                            f"{source.relative_to(ROOT)}:{line_number}: missing anchor #{fragment} in "
                            f"{resolved.relative_to(ROOT)}"
                        )
    return errors


def validate_toml(files: list[Path]) -> list[str]:
    errors: list[str] = []
    for source in files:
        language = ""
        block_start = 0
        block: list[str] = []
        for line_number, line in enumerate(
            source.read_text(encoding="utf-8").splitlines(), 1
        ):
            fence = FENCE_RE.match(line)
            if fence:
                if language:
                    if language == "toml":
                        try:
                            tomllib.loads("\n".join(block))
                        except tomllib.TOMLDecodeError as error:
                            errors.append(
                                f"{source.relative_to(ROOT)}:{block_start}: invalid TOML: {error}"
                            )
                    language = ""
                    block = []
                else:
                    language = fence.group(1).lower()
                    block_start = line_number + 1
                continue
            if language:
                block.append(line)
        if language:
            errors.append(
                f"{source.relative_to(ROOT)}:{block_start - 1}: unterminated fenced block"
            )
    return errors


def main() -> int:
    files = markdown_files()
    errors = validate_links(files) + validate_toml(files)
    if errors:
        print("documentation validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1
    print(f"validated {len(files)} Markdown files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
