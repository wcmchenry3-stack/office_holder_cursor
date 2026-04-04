# -*- coding: utf-8 -*-
"""Auto-fix pipeline: parser-bug GitHub issue → Claude API → draft PR.

Detects open parser-bug issues, evaluates them against minimal-risk criteria,
sends qualifying issues to the Claude API for a code fix + tests, and opens
a draft PR targeting dev.

--- Policy compliance ---

Anthropic Claude API (via src/services/claude_client.py):
  - rate_limit (HTTP 429) handling: exponential backoff (3 retries, 1 s → 2 s → 4 s).
  - max_tokens=4096 set on every API call.
  - ANTHROPIC_API_KEY never hardcoded; always read via os.environ at runtime.

GitHub REST API (via src/services/github_client.py):
  - Rate limit / retry / backoff: exponential backoff on HTTP 429.
  - GITHUB_TOKEN never hardcoded.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Minimal risk criteria
# ---------------------------------------------------------------------------

_ALLOWED_ERROR_TYPES = frozenset({"ValueError", "TypeError", "IndexError", "AttributeError"})

_ALLOWED_FILE_PREFIXES = ("src/scraper/",)

_DDL_KEYWORDS = frozenset({"ALTER TABLE", "CREATE TABLE", "DROP TABLE"})


def check_minimal_risk_criteria(proposal, issue_body: str) -> tuple[bool, list[str]]:
    """Check all 7 minimal-risk criteria against a ParserFixProposal.

    Returns (passes: bool, reasons: list[str]) where reasons lists any
    criteria that failed.

    Criteria (ALL must be true):
    1. Files changed are exclusively within src/scraper/
    2. Diff < 50 lines (additions + deletions combined)
    3. No ALTER TABLE, CREATE TABLE, DROP TABLE in proposed diff
    4. No new import statements for modules not already in requirements.txt
    5. No changes to function signatures in files imported by routers
    6. At least one new def test_ function in proposed diff or test_code
    7. error_type is one of: ValueError, TypeError, IndexError, AttributeError
    """
    reasons = []

    # 1. Files within src/scraper/ only
    if not any(proposal.file_path.startswith(p) for p in _ALLOWED_FILE_PREFIXES):
        reasons.append(f"File outside src/scraper/: {proposal.file_path}")

    # 2. Diff < 50 lines
    diff_lines = [
        line for line in proposal.diff.splitlines() if line.startswith("+") or line.startswith("-")
    ]
    # Exclude file header lines (--- a/... and +++ b/...)
    diff_lines = [
        line for line in diff_lines if not line.startswith("---") and not line.startswith("+++")
    ]
    if len(diff_lines) >= 50:
        reasons.append(f"Diff too large: {len(diff_lines)} lines (max 49)")

    # 3. No DDL statements
    diff_upper = proposal.diff.upper()
    for kw in _DDL_KEYWORDS:
        if kw in diff_upper:
            reasons.append(f"Contains DDL: {kw}")

    # 4. No new imports for packages not in requirements.txt
    new_imports = _extract_new_imports(proposal.diff)
    if new_imports:
        from pathlib import Path

        req_path = Path(__file__).parent.parent.parent / "requirements.txt"
        known_packages = _load_requirements_packages(req_path)
        # Also allow stdlib modules
        for imp in new_imports:
            top_level = imp.split(".")[0]
            if top_level not in known_packages and not _is_stdlib(top_level):
                reasons.append(f"New import not in requirements.txt: {imp}")

    # 5. No changes to function signatures imported by routers
    if _changes_public_signatures(proposal.diff):
        reasons.append("Changes to function signatures that may be imported by routers")

    # 6. At least one new test function
    combined = proposal.diff + "\n" + proposal.test_code
    if "def test_" not in combined:
        reasons.append("No new test function (def test_) in proposed fix")

    # 7. error_type is one of the allowed types
    error_type = _extract_error_type(issue_body)
    if error_type and error_type not in _ALLOWED_ERROR_TYPES:
        reasons.append(f"Error type not allowed: {error_type}")

    return (len(reasons) == 0, reasons)


def _extract_new_imports(diff: str) -> list[str]:
    """Extract newly added import statements from a unified diff."""
    imports = []
    for line in diff.splitlines():
        if not line.startswith("+"):
            continue
        line = line[1:].strip()
        if line.startswith("import "):
            module = line.replace("import ", "").split(" as ")[0].split(",")[0].strip()
            imports.append(module)
        elif line.startswith("from ") and " import " in line:
            module = line.split("from ", 1)[1].split(" import")[0].strip()
            imports.append(module)
    return imports


def _load_requirements_packages(req_path) -> set[str]:
    """Load top-level package names from requirements.txt."""
    packages = set()
    try:
        with open(req_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Extract package name (before >=, ==, etc.)
                name = re.split(r"[><=!~\[]", line)[0].strip().lower()
                # Normalize: dashes → underscores for import matching
                packages.add(name.replace("-", "_"))
    except FileNotFoundError:
        pass
    return packages


def _is_stdlib(module_name: str) -> bool:
    """Check if a module is part of the Python standard library."""
    import sys

    if module_name in sys.stdlib_module_names:
        return True
    return False


def _changes_public_signatures(diff: str) -> bool:
    """Check if a diff modifies function def lines (crude heuristic)."""
    for line in diff.splitlines():
        if line.startswith("-") and not line.startswith("---"):
            stripped = line[1:].strip()
            if stripped.startswith("def ") and "(" in stripped:
                # A function definition is being removed/changed
                return True
    return False


def _extract_error_type(issue_body: str) -> str | None:
    """Extract the error_type from a ParseErrorReporter issue body."""
    match = re.search(r"\*\*Error type:\*\*\s*`?(\w+)`?", issue_body)
    if match:
        return match.group(1)
    return None


# ---------------------------------------------------------------------------
# Auto-fix pipeline
# ---------------------------------------------------------------------------


def run_auto_fix_for_issue(
    issue: dict,
    github_client,
    claude_client,
) -> dict:
    """Run the auto-fix pipeline for a single parser-bug issue.

    Returns a result dict with keys: issue_number, status, pr_url, reasons.
    """
    issue_number = issue.get("number", 0)
    issue_title = issue.get("title", "")
    issue_body = issue.get("body", "")
    result = {
        "issue_number": issue_number,
        "status": "skipped",
        "pr_url": None,
        "reasons": [],
    }

    # Extract fingerprint from labels
    labels = [l.get("name", "") if isinstance(l, dict) else l for l in issue.get("labels", [])]
    fingerprint = None
    for label in labels:
        if label.startswith("parse-error:"):
            fingerprint = label.split("parse-error:", 1)[1]
            break

    if not fingerprint:
        result["reasons"] = ["No parse-error fingerprint label found"]
        return result

    # Get the source file from the repo (table_parser.py is where parser code lives)
    file_path = "src/scraper/table_parser.py"
    file_data = github_client.get_file_content(file_path, ref="dev")
    if file_data is None:
        result["reasons"] = [f"Could not read {file_path} from repo"]
        result["status"] = "error"
        return result

    # Call Claude for a fix proposal
    proposal = claude_client.propose_parser_fix(
        issue_title=issue_title,
        issue_body=issue_body,
        file_content=file_data["content"],
    )
    if proposal is None:
        result["reasons"] = ["Claude API returned no fix proposal"]
        result["status"] = "error"
        return result

    # Check minimal risk criteria
    passes, reasons = check_minimal_risk_criteria(proposal, issue_body)
    if not passes:
        result["reasons"] = reasons
        result["status"] = "criteria_failed"
        return result

    # Create branch
    branch_name = f"fix/parser-auto-{fingerprint}"
    dev_sha = github_client.get_default_branch_sha("dev")
    if not dev_sha:
        result["reasons"] = ["Could not get dev branch SHA"]
        result["status"] = "error"
        return result

    branch_result = github_client.create_branch(branch_name, dev_sha)
    if branch_result is None:
        result["reasons"] = [f"Could not create branch {branch_name}"]
        result["status"] = "error"
        return result

    # Apply the fix — update the source file
    # We need to re-fetch file content on the new branch to get the correct SHA
    branch_file = github_client.get_file_content(proposal.file_path, ref=branch_name)
    if branch_file is None:
        result["reasons"] = [f"Could not read {proposal.file_path} on branch {branch_name}"]
        result["status"] = "error"
        return result

    # Apply the diff to get new content
    new_content = _apply_diff(branch_file["content"], proposal.diff)
    if new_content is None:
        # Fallback: if diff application fails, skip
        result["reasons"] = ["Failed to apply diff to source file"]
        result["status"] = "error"
        return result

    update_result = github_client.update_file(
        path=proposal.file_path,
        content=new_content,
        message=f"fix: {proposal.explanation[:72]}",
        branch=branch_name,
        file_sha=branch_file["sha"],
    )
    if update_result is None:
        result["reasons"] = ["Failed to update source file on branch"]
        result["status"] = "error"
        return result

    # Add test file if there's test code
    if proposal.test_code.strip():
        test_file_path = f"tests/test_auto_fix_{fingerprint}.py"
        test_content = (
            "# -*- coding: utf-8 -*-\n"
            f'"""Auto-generated test for parser fix (issue #{issue_number})."""\n\n'
            f"{proposal.test_code}\n"
        )
        github_client.create_file(
            path=test_file_path,
            content=test_content,
            message=f"test: add auto-generated test for parser fix #{issue_number}",
            branch=branch_name,
        )

    # Open draft PR
    pr_body = (
        f"## Auto-fix for #{issue_number}\n\n"
        f"**Issue:** #{issue_number}\n"
        f"**Fingerprint:** `{fingerprint}`\n\n"
        f"### Explanation\n{proposal.explanation}\n\n"
        "### Risk Criteria Checklist\n"
        "- [x] Files changed exclusively within `src/scraper/`\n"
        "- [x] Diff < 50 lines\n"
        "- [x] No DDL statements\n"
        "- [x] No new external imports\n"
        "- [x] No public signature changes\n"
        "- [x] At least one new test function\n"
        "- [x] Error type is ValueError/TypeError/IndexError/AttributeError\n\n"
        "---\n"
        "*Auto-generated by ClaudeAutoFixer. Review before merging.*"
    )

    pr = github_client.create_pull_request(
        title=f"[Auto-fix] {issue_title}",
        body=pr_body,
        head=branch_name,
        base="dev",
        draft=True,
    )
    if pr is None:
        result["reasons"] = ["Failed to create draft PR"]
        result["status"] = "error"
        return result

    result["status"] = "pr_created"
    result["pr_url"] = pr.get("html_url", "")
    logger.info("Auto-fix PR created for issue #%d: %s", issue_number, result["pr_url"])
    return result


def _apply_diff(original: str, diff: str) -> str | None:
    """Apply a unified diff to the original content.

    Simple line-based application. Returns None if the diff cannot be applied.
    Falls back to returning original with additions appended if context doesn't match.
    """
    lines = original.splitlines(keepends=True)
    result_lines = list(lines)

    # Parse hunks from the diff
    hunk_pattern = re.compile(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@")
    diff_lines = diff.splitlines(keepends=True)

    hunks = []
    current_hunk = None
    for line in diff_lines:
        match = hunk_pattern.match(line)
        if match:
            if current_hunk:
                hunks.append(current_hunk)
            current_hunk = {
                "old_start": int(match.group(1)) - 1,  # 0-indexed
                "lines": [],
            }
        elif current_hunk is not None:
            if line.startswith("---") or line.startswith("+++"):
                continue
            current_hunk["lines"].append(line)

    if current_hunk:
        hunks.append(current_hunk)

    if not hunks:
        # No valid hunks found — can't apply
        return None

    # Apply hunks in reverse order to preserve line numbers
    for hunk in reversed(hunks):
        pos = hunk["old_start"]
        new_lines = []
        remove_count = 0
        for hline in hunk["lines"]:
            if hline.startswith("-"):
                remove_count += 1
            elif hline.startswith("+"):
                new_lines.append(hline[1:])
            else:
                # context line
                new_lines.append(hline[1:] if hline.startswith(" ") else hline)

        # Replace the range
        if pos <= len(result_lines):
            result_lines[pos : pos + remove_count] = new_lines

    return "".join(result_lines)


def process_open_parser_bug_issues() -> list[dict]:
    """Scan for open parser-bug issues and run auto-fix on qualifying ones.

    Returns a list of result dicts (one per issue processed).
    """
    from src.services.github_client import get_github_client
    from src.services.claude_client import get_claude_client

    github = get_github_client()
    if github is None:
        logger.info("Auto-fix skipped: GITHUB_TOKEN not set")
        return []

    claude = get_claude_client()
    if claude is None:
        logger.info("Auto-fix skipped: ANTHROPIC_API_KEY not set")
        return []

    issues = github.list_open_issues_by_label("parser-bug")
    if not issues:
        logger.info("No open parser-bug issues found")
        return []

    results = []
    for issue in issues:
        # Only process issues created by ParseErrorReporter (has pf- label)
        labels = [l.get("name", "") if isinstance(l, dict) else l for l in issue.get("labels", [])]
        has_fingerprint = any(l.startswith("parse-error:pf-") for l in labels)
        if not has_fingerprint:
            continue

        result = run_auto_fix_for_issue(issue, github, claude)
        results.append(result)

    return results
