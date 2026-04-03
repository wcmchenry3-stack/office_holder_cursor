#!/usr/bin/env bash
# policy-gate.sh — PreToolUse hook for Claude Code
# Gates `gh pr create` when changed files match API policy detection patterns.
#
# Flow:
#   1. First run: detects policy-relevant changes → writes .claude/policy-review.pending → BLOCKS
#   2. Policy-compliance agent reviews → writes .claude/policy-review.passed (with commit SHA)
#   3. Second run: sees .passed stamp matches HEAD → PASSES
#
# The .passed stamp is invalidated when HEAD changes (new commits).
set -uo pipefail

# ── Read tool input from stdin ───────────────────────────────────
INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty' 2>/dev/null)

# Only gate on PR-creation commands
if ! echo "$COMMAND" | grep -qE 'gh\s+pr\s+create'; then
  exit 0
fi

# ── Locate policy-patterns.json ──────────────────────────────────
PATTERNS_FILE=".claude/policies/policy-patterns.json"
if [ ! -f "$PATTERNS_FILE" ]; then
  exit 0
fi

STAMP_FILE=".claude/policy-review.passed"
HEAD_SHA=$(git rev-parse HEAD 2>/dev/null || echo "unknown")

# ── Check if a valid review stamp exists ─────────────────────────
if [ -f "$STAMP_FILE" ]; then
  STAMP_SHA=$(head -1 "$STAMP_FILE" 2>/dev/null || echo "")
  if [ "$STAMP_SHA" = "$HEAD_SHA" ]; then
    # Review was done for this exact commit — allow through
    exit 0
  fi
fi

# ── Get changed files ────────────────────────────────────────────
CHANGED_FILES=$(git diff --name-only origin/main...HEAD 2>/dev/null || \
                git diff --name-only HEAD~1 HEAD 2>/dev/null || \
                echo "")

if [ -z "$CHANGED_FILES" ]; then
  exit 0
fi

# ── Check each policy's detection patterns ───────────────────────
TRIGGERED=""

for POLICY in $(jq -r 'keys[]' "$PATTERNS_FILE"); do
  DETECT=$(jq -r --arg p "$POLICY" '.[$p].detect' "$PATTERNS_FILE")
  SKIP=$(jq -r --arg p "$POLICY" '.[$p].skip // empty' "$PATTERNS_FILE")

  while IFS= read -r file; do
    # Skip .claude/ directory
    case "$file" in .claude/*) continue ;; esac

    # Skip files matching the skip pattern
    if [ -n "$SKIP" ] && echo "$(basename "$file")" | grep -qE "$SKIP"; then
      continue
    fi

    # Check if file exists and matches detection pattern
    if [ -f "$file" ] && grep -qE "$DETECT" "$file" 2>/dev/null; then
      TRIGGERED+="  - $POLICY → $file\n"
      break
    fi
  done <<< "$CHANGED_FILES"
done

# ── Verdict ──────────────────────────────────────────────────────
if [ -z "$TRIGGERED" ]; then
  exit 0
fi

# Write pending file so the compliance agent knows what to review
echo -e "$TRIGGERED" > .claude/policy-review.pending

{
  echo "BLOCKED: Policy-relevant files changed. Run the policy-compliance agent, then retry."
  echo ""
  echo "Triggered policies:"
  echo -e "$TRIGGERED"
  echo "After review, the agent should run:  echo '$HEAD_SHA' > .claude/policy-review.passed"
} >&2
exit 2
