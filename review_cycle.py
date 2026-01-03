#!/usr/bin/env python3
"""
Automated Claude Code review cycle script.
Runs 5 parallel reviewers, verifies findings, fixes issues, and commits.
"""

import subprocess
import sys
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

REVIEW_PROMPT = """
Spin up 5 independent staff software engineer reviewers using Task agents in parallel. Each reviewer should:

1. Review the architecture and code of this repository thoroughly
2. Grade the architecture and code between 0-10
3. Leave detailed comments explaining why they gave that grade
4. Provide a confidence score between 0-10 on how sure they are about issues found
5. Point to exact code locations (file:line) or specific design decisions that influenced their grading
6. Leave concrete suggestions on how to move the grade towards 10

Make sure all 5 reviewers run in parallel and cover different aspects:
- Reviewer 1: Focus on overall architecture and design patterns
- Reviewer 2: Focus on error handling, edge cases, and robustness
- Reviewer 3: Focus on performance, efficiency, and scalability
- Reviewer 4: Focus on security, input validation, and safety
- Reviewer 5: Focus on code quality, readability, and maintainability

Compile all their findings into a structured report.
"""

VERIFY_PROMPT = """
Review the assessments from the previous reviewers. Go through each comment and finding:

1. Do you agree with their assessments? Why or why not?
2. Verify each issue by examining the actual code
3. Mark findings as VERIFIED or DISPUTED with explanation
4. Prioritize verified issues by impact (HIGH/MEDIUM/LOW)
5. Create a consolidated list of verified issues to fix

Be critical - only verify issues that are legitimate problems.
"""

FIX_PROMPT = """
Fix all the verified issues from the previous assessment. For each fix:

1. Address the root cause, not just symptoms
2. Follow Rust best practices (private scope preferred, no unnecessary comments)
3. Ensure changes don't break existing functionality
4. Run cargo check and cargo clippy to verify fixes compile cleanly

Work through issues systematically from highest to lowest priority.
"""

COMMIT_PROMPT = """
Commit and push the changes:

1. Run cargo fmt to format code
2. Run cargo check to verify compilation
3. Run cargo clippy to check for warnings
4. Stage all changes with git add
5. Create a descriptive commit message summarizing the fixes
6. Push to the current branch

Do not include "Generated with Claude Code" in the commit message.
"""


def run_claude(prompt: str, session_id: str, is_first: bool = False) -> int:
    """Run claude code with the given prompt in a specific session."""
    if is_first:
        cmd = [
            "claude",
            "--session-id", session_id,
            "--dangerously-skip-permissions",
            "-p",
            prompt
        ]
    else:
        cmd = [
            "claude",
            "--resume", session_id,
            "--dangerously-skip-permissions",
            "-p",
            prompt
        ]

    log.info("Running Claude Code...")

    process = subprocess.Popen(
        cmd,
        cwd=Path(__file__).parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        print(line, end="", flush=True)

    process.wait()
    return process.returncode


def main():
    import uuid

    session_id = str(uuid.uuid4())
    cycle = 0
    is_first = True

    log.info(f"Starting review cycle with session: {session_id}")

    while True:
        cycle += 1
        log.info(f"{'='*60}")
        log.info(f"CYCLE {cycle}")
        log.info(f"{'='*60}")

        log.info("STEP 1: Running 5 parallel reviewers...")
        if run_claude(REVIEW_PROMPT, session_id, is_first) != 0:
            log.error("Review step failed")
            break
        is_first = False

        log.info("STEP 2: Verifying assessments...")
        if run_claude(VERIFY_PROMPT, session_id) != 0:
            log.error("Verification step failed")
            break

        log.info("STEP 3: Fixing verified issues...")
        if run_claude(FIX_PROMPT, session_id) != 0:
            log.error("Fix step failed")
            break

        log.info("STEP 4: Committing and pushing...")
        if run_claude(COMMIT_PROMPT, session_id) != 0:
            log.error("Commit step failed")
            break

        log.info(f"Cycle {cycle} complete!")

        response = input("\nContinue to next cycle? [Y/n]: ").strip().lower()
        if response == 'n':
            log.info("Exiting review cycle.")
            break


if __name__ == "__main__":
    main()
