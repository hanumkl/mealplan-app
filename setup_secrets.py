"""
One-time setup: create the Databricks secret scopes this project needs.

Run from a Databricks notebook (`%sh python setup_secrets.py`) or locally with
the Databricks CLI configured. Values are read via getpass, so nothing is
echoed to the terminal or written to shell history.

Stage 1 only needs the Lakebase URL. The YouTube key is prompted for too, but
you can press Enter to skip it and re-run this script before Stage 2.
"""

import getpass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

w = WorkspaceClient()


def ensure_scope(scope: str) -> None:
    """Create a secret scope, ignoring the error if it already exists."""
    try:
        w.secrets.create_scope(scope=scope)
        print(f"created scope: {scope}")
    except Exception as exc:  # already-exists is the common case here
        print(f"scope {scope}: {exc}")


def grant_read(scope: str) -> None:
    """Let the workspace `users` group read the scope (needed by Databricks Apps)."""
    try:
        w.secrets.put_acl(
            scope=scope,
            principal="users",
            permission=workspace.AclPermission.READ,
        )
    except Exception as exc:
        print(f"acl {scope}: {exc}")


# --- Lakebase (required for Stage 1) ---------------------------------------
#
# Deliberately `mealplan/lakebase-url`, not the bootcamp's `database/lakebase-url`.
# Every project in the course uses that same default name, so setting up a
# second one silently repoints this project at the wrong database - the
# notebooks then fail with "relation does not exist" against tables that
# plainly exist, which is a genuinely confusing hour to lose.
ensure_scope("mealplan")
lakebase_url = getpass.getpass("Paste your Lakebase connection URL: ").strip()
if lakebase_url:
    w.secrets.put_secret(scope="mealplan", key="lakebase-url", string_value=lakebase_url)
    print("stored mealplan/lakebase-url")


# --- YouTube Data API v3 (needed from Stage 2 onward) ----------------------
youtube_key = getpass.getpass("Paste your YouTube Data API key (Enter to skip): ").strip()
if youtube_key:
    w.secrets.put_secret(scope="mealplan", key="youtube-api-key", string_value=youtube_key)
    print("stored mealplan/youtube-api-key")
else:
    print("skipped YouTube key - re-run this script before Stage 2")
grant_read("mealplan")

print("\nDone.")
