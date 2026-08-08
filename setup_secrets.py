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
ensure_scope("database")
lakebase_url = getpass.getpass("Paste your Lakebase connection URL: ").strip()
if lakebase_url:
    w.secrets.put_secret(scope="database", key="lakebase-url", string_value=lakebase_url)
    print("stored database/lakebase-url")
grant_read("database")


# --- YouTube Data API v3 (needed from Stage 2 onward) ----------------------
ensure_scope("mealplan")
youtube_key = getpass.getpass("Paste your YouTube Data API key (Enter to skip): ").strip()
if youtube_key:
    w.secrets.put_secret(scope="mealplan", key="youtube-api-key", string_value=youtube_key)
    print("stored mealplan/youtube-api-key")
else:
    print("skipped YouTube key - re-run this script before Stage 2")
grant_read("mealplan")

print("\nDone.")
