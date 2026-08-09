"""
Mealplan MCP server (FastMCP, streamable HTTP).

Deployed as its own Databricks App and registered with Agent Bricks as an
external MCP server - the Day 3 pattern.

Every function here is a thin wrapper. All SQL, unit conversion and ranking
logic lives in `mealplan_store.py`, the same split as Day 3's
alpaca_mcp_server.py / alpaca_broker.py.

Eight tools. Five read, three WRITE:
    read   get_household_profile, search_recipes, get_recipe_details,
           get_cooking_history, suggest_todays_meal
    write  create_meal_plan, log_cooked_meal, build_grocery_list

The write tools are the point. An agent that only answers questions doesn't
close any loop; this one commits a plan, records what really happened, and
prices the shopping.
"""

from __future__ import annotations

import logging
import os

# FastMCP moved between releases: it ships inside `mcp` 1.x (the Day 3
# pattern), and as a standalone `fastmcp` package from mcp 2.0 onward.
# requirements.txt pins 1.x, but accept either so the server doesn't die on an
# image that resolved differently.
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:  # pragma: no cover
    from fastmcp import FastMCP

import mealplan_store as store

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mealplan-mcp")

mcp = FastMCP("mealplan")


def _safe(fn, **kwargs) -> dict:
    """Return a clean error dict instead of a stack trace.

    The agent can act on {"error": "..."} - it can ask the user to clarify or
    try a different tool. A traceback just ends the conversation.
    """
    try:
        return fn(**kwargs)
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s failed", getattr(fn, "__name__", fn))
        return {"error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# read tools
# ---------------------------------------------------------------------------

@mcp.tool()
def get_household_profile() -> dict:
    """Who is being cooked for, and what must never be served.

    Call this first when planning anything. It returns each member's calorie
    and protein targets plus the household's dietary restrictions, split into
    strict (absolute, never violate) and preference (tradeable).

    Returns:
        dict: members, strict_restrictions, preferences,
              requires_split_protein (true when a vegetarian and a meat-eater
              share the table, meaning the base dish must be vegetarian with
              meat added per person), and the cooking model.
    """
    return _safe(store.get_household_profile)


@mcp.tool()
def search_recipes(query: str, limit: int = 5) -> dict:
    """Find recipes by describing the dish in plain language.

    Semantic search over recipes harvested from YouTube, so "warming coconut
    chicken" matches dishes that never use those words - including Finnish and
    Indonesian titles. Falls back to keyword matching if the embedding model
    is unavailable; the response says which mode was used.

    Recipes violating a strict household restriction are excluded in SQL, so
    they never reach you.

    Args:
        query: Plain-language description, e.g. "quick vegetarian weeknight".
        limit: Maximum results, 1-15. Defaults to 5.

    Returns:
        dict: mode ('semantic' or 'keyword'), count, and results with
              recipe_id, title, cuisine, duration_min and dietary flags.
              Use the recipe_id when planning - never invent one.
    """
    return _safe(store.search_recipes, query=query, limit=limit)


@mcp.tool()
def get_recipe_details(recipe_id: int, servings: float = None) -> dict:
    """Full recipe scaled to a serving count, with nutrition and cost.

    Quantities scale by class: most ingredients linearly, spices and oil by
    factor^0.8 (tripling a recipe and tripling the chilli makes it inedible),
    and things like a bay leaf not at all.

    Args:
        recipe_id: From search_recipes.
        servings: Scale to this many. Omit for the recipe's own base servings.

    Returns:
        dict: recipe, ingredients with scaled_quantity, totals (kcal,
              protein_g, cost_eur), per_serving, and `coverage` - how many
              ingredients those totals are actually based on. Check
              `is_complete`; when false, say the numbers are partial rather
              than quoting them as fact.
    """
    return _safe(store.get_recipe_details, recipe_id=recipe_id, servings=servings)


@mcp.tool()
def get_cooking_history(limit: int = 14) -> dict:
    """What was actually cooked recently, and why plans were abandoned.

    Use this before planning: it shows which dishes were cooked as planned,
    which were swapped, and the household's own words about why. That free
    text is the most useful signal available for planning a week they will
    actually cook.

    Args:
        limit: How many days back, 1-60. Defaults to 14.

    Returns:
        dict: entries with cooked_date, planned, actual, was_planned,
              deviation_reason, mood_note, rating.
    """
    result = _safe(store.get_cooking_history, limit=limit)
    return result if isinstance(result, dict) else {"entries": result}


@mcp.tool()
def suggest_todays_meal(max_minutes: int = None, avoid_recent_days: int = 10) -> dict:
    """Recommend one dish for today, with the reasoning made explicit.

    This is a judgement call, not a passthrough of a search. It applies, in
    order:
      1. Strict dietary restrictions - enforced in SQL, never negotiable.
      2. Excludes anything cooked in the last `avoid_recent_days` days,
         because repetition is the usual reason meal plans get abandoned.
      3. Drops recipes longer than `max_minutes`, if given.
      4. Ranks what remains by how closely one serving matches the household's
         average per-meal calorie need - taking one dish as covering lunch and
         dinner, about 65% of a day's calories.

    Args:
        max_minutes: Optional cap on cooking time.
        avoid_recent_days: Repeat-avoidance window. Defaults to 10.

    Returns:
        dict: suggestion, reason (the specific numbers behind the pick),
              rules_applied, and alternatives. If no candidate had complete
              nutrition data it says so and picks on constraints alone -
              relay that caveat rather than implying the calories were checked.
    """
    return _safe(store.suggest_todays_meal, max_minutes=max_minutes,
                 avoid_recent_days=avoid_recent_days)


# ---------------------------------------------------------------------------
# write tools
# ---------------------------------------------------------------------------

@mcp.tool()
def create_meal_plan(week_start: str, days: list[dict], rationale: str = None) -> dict:
    """WRITE. Commit a week's meal plan to the database.

    One dish per day - that dish covers both lunch and dinner. Seven entries
    is a full week. Re-planning the same week replaces its days rather than
    adding duplicates.

    Only recipe_ids returned by search_recipes or suggest_todays_meal are
    accepted; anything else is refused rather than written, because a plan
    pointing at a recipe that doesn't exist is worse than an error.

    Args:
        week_start: Monday of the week, 'YYYY-MM-DD'.
        days: List of {"plan_date": "YYYY-MM-DD", "recipe_id": int,
              "servings": float (optional), "notes": str (optional)}.
        rationale: Why the week looks like this. Shown to the household, so
                   write it for them, not for the log.

    Returns:
        dict: plan_id, week_start, days_written - or {"error": ...} listing
              any recipe_ids that were rejected.
    """
    return _safe(store.create_meal_plan, week_start=week_start, days=days,
                 rationale=rationale)


@mcp.tool()
def log_cooked_meal(cooked_date: str, actual_recipe_id: int = None,
                    actual_freetext: str = None, deviation_reason: str = None,
                    mood_note: str = None, rating: int = None) -> dict:
    """WRITE. Record what was actually cooked on a given day.

    Use this whenever the household says they cooked something different,
    skipped cooking, or ordered in. Capture `deviation_reason` in their own
    words - that text gets embedded and becomes retrievable context, so it
    matters more than the dish name. Logging the same date twice updates the
    entry rather than duplicating it.

    Args:
        cooked_date: 'YYYY-MM-DD'.
        actual_recipe_id: If it was a catalogue recipe.
        actual_freetext: What they made, if it wasn't ("instant noodles").
        deviation_reason: Why it differed, in their words.
        mood_note: Any mood or context they mention.
        rating: 1-5, if given.

    Returns:
        dict: log_id, cooked_date, matched_the_plan.
    """
    return _safe(store.log_cooked_meal, cooked_date=cooked_date,
                 actual_recipe_id=actual_recipe_id,
                 actual_freetext=actual_freetext,
                 deviation_reason=deviation_reason, mood_note=mood_note,
                 rating=rating)


@mcp.tool()
def build_grocery_list(week_start: str) -> dict:
    """WRITE. Build a priced shopping list for a committed plan.

    Aggregates every ingredient across the week (so chicken appearing on three
    days becomes one line), prices it from the household's own receipt data,
    and groups the total by store.

    Prices are estimates from past receipts, not live shelf prices, and
    anything that couldn't be priced is excluded from the total - so the total
    is a floor. Say that when you report it.

    Args:
        week_start: Monday of a week that already has a plan, 'YYYY-MM-DD'.

    Returns:
        dict: list_id, distinct_items, estimated_total_eur, by_store,
              unpriced_items, coverage_note.
    """
    return _safe(store.build_grocery_list, week_start=week_start)


# ---------------------------------------------------------------------------

@mcp.tool()
def health_check() -> dict:
    """Confirm the server can reach Lakebase, and how much data is there.

    Useful as a first call when something looks wrong - it distinguishes "the
    database is empty" from "the server can't connect".
    """
    def _check():
        counts = store._one(
            """SELECT (SELECT COUNT(*) FROM recipes WHERE review_status='approved')
                        AS approved_recipes,
                      (SELECT COUNT(*) FROM ingredients) AS ingredients,
                      (SELECT COUNT(*) FROM recipe_embeddings) AS recipe_vectors,
                      (SELECT COUNT(*) FROM cooking_log) AS cooking_log"""
        )
        return {"status": "ok", "counts": counts}
    return _safe(_check)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    logger.info("mealplan MCP server on :%s (streamable-http)", port)
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = port
    mcp.run(transport="streamable-http")
