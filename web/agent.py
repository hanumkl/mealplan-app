"""
Stage 3 - the meal-planning agent.

Tools that read *and* write. The write half is the point: an assistant that
only answers questions doesn't close any loop. This one commits a week's plan,
records what was actually cooked, and builds a priced grocery list - all real
rows in Lakebase that the rest of the app then reads.

The loop it closes:
    plan -> cook -> log what really happened -> that text gets embedded ->
    next week's plan retrieves it as context.

Runs on a Databricks Foundation Model endpoint. On Free Edition that means
databricks-llama-4-maverick, which is also the only multimodal one.
"""

from __future__ import annotations

import json
import logging
import os

import embeddings
from lakebase import run_one, run_query, run_returning, run_write
from units import price_for_grams, to_grams

logger = logging.getLogger("mealplan.agent")

AGENT_ENDPOINT = os.environ.get("AGENT_ENDPOINT", "databricks-llama-4-maverick")
MAX_STEPS = int(os.environ.get("AGENT_MAX_STEPS", "6"))

SYSTEM_PROMPT = """You are the meal planner for a family in Finland.

THE COOKING MODEL - this is not negotiable:
One cooking session each morning. That single dish covers BOTH lunch and dinner
the same day. A week is 7 dishes, not 21. Never propose separate lunch and
dinner meals.

ONE POT, DIFFERENT PLATES:
Members have different calorie goals. You do not cook different meals for them -
you vary portion size, and add protein per person. If someone is vegetarian and
someone is bulking on meat, the base dish is vegetarian and the meat is a
per-person add-on.

HARD RULES:
- Strict restrictions are absolute. Never plan a dish that violates one, and
  never argue with one.
- A recipe marked 'halal unverified' has no pork or alcohol in its ingredient
  list, but nothing confirms how its meat was sourced. Say so plainly when it
  matters rather than implying it is approved.
- Never invent a recipe. Only plan recipes returned by search_recipes, using
  their real recipe_id.
- Nutrition numbers are often partial because not every ingredient matched the
  price catalogue. When you quote a number that is based on partial data, say
  so.

STYLE:
Be brief and concrete. The user is cooking, not reading. Before writing
anything to the database, say what you are about to do in one line.
"""

# ---------------------------------------------------------------------------
# tool schemas
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_household",
            "description": "Who is being cooked for: members, calorie/protein "
                           "goals, and dietary restrictions split into strict "
                           "(absolute) and preference (tradeable). Call this "
                           "first when planning.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_recipes",
            "description": "Semantic search over the harvested recipe "
                           "catalogue. Describe the dish in plain language. "
                           "Automatically excludes anything violating the "
                           "household's strict restrictions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string",
                              "description": "e.g. 'warming coconut chicken', "
                                             "'quick vegetarian weeknight'"},
                    "limit": {"type": "integer", "description": "default 5"},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_recipe",
            "description": "Full detail for one recipe: ingredients, calories "
                           "and protein per serving, cost to cook, and how "
                           "complete those numbers are.",
            "parameters": {
                "type": "object",
                "properties": {
                    "recipe_id": {"type": "integer"},
                    "servings": {"type": "number",
                                 "description": "scale to this many servings"},
                },
                "required": ["recipe_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_cooking_history",
            "description": "What was actually cooked recently, including the "
                           "reasons meals were swapped. Use this to avoid "
                           "repeating dishes and to learn what gets abandoned.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer",
                                         "description": "default 14"}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_meal_plan",
            "description": "WRITE. Commit a week's plan: one dish per day. "
                           "Replaces any existing plan for that week.",
            "parameters": {
                "type": "object",
                "properties": {
                    "week_start": {"type": "string",
                                   "description": "Monday, YYYY-MM-DD"},
                    "rationale": {"type": "string",
                                  "description": "why this week looks like "
                                                 "this - shown to the user"},
                    "days": {
                        "type": "array",
                        "description": "one entry per cooking day",
                        "items": {
                            "type": "object",
                            "properties": {
                                "plan_date": {"type": "string",
                                              "description": "YYYY-MM-DD"},
                                "recipe_id": {"type": "integer"},
                                "servings": {"type": "number"},
                                "notes": {"type": "string"},
                            },
                            "required": ["plan_date", "recipe_id"],
                        },
                    },
                },
                "required": ["week_start", "days"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "log_cooked",
            "description": "WRITE. Record what was actually cooked on a day. "
                           "Use this when the user says they made something "
                           "different, or skipped cooking. The reason matters "
                           "more than the dish - it is what future planning "
                           "learns from.",
            "parameters": {
                "type": "object",
                "properties": {
                    "cooked_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "actual_recipe_id": {"type": "integer",
                                         "description": "omit if it wasn't a "
                                                        "catalogue recipe"},
                    "actual_freetext": {"type": "string",
                                        "description": "what they made, if not "
                                                       "a catalogue recipe"},
                    "deviation_reason": {"type": "string",
                                         "description": "why it differed from "
                                                        "the plan, in their words"},
                    "mood_note": {"type": "string"},
                    "rating": {"type": "integer", "description": "1-5"},
                },
                "required": ["cooked_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "build_grocery_list",
            "description": "WRITE. Build a priced shopping list for a committed "
                           "plan, aggregated across the week and grouped by "
                           "store.",
            "parameters": {
                "type": "object",
                "properties": {"week_start": {"type": "string",
                                              "description": "YYYY-MM-DD"}},
                "required": ["week_start"],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# tool implementations
# ---------------------------------------------------------------------------

RESTRICTION_SQL = {
    "vegetarian":   "r.is_vegetarian IS TRUE",
    "vegan":        "r.is_vegan IS TRUE",
    "halal":        "r.contains_pork IS NOT TRUE AND r.halal_status <> 'contains_flagged'",
    "no_pork":      "r.contains_pork IS NOT TRUE",
    "gluten_free":  "r.contains_gluten IS NOT TRUE",
    "lactose_free": "r.contains_lactose IS NOT TRUE",
}


def _strict_clauses(household_id: int):
    rows = run_query(
        """
        SELECT DISTINCT r.restriction
        FROM member_restrictions r
        JOIN members m ON m.member_id = r.member_id
        WHERE m.household_id = %s AND r.severity = 'strict'
        """,
        (household_id,),
    )
    clauses, ignored = [], []
    for row in rows:
        key = row["restriction"]
        (clauses if key in RESTRICTION_SQL else ignored).append(
            RESTRICTION_SQL.get(key, key))
    return clauses, ignored


def tool_get_household(household_id: int, **_):
    members = run_query(
        """
        SELECT m.member_id, m.name, m.role, m.birth_year, m.activity_level,
               g.goal_type, g.target_kcal, g.target_protein_g
        FROM members m
        LEFT JOIN member_goals g ON g.member_id = m.member_id AND g.is_active
        WHERE m.household_id = %s ORDER BY m.member_id
        """,
        (household_id,),
    )
    restrictions = run_query(
        """
        SELECT m.name, r.restriction, r.severity
        FROM member_restrictions r
        JOIN members m ON m.member_id = r.member_id
        WHERE m.household_id = %s
        """,
        (household_id,),
    )
    strict = [r for r in restrictions if r["severity"] == "strict"]
    return {
        "members": members,
        "strict_restrictions": strict,
        "preferences": [r for r in restrictions if r["severity"] != "strict"],
        "requires_split_protein": any(
            r["restriction"] in ("vegetarian", "vegan") for r in strict),
        "cooking_sessions_per_week": 7,
    }


def tool_search_recipes(household_id: int, query: str, limit: int = 5, **_):
    limit = max(1, min(int(limit or 5), 15))
    clauses, ignored = _strict_clauses(household_id)
    filter_sql = (" AND " + " AND ".join(clauses)) if clauses else ""

    if not embeddings.available():
        rows = run_query(
            f"""SELECT r.recipe_id, r.title, r.cuisine, r.duration_min,
                       r.halal_status, r.is_vegetarian, r.contains_pork
                FROM recipes r
                WHERE r.review_status = 'approved'
                  AND (r.title ILIKE %s OR r.description ILIKE %s){filter_sql}
                LIMIT %s""",
            (f"%{query}%", f"%{query}%", limit),
        )
        return {"mode": "keyword", "results": rows,
                "unenforced_restrictions": ignored}

    vec = embeddings.vector_literal(embeddings.embed_query(query))
    rows = run_query(
        f"""SELECT r.recipe_id, r.title, r.cuisine, r.duration_min,
                   r.base_servings, r.halal_status, r.is_vegetarian,
                   r.is_vegan, r.contains_pork, r.contains_gluten,
                   r.contains_lactose,
                   round((1 - (e.embedding <=> %s::vector))::numeric, 3) AS similarity
            FROM recipe_embeddings e
            JOIN recipes r ON r.recipe_id = e.recipe_id
            WHERE r.review_status = 'approved'{filter_sql}
            ORDER BY e.embedding <=> %s::vector
            LIMIT %s""",
        (vec, vec, limit),
    )
    return {"mode": "semantic", "results": rows,
            "unenforced_restrictions": ignored}


def tool_get_recipe(household_id: int, recipe_id: int, servings: float = None, **_):
    # Imported here to avoid a circular import at module load.
    from app import recipe_nutrition_payload
    return recipe_nutrition_payload(recipe_id, servings)


def tool_get_cooking_history(household_id: int, limit: int = 14, **_):
    return run_query(
        """
        SELECT l.cooked_date, l.was_planned, l.deviation_reason, l.mood_note,
               l.rating, l.cook_minutes,
               p.title AS planned, a.title AS actual, l.actual_freetext
        FROM cooking_log l
        LEFT JOIN recipes p ON p.recipe_id = l.planned_recipe_id
        LEFT JOIN recipes a ON a.recipe_id = l.actual_recipe_id
        WHERE l.household_id = %s
        ORDER BY l.cooked_date DESC LIMIT %s
        """,
        (household_id, max(1, min(int(limit or 14), 60))),
    )


def tool_create_meal_plan(household_id: int, week_start: str, days: list,
                          rationale: str = None, **_):
    if not days:
        return {"error": "days is empty - nothing to plan"}

    valid = {r["recipe_id"] for r in run_query(
        "SELECT recipe_id FROM recipes WHERE recipe_id = ANY(%s)",
        ([int(d["recipe_id"]) for d in days if d.get("recipe_id")],),
    )}
    unknown = [d["recipe_id"] for d in days if d.get("recipe_id") not in valid]
    if unknown:
        # The model occasionally invents plausible ids. Refuse rather than
        # write a plan pointing at recipes that don't exist.
        return {"error": f"unknown recipe_ids: {unknown}. Use search_recipes "
                         f"and plan only ids it returned."}

    plan = run_returning(
        """
        INSERT INTO meal_plans (household_id, week_start, status, rationale)
        VALUES (%s, %s, 'active', %s)
        ON CONFLICT (household_id, week_start)
        DO UPDATE SET rationale = EXCLUDED.rationale, status = 'active'
        RETURNING plan_id, week_start
        """,
        (household_id, week_start, rationale),
    )
    run_write("DELETE FROM meal_plan_items WHERE plan_id = %s", (plan["plan_id"],))

    written = []
    for day in days:
        item = run_returning(
            """
            INSERT INTO meal_plan_items (plan_id, plan_date, recipe_id,
                                         base_servings, notes)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (plan_id, plan_date) DO UPDATE
                SET recipe_id = EXCLUDED.recipe_id, notes = EXCLUDED.notes
            RETURNING item_id, plan_date, recipe_id
            """,
            (plan["plan_id"], day["plan_date"], day["recipe_id"],
             day.get("servings") or 4, day.get("notes")),
        )
        written.append(item)

    return {"plan_id": plan["plan_id"], "week_start": str(plan["week_start"]),
            "days_written": len(written), "days": written}


def tool_log_cooked(household_id: int, cooked_date: str,
                    actual_recipe_id: int = None, actual_freetext: str = None,
                    deviation_reason: str = None, mood_note: str = None,
                    rating: int = None, **_):
    planned = run_one(
        """
        SELECT i.recipe_id FROM meal_plan_items i
        JOIN meal_plans p ON p.plan_id = i.plan_id
        WHERE p.household_id = %s AND i.plan_date = %s
        """,
        (household_id, cooked_date),
    )
    planned_id = planned["recipe_id"] if planned else None
    was_planned = bool(planned_id and actual_recipe_id == planned_id)

    row = run_returning(
        """
        INSERT INTO cooking_log (household_id, cooked_date, planned_recipe_id,
            actual_recipe_id, actual_freetext, was_planned, deviation_reason,
            mood_note, rating)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (household_id, cooked_date) DO UPDATE SET
            actual_recipe_id = EXCLUDED.actual_recipe_id,
            actual_freetext  = EXCLUDED.actual_freetext,
            was_planned      = EXCLUDED.was_planned,
            deviation_reason = EXCLUDED.deviation_reason,
            mood_note        = EXCLUDED.mood_note,
            rating           = EXCLUDED.rating
        RETURNING log_id, cooked_date, was_planned
        """,
        (household_id, cooked_date, planned_id, actual_recipe_id,
         actual_freetext, was_planned, deviation_reason, mood_note, rating),
    )
    return {
        "log_id": row["log_id"],
        "cooked_date": str(row["cooked_date"]),
        "matched_the_plan": row["was_planned"],
        "note": "Saved. The reason text gets embedded by embed_content.py and "
                "becomes retrievable context for future planning.",
    }


def tool_build_grocery_list(household_id: int, week_start: str, **_):
    plan = run_one(
        "SELECT plan_id FROM meal_plans WHERE household_id = %s AND week_start = %s",
        (household_id, week_start),
    )
    if not plan:
        return {"error": f"no plan for week starting {week_start}"}

    lines = run_query(
        """
        SELECT ri.ingredient_id, ri.raw_text, ri.ingredient_name, ri.quantity,
               ri.unit, ri.scaling_class, i.canonical_name,
               p.unit_price_eur, p.unit_basis, p.store_name, p.store_id,
               p.source AS price_source
        FROM meal_plan_items mi
        JOIN recipe_ingredients ri ON ri.recipe_id = mi.recipe_id
        LEFT JOIN ingredients i ON i.ingredient_id = ri.ingredient_id
        LEFT JOIN LATERAL (
            SELECT * FROM latest_ingredient_prices lp
            WHERE lp.ingredient_id = ri.ingredient_id
            ORDER BY lp.unit_price_eur NULLS LAST LIMIT 1
        ) p ON TRUE
        WHERE mi.plan_id = %s
        """,
        (plan["plan_id"],),
    )

    agg: dict = {}
    unpriced: list[str] = []
    for ln in lines:
        key = ln["ingredient_id"] or f"txt:{(ln['ingredient_name'] or ln['raw_text']).lower()}"
        grams, _quality = to_grams(ln["quantity"], ln["unit"])
        entry = agg.setdefault(key, {
            "ingredient_id": ln["ingredient_id"],
            "display_name": ln["canonical_name"] or ln["ingredient_name"]
                            or ln["raw_text"],
            "grams": 0.0, "unit_price_eur": ln["unit_price_eur"],
            "unit_basis": ln["unit_basis"], "store_name": ln["store_name"],
            "store_id": ln["store_id"], "price_source": ln["price_source"],
            "mentions": 0,
        })
        entry["mentions"] += 1
        if grams:
            entry["grams"] += grams

    run_write("DELETE FROM grocery_lists WHERE plan_id = %s", (plan["plan_id"],))
    glist = run_returning(
        "INSERT INTO grocery_lists (plan_id) VALUES (%s) RETURNING list_id",
        (plan["plan_id"],),
    )

    total = 0.0
    by_store: dict = {}
    for entry in agg.values():
        cost = price_for_grams(entry["grams"] or None, entry["unit_price_eur"],
                               entry["unit_basis"])
        if cost is None:
            unpriced.append(entry["display_name"])
        else:
            total += cost
            store = entry["store_name"] or "unknown store"
            by_store[store] = round(by_store.get(store, 0) + cost, 2)

        run_write(
            """INSERT INTO grocery_items (list_id, ingredient_id, display_name,
                   quantity, unit, store_id, est_price_eur, price_source)
               VALUES (%s,%s,%s,%s,'g',%s,%s,%s)""",
            (glist["list_id"], entry["ingredient_id"], entry["display_name"],
             round(entry["grams"], 1) or None, entry["store_id"],
             None if cost is None else round(cost, 2), entry["price_source"]),
        )

    run_write("UPDATE grocery_lists SET total_eur = %s WHERE list_id = %s",
              (round(total, 2), glist["list_id"]))

    return {
        "list_id": glist["list_id"],
        "week_start": week_start,
        "distinct_items": len(agg),
        "estimated_total_eur": round(total, 2),
        "by_store": by_store,
        "unpriced_items": unpriced[:12],
        "coverage_note": f"{len(agg) - len(unpriced)} of {len(agg)} items priced. "
                         f"The total covers only those - it is a floor, not a "
                         f"final bill.",
    }


TOOL_IMPLS = {
    "get_household": tool_get_household,
    "search_recipes": tool_search_recipes,
    "get_recipe": tool_get_recipe,
    "get_cooking_history": tool_get_cooking_history,
    "create_meal_plan": tool_create_meal_plan,
    "log_cooked": tool_log_cooked,
    "build_grocery_list": tool_build_grocery_list,
}

WRITE_TOOLS = {"create_meal_plan", "log_cooked", "build_grocery_list"}


# ---------------------------------------------------------------------------
# the loop
# ---------------------------------------------------------------------------

class AgentEndpointError(RuntimeError):
    pass


def _auth_headers() -> tuple[str, dict]:
    """(host, headers) for calling a serving endpoint.

    Deliberately not `serving_endpoints.get_open_ai_client()` - that helper
    doesn't exist in every databricks-sdk version, and the one the app image
    ships with is not ours to choose. The REST path is stable and the payload
    is OpenAI-shaped either way.
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = (w.config.host or "").rstrip("/")

    headers = {}
    try:
        # Works for both PAT and the OAuth service principal used by Apps.
        headers = dict(w.config.authenticate() or {})
    except Exception:  # noqa: BLE001
        pass
    if "Authorization" not in headers and getattr(w.config, "token", None):
        headers["Authorization"] = f"Bearer {w.config.token}"
    if "Authorization" not in headers:
        raise AgentEndpointError(
            "Could not obtain Databricks credentials for the serving endpoint."
        )
    headers["Content-Type"] = "application/json"
    return host, headers


def _invoke(payload: dict) -> dict:
    """POST an OpenAI-style chat payload to the serving endpoint."""
    import requests

    host, headers = _auth_headers()
    url = f"{host}/serving-endpoints/{AGENT_ENDPOINT}/invocations"
    resp = requests.post(url, headers=headers, json=payload, timeout=180)

    if resp.status_code == 404:
        raise AgentEndpointError(
            f"Serving endpoint '{AGENT_ENDPOINT}' not found. Set AGENT_ENDPOINT "
            f"in app.yaml to one that exists in this workspace."
        )
    if resp.status_code == 429:
        raise AgentEndpointError(
            "The model endpoint is rate-limiting. Wait a few seconds and retry."
        )
    if resp.status_code >= 400:
        raise AgentEndpointError(
            f"{AGENT_ENDPOINT} returned {resp.status_code}: {resp.text[:400]}"
        )
    return resp.json()


def _json_default(obj):
    return str(obj)      # dates, Decimals


def chat(messages: list[dict], household_id: int) -> dict:
    """Run the agent until it produces an answer or hits the step limit.

    Returns the assistant's reply plus a trace of the tools it used, so the UI
    can show that a write actually happened rather than asking the user to
    take the model's word for it.
    """
    convo = [{"role": "system", "content": SYSTEM_PROMPT}] + messages
    trace = []

    for _step in range(MAX_STEPS):
        data = _invoke({
            "messages": convo,
            "tools": TOOLS,
            "tool_choice": "auto",
            "max_tokens": 1600,
        })
        choices = data.get("choices") or []
        if not choices:
            raise AgentEndpointError(f"empty response from {AGENT_ENDPOINT}")

        choice = choices[0].get("message", {})
        calls = choice.get("tool_calls") or []

        if not calls:
            return {"reply": choice.get("content") or "", "trace": trace,
                    "endpoint": AGENT_ENDPOINT}

        convo.append({
            "role": "assistant",
            "content": choice.get("content") or "",
            "tool_calls": [
                {"id": c.get("id"), "type": "function",
                 "function": {"name": c.get("function", {}).get("name"),
                              "arguments": c.get("function", {}).get("arguments") or "{}"}}
                for c in calls
            ],
        })

        for call in calls:
            fn = call.get("function", {})
            name = fn.get("name")
            raw_args = fn.get("arguments")
            # Some models return arguments already decoded rather than as a
            # JSON string, so accept both shapes.
            if isinstance(raw_args, dict):
                args = raw_args
            else:
                try:
                    args = json.loads(raw_args or "{}")
                except json.JSONDecodeError:
                    args = {}

            impl = TOOL_IMPLS.get(name)
            if impl is None:
                result = {"error": f"unknown tool {name}"}
            else:
                try:
                    result = impl(household_id=household_id, **args)
                except Exception as exc:  # noqa: BLE001
                    # Hand the error back to the model - it can usually correct
                    # a bad argument itself, and a stack trace to the user is
                    # worse than a retry.
                    logger.exception("tool %s failed", name)
                    result = {"error": f"{type(exc).__name__}: {exc}"}

            trace.append({"tool": name, "args": args,
                          "is_write": name in WRITE_TOOLS,
                          "ok": "error" not in (result if isinstance(result, dict) else {})})
            convo.append({
                "role": "tool",
                "tool_call_id": call.get("id"),
                "content": json.dumps(result, default=_json_default)[:12000],
            })

    return {
        "reply": "I ran out of steps before finishing. Try asking for one "
                 "thing at a time - for example just the plan, then the "
                 "grocery list.",
        "trace": trace,
        "endpoint": AGENT_ENDPOINT,
    }
