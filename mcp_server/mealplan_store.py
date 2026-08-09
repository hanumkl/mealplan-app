"""
Adapter module for the mealplan MCP server.

Same role as Day 3's `alpaca_broker.py`: every database call, every query and
all the derived logic lives here, so the `@mcp.tool` functions upstairs stay
thin wrappers with docstrings. Nothing in mealplan_mcp_server.py talks to
Postgres directly.

Self-contained on purpose - each Databricks App deploys from its own folder, so
this cannot import from `web/`.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import date, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("mealplan.store")

HOUSEHOLD_ID = int(os.environ.get("DEFAULT_HOUSEHOLD_ID", "1"))
_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "mealplan")
_KEY = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")
EMBEDDING_MODEL = os.environ.get(
    "EMBEDDING_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)

_url: str | None = None
_model = None
_model_failed = False


def _lakebase_url() -> str:
    """Resolve the connection string. Never hardcoded, never committed."""
    global _url
    if _url:
        return _url
    env = os.environ.get("LAKEBASE_URL")
    if env:
        _url = env
        return _url

    from databricks.sdk import WorkspaceClient

    secret = WorkspaceClient().secrets.get_secret(scope=_SCOPE, key=_KEY)
    import base64

    _url = base64.b64decode(secret.value).decode()
    return _url


@contextmanager
def _conn():
    connection = psycopg2.connect(_lakebase_url(), cursor_factory=RealDictCursor)
    try:
        yield connection
    finally:
        connection.close()


def _query(sql: str, params=None) -> list[dict]:
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def _one(sql: str, params=None) -> dict | None:
    rows = _query(sql, params)
    return rows[0] if rows else None


def _write(sql: str, params=None) -> dict | None:
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone() if cur.description else None
            c.commit()
            return dict(row) if row else None


# ---------------------------------------------------------------------------
# semantic search, with graceful degradation
# ---------------------------------------------------------------------------

def _embed(text: str):
    """Embed a query, or return None if the model can't load.

    Must be the same model that notebooks/embed_content.py used - two models
    give same-length vectors that mean different things, and the search then
    returns confident nonsense rather than failing.
    """
    global _model, _model_failed
    if _model_failed:
        return None
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer

            _model = SentenceTransformer(EMBEDDING_MODEL)
        except Exception as exc:  # noqa: BLE001
            logger.warning("embedding model unavailable (%s); keyword search only", exc)
            _model_failed = True
            return None
    vec = _model.encode([text], normalize_embeddings=True)[0]
    return "[" + ",".join(str(float(x)) for x in vec.tolist()) + "]"


# ---------------------------------------------------------------------------
# household constraints
# ---------------------------------------------------------------------------

RESTRICTION_SQL = {
    "vegetarian":   "r.is_vegetarian IS TRUE",
    "vegan":        "r.is_vegan IS TRUE",
    "halal":        "r.contains_pork IS NOT TRUE AND r.halal_status <> 'contains_flagged'",
    "no_pork":      "r.contains_pork IS NOT TRUE",
    "gluten_free":  "r.contains_gluten IS NOT TRUE",
    "lactose_free": "r.contains_lactose IS NOT TRUE",
}


def _strict_filter() -> tuple[str, list[str]]:
    """SQL that excludes anything violating a strict restriction.

    Applied in the query rather than left to the model. A dish the household
    must never eat should not reach the agent's context at all.
    """
    rows = _query(
        """SELECT DISTINCT r.restriction
           FROM member_restrictions r
           JOIN members m ON m.member_id = r.member_id
           WHERE m.household_id = %s AND r.severity = 'strict'""",
        (HOUSEHOLD_ID,),
    )
    clauses, unenforceable = [], []
    for row in rows:
        key = row["restriction"]
        if key in RESTRICTION_SQL:
            clauses.append(RESTRICTION_SQL[key])
        else:
            unenforceable.append(key)
    return (" AND " + " AND ".join(clauses)) if clauses else "", unenforceable


def get_household_profile() -> dict:
    """Members, goals and restrictions."""
    members = _query(
        """SELECT m.member_id, m.name, m.role, m.birth_year, m.activity_level,
                  g.goal_type, g.target_kcal, g.target_protein_g
           FROM members m
           LEFT JOIN member_goals g ON g.member_id = m.member_id AND g.is_active
           WHERE m.household_id = %s ORDER BY m.member_id""",
        (HOUSEHOLD_ID,),
    )
    restrictions = _query(
        """SELECT m.name, r.restriction, r.severity
           FROM member_restrictions r
           JOIN members m ON m.member_id = r.member_id
           WHERE m.household_id = %s""",
        (HOUSEHOLD_ID,),
    )
    strict = [r for r in restrictions if r["severity"] == "strict"]
    return {
        "household_id": HOUSEHOLD_ID,
        "members": members,
        "strict_restrictions": strict,
        "preferences": [r for r in restrictions if r["severity"] != "strict"],
        "requires_split_protein": any(
            r["restriction"] in ("vegetarian", "vegan") for r in strict),
        "cooking_model": "One dish cooked each morning covers lunch and dinner "
                         "that day. Seven cooking sessions per week.",
    }


def search_recipes(query: str, limit: int = 5) -> dict:
    """Semantic search, falling back to keyword when embeddings are absent."""
    limit = max(1, min(int(limit or 5), 15))
    filter_sql, unenforceable = _strict_filter()
    vec = _embed(query)

    if vec:
        rows = _query(
            f"""SELECT r.recipe_id, r.title, r.cuisine, r.duration_min,
                       r.base_servings, r.halal_status, r.is_vegetarian,
                       r.is_vegan, r.contains_pork, r.contains_gluten,
                       r.contains_lactose, r.video_url,
                       round((1 - (e.embedding <=> %s::vector))::numeric, 3) AS similarity
                FROM recipe_embeddings e
                JOIN recipes r ON r.recipe_id = e.recipe_id
                WHERE r.review_status = 'approved'{filter_sql}
                ORDER BY e.embedding <=> %s::vector
                LIMIT %s""",
            (vec, vec, limit),
        )
        mode = "semantic"
    else:
        rows = _query(
            f"""SELECT r.recipe_id, r.title, r.cuisine, r.duration_min,
                       r.base_servings, r.halal_status, r.is_vegetarian,
                       r.contains_pork, r.video_url, NULL::numeric AS similarity
                FROM recipes r
                WHERE r.review_status = 'approved'
                  AND (r.title ILIKE %s OR r.description ILIKE %s){filter_sql}
                LIMIT %s""",
            (f"%{query}%", f"%{query}%", limit),
        )
        mode = "keyword"

    return {"mode": mode, "count": len(rows), "results": rows,
            "unenforceable_restrictions": unenforceable}


# ---------------------------------------------------------------------------
# units - recipe quantities to grams, honest about what it can't convert
# ---------------------------------------------------------------------------

EXACT = {"g": 1.0, "gr": 1.0, "gram": 1.0, "kg": 1000.0, "mg": 0.001}
APPROX = {"ml": 1.0, "l": 1000.0, "dl": 100.0, "tbsp": 15.0, "sdm": 15.0,
          "tsp": 5.0, "sdt": 5.0, "cup": 240.0, "gelas": 240.0}


def _to_grams(quantity, unit) -> float | None:
    """None, never 0.0, when it can't be converted.

    '1 ekor ayam' (one whole chicken) counted as zero would make a bulking
    meal look like a cutting one.
    """
    if quantity is None:
        return None
    try:
        qty = float(quantity)
    except (TypeError, ValueError):
        return None
    if qty <= 0:
        return None
    key = (unit or "").strip().lower().rstrip(".")
    if key in EXACT:
        return qty * EXACT[key]
    if key in APPROX:
        return qty * APPROX[key]
    return None


def _price(grams, unit_price, basis) -> float | None:
    if grams is None or unit_price is None or basis not in ("kg", "l"):
        return None
    return float(unit_price) * (grams / 1000.0)


def get_recipe_details(recipe_id: int, servings: float | None = None) -> dict:
    """Ingredients scaled to `servings`, with nutrition and cost."""
    recipe = _one("SELECT * FROM recipes WHERE recipe_id = %s", (recipe_id,))
    if not recipe:
        return {"error": f"no recipe with id {recipe_id}"}

    base = float(recipe.get("base_servings") or 4)
    factor = (servings / base) if servings else 1.0

    rows = _query(
        """SELECT ri.raw_text, ri.ingredient_name, ri.quantity, ri.unit,
                  ri.scaling_class, ri.is_protein_component, ri.is_optional,
                  i.canonical_name, i.kcal_per_100g, i.protein_g_per_100g,
                  p.unit_price_eur, p.unit_basis, p.store_name
           FROM recipe_ingredients ri
           LEFT JOIN ingredients i ON i.ingredient_id = ri.ingredient_id
           LEFT JOIN LATERAL (
               SELECT * FROM latest_ingredient_prices lp
               WHERE lp.ingredient_id = ri.ingredient_id
               ORDER BY lp.unit_price_eur NULLS LAST LIMIT 1
           ) p ON TRUE
           WHERE ri.recipe_id = %s ORDER BY ri.sort_order, ri.ri_id""",
        (recipe_id,),
    )

    kcal = protein = cost = 0.0
    counted = 0
    for r in rows:
        qty = r["quantity"]
        if qty is not None:
            # Spices scale sub-linearly; tripling the chilli ruins the dish.
            if r["scaling_class"] == "fixed":
                scaled = float(qty)
            elif r["scaling_class"] == "sublinear":
                scaled = float(qty) * (factor ** 0.8)
            else:
                scaled = float(qty) * factor
            r["scaled_quantity"] = round(scaled, 2)
        else:
            scaled = None
            r["scaled_quantity"] = None

        grams = _to_grams(scaled, r["unit"])
        if grams and r["kcal_per_100g"] is not None:
            kcal += float(r["kcal_per_100g"]) * grams / 100.0
            if r["protein_g_per_100g"] is not None:
                protein += float(r["protein_g_per_100g"]) * grams / 100.0
            counted += 1
            line = _price(grams, r["unit_price_eur"], r["unit_basis"])
            if line:
                cost += line

    eff = servings or base
    return {
        "recipe": {k: recipe[k] for k in
                   ("recipe_id", "title", "cuisine", "video_url", "duration_min",
                    "base_servings", "halal_status", "is_vegetarian",
                    "contains_pork", "contains_gluten", "contains_lactose",
                    "instructions") if k in recipe},
        "servings": eff,
        "ingredients": rows,
        "totals": {"kcal": round(kcal), "protein_g": round(protein, 1),
                   "cost_eur": round(cost, 2) if cost else None},
        "per_serving": {"kcal": round(kcal / eff) if eff else None,
                        "protein_g": round(protein / eff, 1) if eff else None},
        "coverage": f"{counted} of {len(rows)} ingredients had both a catalogue "
                    f"match and a convertible quantity. Totals cover only those.",
        "is_complete": counted == len(rows) and len(rows) > 0,
    }


def get_cooking_history(limit: int = 14) -> list[dict]:
    """Recent cooking log, including why meals were swapped."""
    return _query(
        """SELECT l.cooked_date, l.was_planned, l.deviation_reason, l.mood_note,
                  l.rating, p.title AS planned, a.title AS actual,
                  l.actual_freetext
           FROM cooking_log l
           LEFT JOIN recipes p ON p.recipe_id = l.planned_recipe_id
           LEFT JOIN recipes a ON a.recipe_id = l.actual_recipe_id
           WHERE l.household_id = %s
           ORDER BY l.cooked_date DESC LIMIT %s""",
        (HOUSEHOLD_ID, max(1, min(int(limit or 14), 60))),
    )


# ---------------------------------------------------------------------------
# derived judgement - not a passthrough
# ---------------------------------------------------------------------------

def suggest_todays_meal(max_minutes: int = None, avoid_recent_days: int = 10) -> dict:
    """Pick a dish for today by applying explicit rules, not by echoing a query.

    The rules, in order:
      1. Strict restrictions are absolute (applied in SQL, not left to chance).
      2. Anything cooked within `avoid_recent_days` is excluded - the most
         common complaint about meal planners is repetition.
      3. If `max_minutes` is given, longer recipes are dropped.
      4. Remaining candidates are ranked by how well one serving covers the
         household's average per-meal calorie need. A dish that misses badly
         in either direction ranks lower, because a plan you have to eat three
         servings of is not really a plan.
    """
    filter_sql, unenforceable = _strict_filter()
    cutoff = date.today() - timedelta(days=int(avoid_recent_days or 10))

    duration_sql = ""
    params: list = [cutoff]
    if max_minutes:
        duration_sql = " AND (r.duration_min IS NULL OR r.duration_min <= %s)"
        params.append(int(max_minutes))

    candidates = _query(
        f"""SELECT r.recipe_id, r.title, r.cuisine, r.duration_min,
                   r.base_servings, r.halal_status, r.is_vegetarian
            FROM recipes r
            WHERE r.review_status = 'approved'{filter_sql}{duration_sql}
              AND NOT EXISTS (
                  SELECT 1 FROM cooking_log l
                  WHERE l.actual_recipe_id = r.recipe_id AND l.cooked_date >= %s)
            LIMIT 40""",
        tuple([params[1]] + [params[0]]) if max_minutes else (cutoff,),
    )
    if not candidates:
        return {"suggestion": None,
                "reason": "Nothing passed the filters. Try relaxing "
                          "max_minutes or avoid_recent_days."}

    target = _one(
        """SELECT COALESCE(AVG(g.target_kcal), 0) AS avg_kcal, COUNT(*) AS n
           FROM member_goals g JOIN members m ON m.member_id = g.member_id
           WHERE m.household_id = %s AND g.is_active""",
        (HOUSEHOLD_ID,),
    )
    # One dish covers lunch and dinner - roughly 65% of a day's calories.
    per_meal = float(target["avg_kcal"] or 0) * 0.65

    scored = []
    for c in candidates[:12]:          # detail lookup is the expensive part
        detail = get_recipe_details(c["recipe_id"])
        kcal = detail["per_serving"]["kcal"] or 0
        if not kcal or not per_meal:
            fit = None
        else:
            fit = round(kcal / per_meal, 2)
        scored.append({**c, "kcal_per_serving": kcal,
                       "cost_eur": detail["totals"]["cost_eur"],
                       "fit_vs_target": fit,
                       "nutrition_complete": detail["is_complete"]})

    known = [s for s in scored if s["fit_vs_target"]]
    if known:
        best = min(known, key=lambda s: abs(s["fit_vs_target"] - 1.0))
        why = (f"One serving is {best['kcal_per_serving']} kcal against a "
               f"per-meal need of about {round(per_meal)} kcal "
               f"({best['fit_vs_target']}x), the closest fit among "
               f"{len(known)} candidates with known nutrition.")
    else:
        best = scored[0]
        why = ("No candidate had complete nutrition data, so this is picked on "
               "constraints and recency only - not on calories.")

    return {
        "suggestion": best,
        "reason": why,
        "target_kcal_per_meal": round(per_meal) if per_meal else None,
        "rules_applied": [
            "strict dietary restrictions enforced in SQL",
            f"excluded anything cooked in the last {avoid_recent_days} days",
            f"max {max_minutes} minutes" if max_minutes else "no time limit",
            "ranked by closeness to per-meal calorie need",
        ],
        "unenforceable_restrictions": unenforceable,
        "alternatives": [s for s in scored if s["recipe_id"] != best["recipe_id"]][:4],
    }


# ---------------------------------------------------------------------------
# writes
# ---------------------------------------------------------------------------

def create_meal_plan(week_start: str, days: list[dict], rationale: str = None) -> dict:
    """Commit a week. Replaces the week's days rather than appending."""
    if not days:
        return {"error": "days is empty"}

    ids = [int(d["recipe_id"]) for d in days if d.get("recipe_id")]
    known = {r["recipe_id"] for r in _query(
        "SELECT recipe_id FROM recipes WHERE recipe_id = ANY(%s)", (ids,))}
    unknown = [i for i in ids if i not in known]
    if unknown:
        # Models invent plausible ids. A plan pointing at recipes that don't
        # exist is worse than a refusal.
        return {"error": f"unknown recipe_ids: {unknown}. Call search_recipes "
                         f"and use only ids it returned."}

    plan = _write(
        """INSERT INTO meal_plans (household_id, week_start, status, rationale)
           VALUES (%s,%s,'active',%s)
           ON CONFLICT (household_id, week_start)
           DO UPDATE SET rationale = EXCLUDED.rationale, status = 'active'
           RETURNING plan_id, week_start""",
        (HOUSEHOLD_ID, week_start, rationale),
    )
    _write("DELETE FROM meal_plan_items WHERE plan_id = %s", (plan["plan_id"],))

    written = []
    for d in days:
        written.append(_write(
            """INSERT INTO meal_plan_items (plan_id, plan_date, recipe_id,
                   base_servings, notes)
               VALUES (%s,%s,%s,%s,%s)
               ON CONFLICT (plan_id, plan_date) DO UPDATE
                   SET recipe_id = EXCLUDED.recipe_id, notes = EXCLUDED.notes
               RETURNING item_id, plan_date, recipe_id""",
            (plan["plan_id"], d["plan_date"], d["recipe_id"],
             d.get("servings") or 4, d.get("notes")),
        ))

    return {"plan_id": plan["plan_id"], "week_start": str(plan["week_start"]),
            "days_written": len(written), "days": written}


def log_cooked_meal(cooked_date: str, actual_recipe_id: int = None,
                    actual_freetext: str = None, deviation_reason: str = None,
                    mood_note: str = None, rating: int = None) -> dict:
    """Record what was really cooked. The reason is the valuable part."""
    planned = _one(
        """SELECT i.recipe_id FROM meal_plan_items i
           JOIN meal_plans p ON p.plan_id = i.plan_id
           WHERE p.household_id = %s AND i.plan_date = %s""",
        (HOUSEHOLD_ID, cooked_date),
    )
    planned_id = planned["recipe_id"] if planned else None
    was_planned = bool(planned_id and actual_recipe_id == planned_id)

    row = _write(
        """INSERT INTO cooking_log (household_id, cooked_date, planned_recipe_id,
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
           RETURNING log_id, cooked_date, was_planned""",
        (HOUSEHOLD_ID, cooked_date, planned_id, actual_recipe_id,
         actual_freetext, was_planned, deviation_reason, mood_note, rating),
    )
    return {"log_id": row["log_id"], "cooked_date": str(row["cooked_date"]),
            "matched_the_plan": row["was_planned"],
            "note": "The free-text reason is embedded by embed_content.py and "
                    "becomes retrievable context for future planning."}


def build_grocery_list(week_start: str) -> dict:
    """Aggregate a week's ingredients and price them from receipt data."""
    plan = _one(
        "SELECT plan_id FROM meal_plans WHERE household_id=%s AND week_start=%s",
        (HOUSEHOLD_ID, week_start),
    )
    if not plan:
        return {"error": f"no plan for week starting {week_start}"}

    lines = _query(
        """SELECT ri.ingredient_id, ri.raw_text, ri.ingredient_name, ri.quantity,
                  ri.unit, i.canonical_name, p.unit_price_eur, p.unit_basis,
                  p.store_name, p.store_id, p.source AS price_source
           FROM meal_plan_items mi
           JOIN recipe_ingredients ri ON ri.recipe_id = mi.recipe_id
           LEFT JOIN ingredients i ON i.ingredient_id = ri.ingredient_id
           LEFT JOIN LATERAL (
               SELECT * FROM latest_ingredient_prices lp
               WHERE lp.ingredient_id = ri.ingredient_id
               ORDER BY lp.unit_price_eur NULLS LAST LIMIT 1
           ) p ON TRUE
           WHERE mi.plan_id = %s""",
        (plan["plan_id"],),
    )

    agg: dict = {}
    for ln in lines:
        key = ln["ingredient_id"] or f"txt:{(ln['ingredient_name'] or ln['raw_text']).lower()}"
        e = agg.setdefault(key, {
            "ingredient_id": ln["ingredient_id"],
            "display_name": ln["canonical_name"] or ln["ingredient_name"] or ln["raw_text"],
            "grams": 0.0, "unit_price_eur": ln["unit_price_eur"],
            "unit_basis": ln["unit_basis"], "store_name": ln["store_name"],
            "store_id": ln["store_id"], "price_source": ln["price_source"],
        })
        g = _to_grams(ln["quantity"], ln["unit"])
        if g:
            e["grams"] += g

    _write("DELETE FROM grocery_lists WHERE plan_id = %s", (plan["plan_id"],))
    glist = _write(
        "INSERT INTO grocery_lists (plan_id) VALUES (%s) RETURNING list_id",
        (plan["plan_id"],))

    total = 0.0
    by_store: dict = {}
    unpriced = []
    for e in agg.values():
        cost = _price(e["grams"] or None, e["unit_price_eur"], e["unit_basis"])
        if cost is None:
            unpriced.append(e["display_name"])
        else:
            total += cost
            s = e["store_name"] or "unknown store"
            by_store[s] = round(by_store.get(s, 0) + cost, 2)
        _write(
            """INSERT INTO grocery_items (list_id, ingredient_id, display_name,
                   quantity, unit, store_id, est_price_eur, price_source)
               VALUES (%s,%s,%s,%s,'g',%s,%s,%s)""",
            (glist["list_id"], e["ingredient_id"], e["display_name"],
             round(e["grams"], 1) or None, e["store_id"],
             None if cost is None else round(cost, 2), e["price_source"]),
        )

    _write("UPDATE grocery_lists SET total_eur=%s WHERE list_id=%s",
           (round(total, 2), glist["list_id"]))

    return {"list_id": glist["list_id"], "week_start": week_start,
            "distinct_items": len(agg), "estimated_total_eur": round(total, 2),
            "by_store": by_store, "unpriced_items": unpriced[:12],
            "coverage_note": f"{len(agg) - len(unpriced)} of {len(agg)} items "
                             f"priced. The total is a floor, not a final bill."}
