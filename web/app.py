"""
Stage 1 - Databricks App: household setup + ingredient/price catalog.

Flask serves both the JSON API and the single-page frontend, following the
bootcamp Day 1/2 pattern (`command: python app.py` in app.yaml).

Stage 2 adds the pgvector search endpoints; Stage 3 adds the MCP server as a
separate Databricks App that reuses this same Lakebase database.
"""

import os

from flask import Flask, jsonify, render_template, request

# Local development reads LAKEBASE_URL from .env; on Databricks Apps the value
# comes from the secret scope declared in app.yaml and there is no .env file.
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except ImportError:
    pass

import embeddings
from lakebase import run_one, run_query, run_returning, run_write
from nutrition import calculate_targets, scale_quantity
from units import price_for_grams, to_grams

app = Flask(__name__)

DEFAULT_HOUSEHOLD_ID = int(os.environ.get("DEFAULT_HOUSEHOLD_ID", "1"))


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def body() -> dict:
    return request.get_json(silent=True) or {}


def bad_request(message: str):
    return jsonify({"error": message}), 400


# --------------------------------------------------------------------------
# health + page
# --------------------------------------------------------------------------

def _health_payload():
    try:
        run_one("SELECT 1 AS ok")
        return {"status": "ok", "database": "connected"}, 200
    except Exception as exc:
        return {"status": "degraded", "database": str(exc)}, 503


@app.get("/healthz")
def healthz():
    """Platform-facing probe. The Databricks Apps proxy may serve this path
    itself, so the frontend uses /api/health instead."""
    payload, code = _health_payload()
    return jsonify(payload), code


@app.get("/api/health")
def api_health():
    """Same check, on a path the proxy won't intercept.

    Always returns 200 so the frontend can tell 'database is down' apart from
    'the app itself is unreachable' - a 503 here would look identical to a
    network failure in the browser.
    """
    payload, _ = _health_payload()
    return jsonify(payload)


@app.get("/")
def index():
    return render_template("index.html")


# --------------------------------------------------------------------------
# households
# --------------------------------------------------------------------------

@app.get("/api/households")
def list_households():
    return jsonify(run_query(
        """
        SELECT h.household_id, h.name, h.city, h.created_at,
               COUNT(m.member_id) AS member_count
        FROM households h
        LEFT JOIN members m ON m.household_id = h.household_id
        GROUP BY h.household_id
        ORDER BY h.household_id
        """
    ))


@app.post("/api/households")
def create_household():
    data = body()
    if not data.get("name"):
        return bad_request("name is required")
    row = run_returning(
        "INSERT INTO households (name, city) VALUES (%s, %s) RETURNING *",
        (data["name"], data.get("city")),
    )
    return jsonify(row), 201


# --------------------------------------------------------------------------
# members, goals, restrictions
# --------------------------------------------------------------------------

@app.get("/api/households/<int:household_id>/members")
def list_members(household_id: int):
    """Members with their active goal and restrictions, in one payload."""
    members = run_query(
        """
        SELECT m.*,
               g.goal_id, g.goal_type, g.target_kcal, g.target_protein_g,
               g.target_carb_g, g.target_fat_g, g.target_source
        FROM members m
        LEFT JOIN member_goals g
               ON g.member_id = m.member_id AND g.is_active
        WHERE m.household_id = %s
        ORDER BY m.member_id
        """,
        (household_id,),
    )
    if not members:
        return jsonify([])

    restrictions = run_query(
        """
        SELECT member_id, restriction_id, restriction, severity, note
        FROM member_restrictions
        WHERE member_id = ANY(%s)
        ORDER BY restriction
        """,
        ([m["member_id"] for m in members],),
    )
    by_member: dict[int, list] = {}
    for r in restrictions:
        by_member.setdefault(r["member_id"], []).append(r)
    for m in members:
        m["restrictions"] = by_member.get(m["member_id"], [])
    return jsonify(members)


@app.post("/api/households/<int:household_id>/members")
def create_member(household_id: int):
    data = body()
    if not data.get("name"):
        return bad_request("name is required")
    row = run_returning(
        """
        INSERT INTO members
            (household_id, name, role, birth_year, sex, weight_kg, height_cm, activity_level)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
        """,
        (
            household_id,
            data["name"],
            data.get("role", "adult"),
            data.get("birth_year"),
            data.get("sex", "unspecified"),
            data.get("weight_kg"),
            data.get("height_cm"),
            data.get("activity_level", "moderate"),
        ),
    )
    return jsonify(row), 201


@app.patch("/api/members/<int:member_id>")
def update_member(member_id: int):
    data = body()
    allowed = ["name", "role", "birth_year", "sex", "weight_kg", "height_cm", "activity_level"]
    fields = [f for f in allowed if f in data]
    if not fields:
        return bad_request("no updatable fields supplied")

    assignments = ", ".join(f"{f} = %s" for f in fields)
    params = [data[f] for f in fields] + [member_id]
    row = run_returning(
        f"UPDATE members SET {assignments} WHERE member_id = %s RETURNING *",
        tuple(params),
    )
    if row is None:
        return jsonify({"error": "member not found"}), 404
    return jsonify(row)


@app.delete("/api/members/<int:member_id>")
def delete_member(member_id: int):
    run_write("DELETE FROM members WHERE member_id = %s", (member_id,))
    return "", 204


@app.post("/api/members/<int:member_id>/suggest-targets")
def suggest_targets(member_id: int):
    """Mifflin-St Jeor targets for this member. Does not save - the UI shows
    them first so the user can accept or override."""
    member = run_one("SELECT * FROM members WHERE member_id = %s", (member_id,))
    if member is None:
        return jsonify({"error": "member not found"}), 404

    goal_type = body().get("goal_type", "maintain")
    try:
        return jsonify(calculate_targets(
            weight_kg=member["weight_kg"],
            height_cm=member["height_cm"],
            birth_year=member["birth_year"],
            sex=member["sex"],
            activity_level=member["activity_level"],
            goal_type=goal_type,
        ))
    except ValueError as exc:
        return bad_request(str(exc))


@app.put("/api/members/<int:member_id>/goal")
def set_goal(member_id: int):
    """Save a goal. Previous goals are deactivated, not deleted, so the Stage 3
    behaviour job can see how targets changed over time."""
    data = body()
    goal_type = data.get("goal_type")
    if goal_type not in ("bulking", "cutting", "maintain", "growth"):
        return bad_request("goal_type must be bulking, cutting, maintain or growth")

    run_write(
        "UPDATE member_goals SET is_active = FALSE WHERE member_id = %s AND is_active",
        (member_id,),
    )
    row = run_returning(
        """
        INSERT INTO member_goals
            (member_id, goal_type, target_kcal, target_protein_g,
             target_carb_g, target_fat_g, target_source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING *
        """,
        (
            member_id,
            goal_type,
            data.get("target_kcal"),
            data.get("target_protein_g"),
            data.get("target_carb_g"),
            data.get("target_fat_g"),
            data.get("target_source", "manual"),
        ),
    )
    return jsonify(row), 201


@app.post("/api/members/<int:member_id>/restrictions")
def add_restriction(member_id: int):
    data = body()
    if not data.get("restriction"):
        return bad_request("restriction is required")
    row = run_returning(
        """
        INSERT INTO member_restrictions (member_id, restriction, severity, note)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (member_id, restriction)
        DO UPDATE SET severity = EXCLUDED.severity, note = EXCLUDED.note
        RETURNING *
        """,
        (
            member_id,
            data["restriction"],
            data.get("severity", "strict"),
            data.get("note"),
        ),
    )
    return jsonify(row), 201


@app.delete("/api/restrictions/<int:restriction_id>")
def delete_restriction(restriction_id: int):
    run_write("DELETE FROM member_restrictions WHERE restriction_id = %s", (restriction_id,))
    return "", 204


# --------------------------------------------------------------------------
# household constraint summary - what the Stage 3 agent will read
# --------------------------------------------------------------------------

@app.get("/api/households/<int:household_id>/constraints")
def household_constraints(household_id: int):
    """Collapse every member's restrictions into household-level rules.

    Strict restrictions bind the whole shared dish. Preferences are returned
    separately so the agent can trade them off instead of treating them as hard
    constraints.
    """
    rows = run_query(
        """
        SELECT r.restriction, r.severity, m.member_id, m.name
        FROM member_restrictions r
        JOIN members m ON m.member_id = r.member_id
        WHERE m.household_id = %s
        """,
        (household_id,),
    )
    strict: dict[str, list[str]] = {}
    soft: dict[str, list[str]] = {}
    for row in rows:
        target = strict if row["severity"] == "strict" else soft
        target.setdefault(row["restriction"], []).append(row["name"])

    totals = run_one(
        """
        SELECT COALESCE(SUM(g.target_kcal), 0)      AS household_kcal,
               COALESCE(SUM(g.target_protein_g), 0) AS household_protein_g,
               COUNT(*)                             AS members_with_goals
        FROM member_goals g
        JOIN members m ON m.member_id = g.member_id
        WHERE m.household_id = %s AND g.is_active
        """,
        (household_id,),
    )

    return jsonify({
        "household_id": household_id,
        "strict": [{"restriction": k, "members": v} for k, v in sorted(strict.items())],
        "preferences": [{"restriction": k, "members": v} for k, v in sorted(soft.items())],
        "daily_totals": totals,
        # Vegetarian + someone bulking on meat can't share one pot, so the base
        # dish goes vegetarian and protein is a per-member add-on.
        "requires_split_protein": "vegetarian" in strict or "vegan" in strict,
    })


# --------------------------------------------------------------------------
# catalog: ingredients, prices, stores
# --------------------------------------------------------------------------

@app.get("/api/ingredients")
def search_ingredients():
    q = (request.args.get("q") or "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)

    if q:
        rows = run_query(
            """
            SELECT i.*, p.price_eur, p.unit_price_eur, p.unit_basis,
                   p.store_name, p.source AS price_source, p.captured_at
            FROM ingredients i
            LEFT JOIN LATERAL (
                SELECT * FROM latest_ingredient_prices lp
                WHERE lp.ingredient_id = i.ingredient_id
                ORDER BY lp.unit_price_eur NULLS LAST
                LIMIT 1
            ) p ON TRUE
            WHERE i.canonical_name ILIKE %s
               OR i.name_fi ILIKE %s
               OR i.name_en ILIKE %s
               OR i.name_id ILIKE %s
               OR i.category_en ILIKE %s
            ORDER BY i.canonical_name
            LIMIT %s
            """,
            (f"%{q}%",) * 5 + (limit,),
        )
    else:
        rows = run_query(
            """
            SELECT i.*, p.price_eur, p.unit_price_eur, p.unit_basis,
                   p.store_name, p.source AS price_source, p.captured_at
            FROM ingredients i
            LEFT JOIN LATERAL (
                SELECT * FROM latest_ingredient_prices lp
                WHERE lp.ingredient_id = i.ingredient_id
                ORDER BY lp.unit_price_eur NULLS LAST
                LIMIT 1
            ) p ON TRUE
            ORDER BY i.updated_at DESC
            LIMIT %s
            """,
            (limit,),
        )
    return jsonify(rows)


@app.get("/api/ingredients/<int:ingredient_id>/prices")
def ingredient_prices(ingredient_id: int):
    return jsonify(run_query(
        """
        SELECT p.*, s.name AS store_name, s.halal_certified
        FROM ingredient_prices p
        LEFT JOIN stores s ON s.store_id = p.store_id
        WHERE p.ingredient_id = %s
        ORDER BY p.captured_at DESC, p.price_id DESC
        """,
        (ingredient_id,),
    ))


@app.post("/api/ingredients/<int:ingredient_id>/prices")
def add_price(ingredient_id: int):
    """Manual price survey entry - the fallback source for staples that don't
    appear on a receipt."""
    data = body()
    if data.get("price_eur") is None:
        return bad_request("price_eur is required")
    row = run_returning(
        """
        INSERT INTO ingredient_prices
            (ingredient_id, store_id, price_eur, quantity, unit,
             unit_price_eur, unit_basis, source, source_ref, confidence, captured_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, CURRENT_DATE))
        RETURNING *
        """,
        (
            ingredient_id,
            data.get("store_id"),
            data["price_eur"],
            data.get("quantity"),
            data.get("unit"),
            data.get("unit_price_eur"),
            data.get("unit_basis"),
            data.get("source", "manual_survey"),
            data.get("source_ref"),
            data.get("confidence", 1.0),
            data.get("captured_at"),
        ),
    )
    return jsonify(row), 201


@app.put("/api/ingredients/<int:ingredient_id>/halal")
def confirm_halal(ingredient_id: int):
    """Record the household's own halal decision for a product.

    This outranks whatever the pipeline derived, and the ingestion notebook's
    upsert preserves it on subsequent runs. For a halal filter the family is
    the authority - a heuristic over crowd-sourced data isn't.
    """
    data = body()
    status = data.get("halal_status")
    if status not in ("certified", "likely_ok", "contains_flagged", "unknown"):
        return bad_request(
            "halal_status must be certified, likely_ok, contains_flagged or unknown"
        )

    note = (data.get("note") or "").strip() or None
    reason = "confirmed by household" + (f": {note}" if note else "")

    row = run_returning(
        """
        UPDATE ingredients
           SET halal_status       = %s,
               halal_reason       = %s,
               halal_note         = %s,
               halal_source       = 'user_confirmed',
               halal_confirmed_at = now(),
               updated_at         = now()
         WHERE ingredient_id = %s
        RETURNING *
        """,
        (status, reason, note, ingredient_id),
    )
    if row is None:
        return jsonify({"error": "ingredient not found"}), 404
    return jsonify(row)


@app.delete("/api/ingredients/<int:ingredient_id>/halal")
def clear_halal_confirmation(ingredient_id: int):
    """Drop a manual confirmation and fall back to the derived value.

    The derived status isn't recomputed here - it's restored on the next
    ingestion run, so until then the product reads 'unknown'.
    """
    row = run_returning(
        """
        UPDATE ingredients
           SET halal_source       = 'derived',
               halal_confirmed_at = NULL,
               halal_note         = NULL,
               halal_status       = 'unknown',
               halal_reason       = 'confirmation removed - rerun ingestion to re-derive',
               updated_at         = now()
         WHERE ingredient_id = %s
        RETURNING *
        """,
        (ingredient_id,),
    )
    if row is None:
        return jsonify({"error": "ingredient not found"}), 404
    return jsonify(row)


@app.get("/api/ingredients/needing-review")
def ingredients_needing_review():
    """Products the pipeline couldn't classify, for the household to decide on."""
    limit = min(int(request.args.get("limit", 50)), 200)
    return jsonify(run_query(
        "SELECT * FROM ingredients_needing_halal_review LIMIT %s", (limit,)
    ))


@app.get("/api/stores")
def list_stores():
    return jsonify(run_query("SELECT * FROM stores ORDER BY name"))


# --------------------------------------------------------------------------
# recipes + semantic search (Stage 2)
# --------------------------------------------------------------------------

# Strict restrictions map onto columns the harvest notebook derives. Anything
# not listed here can't be enforced in SQL and is left to the Stage 3 agent.
RESTRICTION_FILTERS = {
    "vegetarian":   "r.is_vegetarian IS TRUE",
    "vegan":        "r.is_vegan IS TRUE",
    "halal":        "r.contains_pork IS NOT TRUE AND r.halal_status <> 'contains_flagged'",
    "no_pork":      "r.contains_pork IS NOT TRUE",
    "gluten_free":  "r.contains_gluten IS NOT TRUE",
    "lactose_free": "r.contains_lactose IS NOT TRUE",
}


def _strict_restriction_sql(household_id: int) -> tuple[list[str], list[str]]:
    """SQL predicates for a household's strict restrictions.

    Returns (clauses, ignored) - `ignored` names restrictions with no column to
    filter on, so the UI can say so instead of implying the list is safe.
    """
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
        if key in RESTRICTION_FILTERS:
            clauses.append(RESTRICTION_FILTERS[key])
        else:
            ignored.append(key)
    return clauses, ignored


def _embedding_model_warning(table: str) -> str | None:
    """Detect a query/content model mismatch.

    If the notebook embedded with model A and the app queries with model B, the
    vectors are the same length so nothing errors - the results are just
    silently meaningless. That is the worst kind of bug, so it is checked
    explicitly and surfaced in the response.
    """
    try:
        rows = run_query(f"SELECT DISTINCT model_name FROM {table}")
    except Exception:
        return None
    stored = {r["model_name"] for r in rows if r.get("model_name")}
    if stored and embeddings.ACTIVE_MODEL not in stored:
        return (
            f"{table} was embedded with {', '.join(sorted(stored))} but this app "
            f"queries with {embeddings.ACTIVE_MODEL}. Results are not "
            f"meaningful until both use the same model - re-run "
            f"notebooks/embed_content.py with reembed_all=true."
        )
    return None


@app.get("/api/recipes")
def list_recipes():
    """Browse the recipe catalogue. Keyword filter, no embeddings required."""
    q = (request.args.get("q") or "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)
    status = request.args.get("review_status")
    cuisine = request.args.get("cuisine")
    household_id = request.args.get("household_id", type=int)

    where, params = [], []
    if q:
        where.append("(r.title ILIKE %s OR r.cuisine ILIKE %s OR r.description ILIKE %s)")
        params += [f"%{q}%"] * 3
    if status in ("pending", "approved", "rejected"):
        where.append("r.review_status = %s")
        params.append(status)
    if cuisine:
        where.append("r.cuisine ILIKE %s")
        params.append(cuisine)

    ignored: list[str] = []
    if household_id:
        clauses, ignored = _strict_restriction_sql(household_id)
        where += clauses

    sql = """
        SELECT r.*,
               (SELECT COUNT(*) FROM recipe_ingredients ri
                 WHERE ri.recipe_id = r.recipe_id) AS ingredient_count
        FROM recipes r
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY r.extraction_confidence DESC NULLS LAST, r.recipe_id DESC LIMIT %s"
    params.append(limit)

    return jsonify({
        "results": run_query(sql, tuple(params)),
        "unenforced_restrictions": ignored,
    })


@app.get("/api/recipes/search")
def semantic_recipe_search():
    """Semantic search over recipe_embeddings.

    Falls back to keyword search when the embedding model can't load, because a
    degraded search beats a 500 - the app stays usable on a box without torch.
    """
    q = (request.args.get("q") or "").strip()
    if not q:
        return bad_request("q is required")
    limit = min(int(request.args.get("limit", 10)), 50)
    household_id = request.args.get("household_id", type=int)
    approved_only = request.args.get("approved_only", "false").lower() == "true"

    total = run_one("SELECT COUNT(*) AS n FROM recipe_embeddings")
    if not total or total["n"] == 0:
        return jsonify({
            "query": q, "mode": "unavailable", "results": [],
            "message": "No recipe embeddings yet. Run notebooks/harvest_youtube_"
                       "recipes.py then notebooks/embed_content.py.",
        })

    where, params = [], []
    ignored: list[str] = []
    if household_id:
        clauses, ignored = _strict_restriction_sql(household_id)
        where += clauses
    if approved_only:
        where.append("r.review_status = 'approved'")
    filter_sql = (" AND " + " AND ".join(where)) if where else ""

    if not embeddings.available():
        rows = run_query(
            f"""
            SELECT r.*, NULL::float AS similarity
            FROM recipes r
            WHERE (r.title ILIKE %s OR r.description ILIKE %s){filter_sql}
            ORDER BY r.extraction_confidence DESC NULLS LAST
            LIMIT %s
            """,
            tuple([f"%{q}%", f"%{q}%"] + params + [limit]),
        )
        return jsonify({
            "query": q, "mode": "keyword-fallback", "results": rows,
            "unenforced_restrictions": ignored,
            "message": "Embedding model unavailable; showing keyword matches.",
        })

    vec = embeddings.vector_literal(embeddings.embed_query(q))
    rows = run_query(
        f"""
        SELECT r.recipe_id, r.title, r.cuisine, r.video_url, r.thumbnail_url,
               r.channel_title, r.duration_min, r.base_servings, r.description,
               r.is_vegetarian, r.is_vegan, r.contains_pork, r.contains_gluten,
               r.contains_lactose, r.halal_status, r.extraction_confidence,
               r.review_status,
               1 - (e.embedding <=> %s::vector) AS similarity
        FROM recipe_embeddings e
        JOIN recipes r ON r.recipe_id = e.recipe_id
        WHERE TRUE{filter_sql}
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
        """,
        tuple([vec] + params + [vec, limit]),
    )
    return jsonify({
        "query": q,
        "mode": "semantic",
        "model": embeddings.ACTIVE_MODEL,
        "results": rows,
        "unenforced_restrictions": ignored,
        "warning": _embedding_model_warning("recipe_embeddings"),
    })


# One cooking session covers lunch and dinner, so a serving is roughly two of
# the day's three eating occasions. Breakfast and snacks are outside the plan.
LUNCH_DINNER_SHARE = 0.65


def _nutrition_and_cost(ingredients: list[dict]) -> dict:
    """Total kcal/protein/cost for a scaled ingredient list.

    Reports coverage alongside the totals. A recipe where the chicken failed to
    match would otherwise return a confident-looking 300 kcal, and someone
    bulking would plan around a number that is simply wrong.
    """
    totals = {"kcal": 0.0, "protein_g": 0.0, "carb_g": 0.0, "fat_g": 0.0}
    cost = 0.0
    counted = priced = 0
    approximate = False
    unconvertible: list[str] = []

    for row in ingredients:
        grams, quality = to_grams(row.get("scaled_quantity"), row.get("unit"))
        row["grams"] = None if grams is None else round(grams, 1)
        row["grams_quality"] = quality
        if quality == "approximate":
            approximate = True

        if grams is None or row.get("ingredient_id") is None:
            if not row.get("is_optional"):
                unconvertible.append(row.get("ingredient_name")
                                     or row.get("raw_text") or "?")
            continue

        if row.get("kcal_per_100g") is None:
            unconvertible.append(row.get("canonical_name") or "?")
            continue

        share = grams / 100.0
        totals["kcal"] += float(row["kcal_per_100g"]) * share
        for key, col in (("protein_g", "protein_g_per_100g"),
                         ("carb_g", "carb_g_per_100g"),
                         ("fat_g", "fat_g_per_100g")):
            if row.get(col) is not None:
                totals[key] += float(row[col]) * share
        counted += 1

        line_cost = price_for_grams(grams, row.get("unit_price_eur"),
                                    row.get("unit_basis"))
        row["line_cost_eur"] = None if line_cost is None else round(line_cost, 2)
        if line_cost is not None:
            cost += line_cost
            priced += 1

    total = len(ingredients)
    return {
        "kcal": round(totals["kcal"]),
        "protein_g": round(totals["protein_g"], 1),
        "carb_g": round(totals["carb_g"], 1),
        "fat_g": round(totals["fat_g"], 1),
        "cost_eur": round(cost, 2) if priced else None,
        "ingredients_total": total,
        "ingredients_counted": counted,
        "ingredients_priced": priced,
        # True only when every non-optional line contributed. Anything less and
        # the UI must present these as partial.
        "is_complete": counted == total and total > 0,
        "is_approximate": approximate,
        "missing": unconvertible[:8],
    }


def _member_fit(per_serving_kcal: float, per_serving_protein: float) -> list[dict]:
    """How many servings each member needs to hit their goal for the day."""
    if not per_serving_kcal:
        return []
    members = run_query(
        """
        SELECT m.member_id, m.name, g.goal_type, g.target_kcal, g.target_protein_g
        FROM members m
        JOIN member_goals g ON g.member_id = m.member_id AND g.is_active
        WHERE m.household_id = %s
        ORDER BY m.member_id
        """,
        (DEFAULT_HOUSEHOLD_ID,),
    )
    out = []
    for m in members:
        target_kcal = float(m["target_kcal"]) if m["target_kcal"] else None
        if not target_kcal:
            continue
        meal_kcal = target_kcal * LUNCH_DINNER_SHARE
        out.append({
            "member_id": m["member_id"],
            "name": m["name"],
            "goal_type": m["goal_type"],
            "target_kcal": int(target_kcal),
            "meal_kcal_target": int(round(meal_kcal)),
            "servings_needed": round(meal_kcal / per_serving_kcal, 2),
            "protein_from_one_serving": round(per_serving_protein, 1),
            "target_protein_g": (float(m["target_protein_g"])
                                 if m["target_protein_g"] else None),
        })
    return out


def recipe_nutrition_payload(recipe_id: int, servings: float | None = None):
    """Recipe + scaled ingredients + nutrition/cost. Shared by the REST
    endpoint and the agent's get_recipe tool, so both see the same numbers."""
    recipe = run_one("SELECT * FROM recipes WHERE recipe_id = %s", (recipe_id,))
    if recipe is None:
        return {"error": "recipe not found"}

    base = float(recipe.get("base_servings") or 4)
    factor = (servings / base) if servings and base else 1.0

    ingredients = run_query(
        """
        SELECT ri.*, i.canonical_name, i.name_en, i.name_fi, i.halal_status,
               i.kcal_per_100g, i.protein_g_per_100g,
               i.carb_g_per_100g, i.fat_g_per_100g,
               p.unit_price_eur, p.unit_basis, p.store_name
        FROM recipe_ingredients ri
        LEFT JOIN ingredients i ON i.ingredient_id = ri.ingredient_id
        LEFT JOIN LATERAL (
            SELECT * FROM latest_ingredient_prices lp
            WHERE lp.ingredient_id = ri.ingredient_id
            ORDER BY lp.unit_price_eur NULLS LAST
            LIMIT 1
        ) p ON TRUE
        WHERE ri.recipe_id = %s
        ORDER BY ri.sort_order, ri.ri_id
        """,
        (recipe_id,),
    )
    for row in ingredients:
        qty = row.get("quantity")
        row["scaled_quantity"] = (
            None if qty is None
            else round(scale_quantity(float(qty), factor, row["scaling_class"]), 2)
        )

    totals = _nutrition_and_cost(ingredients)
    effective_servings = servings or base
    per_serving_kcal = (totals["kcal"] / effective_servings
                        if effective_servings else 0)
    per_serving_protein = (totals["protein_g"] / effective_servings
                           if effective_servings else 0)

    return {
        "recipe": recipe,
        "ingredients": ingredients,
        "servings": effective_servings,
        "base_servings": base,
        "scale_factor": round(factor, 3),
        "totals": totals,
        "per_serving": {
            "kcal": round(per_serving_kcal),
            "protein_g": round(per_serving_protein, 1),
            "cost_eur": (round(totals["cost_eur"] / effective_servings, 2)
                         if totals["cost_eur"] and effective_servings else None),
        },
        # Portion maths divides by per-serving calories, so partial nutrition
        # produces absurd answers - a recipe whose chicken didn't match showed
        # "10.73x servings". Flagged rather than hidden: the shape of the
        # calculation is still worth seeing, it just isn't a number to cook by.
        "member_fit": _member_fit(per_serving_kcal, per_serving_protein),
        "member_fit_reliable": bool(totals["is_complete"]),
    }


@app.get("/api/recipes/<int:recipe_id>")
def recipe_detail(recipe_id: int):
    payload = recipe_nutrition_payload(recipe_id, request.args.get("servings", type=float))
    if "error" in payload:
        return jsonify(payload), 404
    return jsonify(payload)


@app.put("/api/recipe-ingredients/<int:ri_id>/match")
def set_ingredient_match(ri_id: int):
    """Correct a catalogue match by hand.

    Marked 'manual' so the matching notebook never overwrites it - the same
    rule as halal confirmation. Pass ingredient_id: null to clear a bad match.
    """
    ingredient_id = body().get("ingredient_id")
    if ingredient_id is not None and not isinstance(ingredient_id, int):
        return bad_request("ingredient_id must be an integer or null")

    row = run_returning(
        """
        UPDATE recipe_ingredients
           SET ingredient_id    = %s,
               match_confidence = NULL,
               match_method     = 'manual',
               matched_at       = now()
         WHERE ri_id = %s
        RETURNING *
        """,
        (ingredient_id, ri_id),
    )
    if row is None:
        return jsonify({"error": "recipe ingredient not found"}), 404
    return jsonify(row)


@app.get("/api/recipes/<int:recipe_id>/steps")
def recipe_step_search(recipe_id: int):
    """Search this recipe's step chunks - 'when do I add the coconut milk?'

    Returns `start_second` so the UI can deep-link into the video at the moment
    the step happens, which is the point of chunking with timestamps.
    """
    q = (request.args.get("q") or "").strip()
    limit = min(int(request.args.get("limit", 5)), 20)

    if not q:
        return jsonify({"results": run_query(
            """
            SELECT chunk_id, chunk_index, chunk_text, start_second
            FROM recipe_chunk_embeddings
            WHERE recipe_id = %s
            ORDER BY chunk_index
            """,
            (recipe_id,),
        ), "mode": "all"})

    if not embeddings.available():
        return jsonify({
            "results": [], "mode": "unavailable",
            "message": "Embedding model unavailable; step search needs it.",
        })

    vec = embeddings.vector_literal(embeddings.embed_query(q))
    return jsonify({
        "mode": "semantic",
        "results": run_query(
            """
            SELECT chunk_id, chunk_index, chunk_text, start_second,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM recipe_chunk_embeddings
            WHERE recipe_id = %s
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (vec, recipe_id, vec, limit),
        ),
    })


@app.put("/api/recipes/<int:recipe_id>/review")
def review_recipe(recipe_id: int):
    """Approve or reject an LLM-extracted recipe. Only approved recipes are
    plannable, so this is the human gate on imperfect extraction."""
    status = body().get("review_status")
    if status not in ("pending", "approved", "rejected"):
        return bad_request("review_status must be pending, approved or rejected")
    row = run_returning(
        "UPDATE recipes SET review_status = %s WHERE recipe_id = %s RETURNING *",
        (status, recipe_id),
    )
    if row is None:
        return jsonify({"error": "recipe not found"}), 404
    return jsonify(row)


@app.get("/api/search/ingredients")
def semantic_ingredient_search():
    """Semantic search over the Finnish catalogue - 'chicken' finds 'Broilerin
    fileesuikale'. This is what makes an English-speaking household able to use
    a Finnish grocery catalogue at all."""
    q = (request.args.get("q") or "").strip()
    if not q:
        return bad_request("q is required")
    limit = min(int(request.args.get("limit", 10)), 50)

    total = run_one("SELECT COUNT(*) AS n FROM ingredient_embeddings")
    if not total or total["n"] == 0:
        return jsonify({
            "query": q, "mode": "unavailable", "results": [],
            "message": "No ingredient embeddings yet. Run notebooks/embed_content.py.",
        })
    if not embeddings.available():
        return jsonify({
            "query": q, "mode": "unavailable", "results": [],
            "message": "Embedding model unavailable.",
        })

    vec = embeddings.vector_literal(embeddings.embed_query(q))
    rows = run_query(
        """
        SELECT i.ingredient_id, i.canonical_name, i.name_fi, i.name_en,
               i.category_en, i.halal_status, i.halal_source, i.is_protein_source,
               p.unit_price_eur, p.unit_basis, p.store_name,
               1 - (e.embedding <=> %s::vector) AS similarity
        FROM ingredient_embeddings e
        JOIN ingredients i ON i.ingredient_id = e.ingredient_id
        LEFT JOIN LATERAL (
            SELECT * FROM latest_ingredient_prices lp
            WHERE lp.ingredient_id = i.ingredient_id
            ORDER BY lp.unit_price_eur NULLS LAST
            LIMIT 1
        ) p ON TRUE
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
        """,
        (vec, vec, limit),
    )
    return jsonify({
        "query": q,
        "mode": "semantic",
        "model": embeddings.ACTIVE_MODEL,
        "results": rows,
        "warning": _embedding_model_warning("ingredient_embeddings"),
    })


@app.get("/api/search/status")
def search_status():
    """Whether semantic search is actually usable, and why not if it isn't."""
    counts = {}
    for table in ("ingredient_embeddings", "recipe_embeddings",
                  "recipe_chunk_embeddings", "cooking_log_embeddings"):
        try:
            counts[table] = run_one(f"SELECT COUNT(*) AS n FROM {table}")["n"]
        except Exception as exc:
            counts[table] = f"missing ({exc.__class__.__name__}) - run sql/07_vectors.sql"
    return jsonify({
        "model": embeddings.ACTIVE_MODEL,
        "model_loaded": embeddings.available(),
        "embedding_counts": counts,
        "warnings": [w for w in (
            _embedding_model_warning("recipe_embeddings"),
            _embedding_model_warning("ingredient_embeddings"),
        ) if w],
    })


# --------------------------------------------------------------------------
# the agent (Stage 3)
# --------------------------------------------------------------------------

@app.post("/api/agent/chat")
def agent_chat():
    """Run the planning agent. Tools read the catalogue and write real rows:
    meal plans, cooking-log entries and grocery lists."""
    messages = body().get("messages")
    if not isinstance(messages, list) or not messages:
        return bad_request("messages must be a non-empty list")

    clean = [
        {"role": m["role"], "content": str(m.get("content") or "")}
        for m in messages
        if isinstance(m, dict) and m.get("role") in ("user", "assistant")
    ][-12:]                       # keep the context window bounded
    if not clean:
        return bad_request("no usable messages")

    import agent
    try:
        return jsonify(agent.chat(clean, DEFAULT_HOUSEHOLD_ID))
    except Exception as exc:
        app.logger.exception("agent failed")
        return jsonify({
            "error": f"{type(exc).__name__}: {exc}",
            "hint": f"Check the '{agent.AGENT_ENDPOINT}' serving endpoint "
                    f"exists and supports tool calling.",
        }), 502


@app.get("/api/plans/current")
def current_plan():
    """The most recent plan, with each day's dish and its numbers."""
    plan = run_one(
        """
        SELECT * FROM meal_plans WHERE household_id = %s
        ORDER BY week_start DESC LIMIT 1
        """,
        (DEFAULT_HOUSEHOLD_ID,),
    )
    if plan is None:
        return jsonify({"plan": None, "days": [], "grocery": None})

    days = run_query(
        """
        SELECT i.item_id, i.plan_date, i.recipe_id, i.base_servings, i.notes,
               r.title, r.cuisine, r.duration_min, r.thumbnail_url, r.video_id,
               r.halal_status, r.is_vegetarian, r.contains_pork,
               l.log_id, l.was_planned, l.deviation_reason,
               a.title AS actually_cooked
        FROM meal_plan_items i
        LEFT JOIN recipes r ON r.recipe_id = i.recipe_id
        LEFT JOIN cooking_log l ON l.household_id = %s AND l.cooked_date = i.plan_date
        LEFT JOIN recipes a ON a.recipe_id = l.actual_recipe_id
        WHERE i.plan_id = %s
        ORDER BY i.plan_date
        """,
        (DEFAULT_HOUSEHOLD_ID, plan["plan_id"]),
    )

    grocery = run_one(
        """
        SELECT g.list_id, g.total_eur,
               (SELECT COUNT(*) FROM grocery_items gi WHERE gi.list_id = g.list_id) AS items
        FROM grocery_lists g WHERE g.plan_id = %s
        """,
        (plan["plan_id"],),
    )
    items = run_query(
        """
        SELECT gi.display_name, gi.quantity, gi.unit, gi.est_price_eur,
               gi.price_source, s.name AS store_name
        FROM grocery_items gi
        LEFT JOIN stores s ON s.store_id = gi.store_id
        WHERE gi.list_id = %s
        ORDER BY s.name NULLS LAST, gi.est_price_eur DESC NULLS LAST
        """,
        (grocery["list_id"],),
    ) if grocery else []

    return jsonify({"plan": plan, "days": days, "grocery": grocery,
                    "grocery_items": items})


# --------------------------------------------------------------------------
# pipeline status - proves the Spark jobs actually landed data
# --------------------------------------------------------------------------

@app.get("/api/stats")
def stats():
    counts = run_one(
        """
        SELECT (SELECT COUNT(*) FROM ingredients)        AS ingredients,
               (SELECT COUNT(*) FROM ingredient_prices)  AS prices,
               (SELECT COUNT(*) FROM raw_off_products)   AS off_products,
               (SELECT COUNT(*) FROM raw_receipts)       AS receipts,
               (SELECT COUNT(*) FROM receipt_line_items) AS receipt_lines,
               (SELECT COUNT(*) FROM recipes)            AS recipes,
               (SELECT COUNT(*) FROM cooking_log)        AS cooking_log_entries
        """
    )
    provenance = run_query(
        """
        SELECT source, COUNT(*) AS n, ROUND(AVG(confidence), 2) AS avg_confidence
        FROM ingredient_prices
        GROUP BY source
        ORDER BY n DESC
        """
    )
    halal = run_query(
        """
        SELECT halal_status, COUNT(*) AS n
        FROM ingredients
        GROUP BY halal_status
        ORDER BY n DESC
        """
    )
    return jsonify({
        "counts": counts,
        "price_provenance": provenance,
        "halal_coverage": halal,
    })


if __name__ == "__main__":
    # Load the embedding model at startup so the first search isn't a cold
    # start. It's best-effort: if it fails the app still serves everything
    # else and search degrades to keyword matching.
    embeddings.warm_model()

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), debug=False)
