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

from lakebase import run_one, run_query, run_returning, run_write
from nutrition import calculate_targets

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


@app.get("/api/stores")
def list_stores():
    return jsonify(run_query("SELECT * FROM stores ORDER BY name"))


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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), debug=False)
