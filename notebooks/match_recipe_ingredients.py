# Databricks notebook source
# MAGIC %md
# MAGIC # Match recipe ingredients to the priced catalogue
# MAGIC
# MAGIC Fills in `recipe_ingredients.ingredient_id`, which is what turns a recipe
# MAGIC from a wall of text into something with calories, protein and a euro cost.
# MAGIC
# MAGIC ## Why this needs vectors
# MAGIC
# MAGIC The recipe says `chicken thigh`. The catalogue says
# MAGIC `Broilerin koipireisi`. No amount of string matching bridges Indonesian
# MAGIC recipe text to a Finnish product catalogue - that's the whole reason the
# MAGIC embeddings are multilingual, and this notebook is where they earn it.
# MAGIC
# MAGIC ## What it will not do
# MAGIC
# MAGIC Force a match. Below the similarity threshold the row is left unmatched
# MAGIC and reported, because a wrong match is worse than a missing one: it
# MAGIC silently poisons a week's nutrition totals, while a missing one just
# MAGIC shows up as "not matched" in the app.
# MAGIC
# MAGIC A `manual` match is never overwritten - the household outranks the
# MAGIC vector search, same rule as halal confirmation.
# MAGIC
# MAGIC Requires: `sql/07_vectors.sql`, `sql/08_recipe_matching.sql`, and
# MAGIC `notebooks/embed_content.py` already run (it needs `ingredient_embeddings`).

# COMMAND ----------

# MAGIC %pip install sentence-transformers
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("model_name",
                     "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                     "Embedding model (must match embed_content.py)")
dbutils.widgets.text("threshold", "0.55", "Minimum similarity to accept a match")
dbutils.widgets.text("nutrition_margin", "0.10",
                     "Prefer a nutrition-bearing match within this of the best")
dbutils.widgets.text("batch_size", "256", "Encode batch size")
dbutils.widgets.dropdown("rematch_all", "false", ["true", "false"],
                         "Re-match rows that already have a match")
dbutils.widgets.text("lakebase_scope", "database", "Secret scope")
dbutils.widgets.text("lakebase_key", "lakebase-url", "Secret key")

MODEL_NAME = dbutils.widgets.get("model_name")
THRESHOLD = float(dbutils.widgets.get("threshold"))
NUTRITION_MARGIN = float(dbutils.widgets.get("nutrition_margin"))
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
REMATCH_ALL = dbutils.widgets.get("rematch_all") == "true"

LAKEBASE_URL = dbutils.secrets.get(
    scope=dbutils.widgets.get("lakebase_scope"),
    key=dbutils.widgets.get("lakebase_key"),
)

# COMMAND ----------

import re

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from sentence_transformers import SentenceTransformer

model = SentenceTransformer(MODEL_NAME)
print(f"{MODEL_NAME}\ndimension: {model.get_sentence_embedding_dimension()}")

# COMMAND ----------


def fetch(sql, params=None):
    with psycopg2.connect(LAKEBASE_URL, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


# The embeddings this searches against were written by embed_content.py. If a
# different model wrote them, every similarity below is meaningless - so check
# rather than discover it through bad matches.
stored = fetch("SELECT DISTINCT model_name FROM ingredient_embeddings")
stored_models = {r["model_name"] for r in stored}
if not stored_models:
    raise RuntimeError(
        "ingredient_embeddings is empty. Run notebooks/embed_content.py first."
    )
if MODEL_NAME not in stored_models:
    raise RuntimeError(
        f"ingredient_embeddings was written by {sorted(stored_models)} but this "
        f"notebook uses {MODEL_NAME}. Matching across two models produces "
        f"confident nonsense. Set model_name to match, or re-run "
        f"embed_content.py with reembed_all=true."
    )
print(f"embeddings model check OK: {sorted(stored_models)[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What needs matching

# COMMAND ----------

if REMATCH_ALL:
    # Manual corrections still survive - only vector/none rows are redone.
    pending = fetch("""
        SELECT ri_id, recipe_id, raw_text, ingredient_name, unit, quantity
        FROM recipe_ingredients
        WHERE match_method <> 'manual'
    """)
else:
    pending = fetch("SELECT * FROM recipe_ingredients_needing_match")

print(f"{len(pending)} ingredient lines to match")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clean the text before embedding
# MAGIC
# MAGIC `ingredient_name` is the LLM's clean English name and is used when
# MAGIC present. Older rows only have `raw_text` like `"2 sdm kecap manis"`,
# MAGIC where the quantity and unit are noise - stripping them measurably
# MAGIC improves the match.

# COMMAND ----------

LEADING_QTY = re.compile(
    r"^\s*[\d\s./,-]*\s*"
    r"(kg|g|gr|gram|ml|l|liter|litre|tbsp|tsp|sdm|sdt|cup|gelas|buah|biji|"
    r"butir|siung|ekor|lembar|batang|ikat|clove|cloves|piece|pieces|pcs|"
    r"slice|slices|can|kaleng|pack|sachet|bungkus)?\s*",
    re.IGNORECASE,
)
TRAILING_NOTE = re.compile(
    r"\s*[,(].*$|\s*\b(secukupnya|to taste|optional|opsional|chopped|diced|"
    r"sliced|minced|iris|potong|cincang|halus|kasar)\b.*$",
    re.IGNORECASE,
)


def match_text(row: dict) -> str:
    name = (row.get("ingredient_name") or "").strip()
    if name:
        return name
    text = (row.get("raw_text") or "").strip()
    text = LEADING_QTY.sub("", text, count=1)
    text = TRAILING_NOTE.sub("", text)
    return text.strip() or (row.get("raw_text") or "").strip()


texts = [match_text(r) for r in pending]
for r, t in list(zip(pending, texts))[:15]:
    print(f"  {r['raw_text'][:44]:46s} -> {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Match
# MAGIC
# MAGIC Takes the 10 nearest catalogue entries, then prefers one that actually
# MAGIC carries nutrition data if it's within `nutrition_margin` of the best
# MAGIC score. A marginally-closer product with no calories is useless here -
# MAGIC the entire point of matching is to get calories and a price.

# COMMAND ----------

SEARCH = """
SELECT i.ingredient_id, i.canonical_name, i.kcal_per_100g,
       1 - (e.embedding <=> %s::vector) AS similarity
FROM ingredient_embeddings e
JOIN ingredients i ON i.ingredient_id = e.ingredient_id
ORDER BY e.embedding <=> %s::vector
LIMIT 10
"""

results = []
if pending:
    vectors = model.encode(texts, batch_size=BATCH_SIZE, show_progress_bar=True,
                           normalize_embeddings=True)

    with psycopg2.connect(LAKEBASE_URL, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            for row, text, vec in zip(pending, texts, vectors):
                lit = str(vec.tolist())
                cur.execute(SEARCH, (lit, lit))
                candidates = cur.fetchall()
                if not candidates:
                    results.append((row, text, None, None))
                    continue

                best = candidates[0]
                nutritious = [c for c in candidates if c["kcal_per_100g"] is not None]
                if nutritious and nutritious[0]["similarity"] >= (
                        best["similarity"] - NUTRITION_MARGIN):
                    best = nutritious[0]

                if best["similarity"] >= THRESHOLD:
                    results.append((row, text, best, float(best["similarity"])))
                else:
                    # Reported, not forced. A wrong match poisons the numbers
                    # silently; an unmatched row is visible in the app.
                    results.append((row, text, None, float(best["similarity"])))

matched = [r for r in results if r[2] is not None]
print(f"\nmatched {len(matched)} of {len(results)}")

# COMMAND ----------

# MAGIC %md ## 4. Review before writing

# COMMAND ----------

print("--- accepted (lowest confidence first) ---")
for row, text, best, sim in sorted(matched, key=lambda x: x[3])[:25]:
    print(f"  {sim:.3f}  {text[:34]:36s} -> {best['canonical_name'][:44]}")

print("\n--- rejected (below threshold) ---")
rejected = [r for r in results if r[2] is None]
for row, text, best, sim in sorted(rejected, key=lambda x: -(x[3] or 0))[:25]:
    print(f"  {(sim or 0):.3f}  {text[:34]:36s} (best was still too far)")
print(f"\n{len(rejected)} left unmatched - they show as 'unmatched' in the app")

# COMMAND ----------

# MAGIC %md ## 5. Write

# COMMAND ----------

if matched:
    with psycopg2.connect(LAKEBASE_URL) as conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                UPDATE recipe_ingredients ri
                SET ingredient_id    = v.ingredient_id,
                    match_confidence = v.confidence,
                    match_method     = 'vector',
                    matched_at       = now()
                FROM (VALUES %s) AS v(ri_id, ingredient_id, confidence)
                WHERE ri.ri_id = v.ri_id
                  AND ri.match_method <> 'manual'
            """, [(row["ri_id"], best["ingredient_id"], round(sim, 3))
                  for row, _, best, sim in matched],
                template="(%s::int, %s::int, %s::numeric)", page_size=200)
        conn.commit()
    print(f"wrote {len(matched)} matches")

# COMMAND ----------

# MAGIC %md ## 6. Coverage per recipe

# COMMAND ----------

for r in fetch("""
    SELECT r.title, c.total_ingredients, c.matched_ingredients,
           c.with_nutrition, c.avg_match_confidence
    FROM recipe_match_coverage c
    JOIN recipes r ON r.recipe_id = c.recipe_id
    ORDER BY c.matched_ingredients::float
             / NULLIF(c.total_ingredients, 0) ASC NULLS LAST
    LIMIT 30
"""):
    total = r["total_ingredients"] or 0
    pct = (r["matched_ingredients"] / total * 100) if total else 0
    print(f"  {pct:5.1f}%  {r['matched_ingredients']:2d}/{total:2d} matched, "
          f"{r['with_nutrition']:2d} with nutrition  {r['title'][:40]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Recipes near the bottom of that list will show partial nutrition in the
# MAGIC app, and it says so rather than presenting an incomplete total as fact.
# MAGIC Fix them by confirming a match by hand in the Recipes tab, or widen the
# MAGIC catalogue by re-running the Open Food Facts ingestion.
