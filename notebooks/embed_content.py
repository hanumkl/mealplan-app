# Databricks notebook source
# MAGIC %md
# MAGIC # Embeddings -> pgvector in Lakebase
# MAGIC
# MAGIC Builds the vector indexes that make semantic search work: ingredients,
# MAGIC recipes, recipe step chunks, and cooking-log notes.
# MAGIC
# MAGIC ## Why a multilingual model
# MAGIC
# MAGIC The course uses `all-MiniLM-L6-v2`, which is **English-only**. This project
# MAGIC is deliberately trilingual - Finnish product names, Indonesian recipes,
# MAGIC English queries - so it uses
# MAGIC `paraphrase-multilingual-MiniLM-L12-v2` instead. Same 384 dimensions, so
# MAGIC the schema is unchanged, but all three languages land in one shared vector
# MAGIC space. That's what lets "chicken" retrieve *broileri* and *ayam*.
# MAGIC
# MAGIC ## Incremental by design
# MAGIC
# MAGIC Reads the `*_needing_embedding` views, so re-running only embeds what's
# MAGIC new. Re-embedding 8,500 ingredients on every run would be slow and
# MAGIC pointless.
# MAGIC
# MAGIC Requires: `sql/07_vectors.sql` applied.

# COMMAND ----------

# MAGIC %pip install sentence-transformers
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("model_name",
                     "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                     "Embedding model")
dbutils.widgets.text("batch_size", "256", "Encode batch size")
dbutils.widgets.text("chunk_size", "600", "Instruction chunk size (chars)")
dbutils.widgets.text("chunk_overlap", "80", "Chunk overlap (chars)")
dbutils.widgets.dropdown("reembed_all", "false", ["true", "false"],
                         "Re-embed everything (e.g. after a model change)")
dbutils.widgets.text("lakebase_scope", "database", "Secret scope")
dbutils.widgets.text("lakebase_key", "lakebase-url", "Secret key")

MODEL_NAME = dbutils.widgets.get("model_name")
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
CHUNK_SIZE = int(dbutils.widgets.get("chunk_size"))
CHUNK_OVERLAP = int(dbutils.widgets.get("chunk_overlap"))
REEMBED_ALL = dbutils.widgets.get("reembed_all") == "true"

LAKEBASE_URL = dbutils.secrets.get(
    scope=dbutils.widgets.get("lakebase_scope"),
    key=dbutils.widgets.get("lakebase_key"),
)

# COMMAND ----------

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from sentence_transformers import SentenceTransformer

model = SentenceTransformer(MODEL_NAME)
EMBEDDING_DIM = model.get_sentence_embedding_dimension()
print(f"{MODEL_NAME}\ndimension: {EMBEDDING_DIM}")

# The VECTOR(n) columns are declared as 384. A mismatch here would fail on
# every insert, so check once up front with a clear message.
assert EMBEDDING_DIM == 384, (
    f"sql/07_vectors.sql declares VECTOR(384) but this model produces "
    f"{EMBEDDING_DIM}. Change the schema or pick a 384-dim model."
)

# COMMAND ----------

def fetch(sql, params=None):
    with psycopg2.connect(LAKEBASE_URL, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def embed(texts: list[str]):
    """Encode to plain Python lists - psycopg2 can't adapt numpy arrays."""
    vectors = model.encode(
        texts, batch_size=BATCH_SIZE, show_progress_bar=True,
        normalize_embeddings=True,          # cosine distance expects unit norm
    )
    return [v.tolist() for v in vectors]


def write_embeddings(table: str, id_column: str, rows: list[tuple]):
    """rows: (id, source_text, embedding_list)"""
    if not rows:
        print(f"  {table}: nothing to write")
        return
    sql = f"""
        INSERT INTO {table} ({id_column}, source_text, embedding, model_name)
        VALUES %s
        ON CONFLICT ({id_column}) DO UPDATE SET
            source_text = EXCLUDED.source_text,
            embedding   = EXCLUDED.embedding,
            model_name  = EXCLUDED.model_name,
            embedded_at = now()
    """
    with psycopg2.connect(LAKEBASE_URL) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur, sql,
                [(i, t, str(v), MODEL_NAME) for i, t, v in rows],
                template="(%s, %s, %s::vector, %s)",
                page_size=200,
            )
        conn.commit()
    print(f"  {table}: wrote {len(rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingredients
# MAGIC
# MAGIC Embeds every name we have for a product at once - Finnish, English and
# MAGIC the English category - so a query in any of them retrieves it.

# COMMAND ----------

source = ("SELECT ingredient_id, canonical_name, name_fi, name_en, category_en "
          "FROM ingredients") if REEMBED_ALL else \
         "SELECT * FROM ingredients_needing_embedding"

ingredients = fetch(source)
print(f"{len(ingredients)} ingredients to embed")

if ingredients:
    def ingredient_text(r):
        parts = [r["canonical_name"]]
        for extra in (r.get("name_en"), r.get("name_fi"), r.get("category_en")):
            if extra and extra.lower() != r["canonical_name"].lower():
                parts.append(extra)
        return " | ".join(parts)

    texts = [ingredient_text(r) for r in ingredients]
    vectors = embed(texts)
    write_embeddings(
        "ingredient_embeddings", "ingredient_id",
        [(r["ingredient_id"], t, v)
         for r, t, v in zip(ingredients, texts, vectors)],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Recipes
# MAGIC
# MAGIC Title, cuisine and the full ingredient list go into one vector, so a
# MAGIC query like "warm soupy, nothing over 40 minutes, no chicken" matches on
# MAGIC the dish as a whole rather than just its name.

# COMMAND ----------

recipes = fetch(
    "SELECT recipe_id, title, cuisine, description, instructions FROM recipes"
    if REEMBED_ALL else
    "SELECT * FROM recipes_needing_embedding"
)
print(f"{len(recipes)} recipes to embed")

if recipes:
    ing_rows = fetch("""
        SELECT recipe_id, string_agg(raw_text, ', ' ORDER BY sort_order) AS ings
        FROM recipe_ingredients
        WHERE recipe_id = ANY(%s)
        GROUP BY recipe_id
    """, ([r["recipe_id"] for r in recipes],))
    ing_by_recipe = {r["recipe_id"]: r["ings"] for r in ing_rows}

    def recipe_text(r):
        parts = [r["title"]]
        if r.get("cuisine"):
            parts.append(f"{r['cuisine']} cuisine")
        ings = ing_by_recipe.get(r["recipe_id"])
        if ings:
            parts.append(f"ingredients: {ings}")
        if r.get("instructions"):
            parts.append(r["instructions"][:1200])
        return "\n".join(parts)

    texts = [recipe_text(r) for r in recipes]
    vectors = embed(texts)
    write_embeddings(
        "recipe_embeddings", "recipe_id",
        [(r["recipe_id"], t, v) for r, t, v in zip(recipes, texts, vectors)],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Recipe step chunks
# MAGIC
# MAGIC Separate vectors per step, so Cook Mode can answer "when do I add the
# MAGIC coconut milk?" instead of returning the whole method.

# COMMAND ----------

def chunk_text(text: str, size: int, overlap: int) -> list[str]:
    """Split on paragraph-ish boundaries, falling back to a hard cut."""
    text = (text or "").strip()
    if not text:
        return []
    chunks, start = [], 0
    while start < len(text):
        end = min(start + size, len(text))
        if end < len(text):
            # prefer to break at a sentence or newline
            for sep in ("\n", ". "):
                cut = text.rfind(sep, start + size // 2, end)
                if cut != -1:
                    end = cut + len(sep)
                    break
        chunks.append(text[start:end].strip())
        if end >= len(text):
            break
        start = max(start + 1, end - overlap)
    return [c for c in chunks if len(c) > 30]


chunk_sources = fetch("""
    SELECT r.recipe_id, r.instructions
    FROM recipes r
    WHERE r.instructions IS NOT NULL
      AND length(r.instructions) > 100
      AND (%s OR NOT EXISTS (
            SELECT 1 FROM recipe_chunk_embeddings c WHERE c.recipe_id = r.recipe_id))
""", (REEMBED_ALL,))

pending = []
for r in chunk_sources:
    for idx, chunk in enumerate(chunk_text(r["instructions"], CHUNK_SIZE, CHUNK_OVERLAP)):
        pending.append((r["recipe_id"], idx, chunk))

print(f"{len(pending)} chunks from {len(chunk_sources)} recipes")

if pending:
    vectors = embed([c for _, _, c in pending])
    with psycopg2.connect(LAKEBASE_URL) as conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO recipe_chunk_embeddings
                    (recipe_id, chunk_index, chunk_text, embedding, model_name)
                VALUES %s
                ON CONFLICT (recipe_id, chunk_index) DO UPDATE SET
                    chunk_text  = EXCLUDED.chunk_text,
                    embedding   = EXCLUDED.embedding,
                    model_name  = EXCLUDED.model_name,
                    embedded_at = now()
            """, [(rid, idx, txt, str(vec), MODEL_NAME)
                  for (rid, idx, txt), vec in zip(pending, vectors)],
                template="(%s, %s, %s, %s::vector, %s)", page_size=200)
        conn.commit()
    print(f"  recipe_chunk_embeddings: wrote {len(pending)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cooking log notes
# MAGIC
# MAGIC Empty until Stage 3 starts logging what was actually cooked. This is the
# MAGIC feedback loop: the free-text reason you swapped a meal becomes retrievable
# MAGIC context for next week's plan.

# COMMAND ----------

logs = fetch("""
    SELECT l.log_id, l.deviation_reason, l.mood_note, l.cooked_date,
           r.title AS actual_title
    FROM cooking_log l
    LEFT JOIN recipes r ON r.recipe_id = l.actual_recipe_id
    WHERE (l.deviation_reason IS NOT NULL OR l.mood_note IS NOT NULL)
      AND (%s OR NOT EXISTS (
            SELECT 1 FROM cooking_log_embeddings e WHERE e.log_id = l.log_id))
""", (REEMBED_ALL,))

print(f"{len(logs)} cooking-log notes to embed")

if logs:
    def log_text(r):
        parts = [f"cooked on {r['cooked_date']}"]
        if r.get("actual_title"):
            parts.append(f"made {r['actual_title']}")
        for field in ("deviation_reason", "mood_note"):
            if r.get(field):
                parts.append(r[field])
        return ". ".join(parts)

    texts = [log_text(r) for r in logs]
    vectors = embed(texts)
    write_embeddings("cooking_log_embeddings", "log_id",
                     [(r["log_id"], t, v) for r, t, v in zip(logs, texts, vectors)])

# COMMAND ----------

# MAGIC %md ## 5. Verify retrieval actually works

# COMMAND ----------

def semantic_search(query: str, table: str, id_col: str, limit: int = 5):
    """1 - cosine_distance, so higher is more similar."""
    vector = embed([query])[0]
    return fetch(f"""
        SELECT {id_col}, source_text,
               1 - (embedding <=> %s::vector) AS similarity
        FROM {table}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """, (str(vector), str(vector), limit))


# The cross-language test: an English query should retrieve Finnish products.
for probe in ["chicken breast", "rice", "milk"]:
    print(f"\n=== '{probe}' -> ingredients ===")
    for r in semantic_search(probe, "ingredient_embeddings", "ingredient_id"):
        print(f"  {r['similarity']:.3f}  {r['source_text'][:70]}")

# COMMAND ----------

for probe in ["something warm and soupy for a cold day",
              "quick vegetarian dinner under 30 minutes",
              "spicy indonesian chicken"]:
    print(f"\n=== '{probe}' -> recipes ===")
    for r in semantic_search(probe, "recipe_embeddings", "recipe_id"):
        print(f"  {r['similarity']:.3f}  {r['source_text'][:70]}")

# COMMAND ----------

with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        for table in ("ingredient_embeddings", "recipe_embeddings",
                      "recipe_chunk_embeddings", "cooking_log_embeddings"):
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table:28s} {cur.fetchone()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC
# MAGIC If the cross-language probes above returned sensible Finnish products for
# MAGIC English queries, the multilingual model is doing its job. If they look
# MAGIC random, check that `model_name` really is the multilingual one - the
# MAGIC English-only model produces plausible-looking vectors that simply don't
# MAGIC align across languages.
