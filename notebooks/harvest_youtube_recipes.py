# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube -> recipe catalogue with structured ingredients
# MAGIC
# MAGIC Harvests cooking videos, extracts a structured ingredient list from each
# MAGIC free-text description with an LLM, and writes plannable recipes to Lakebase.
# MAGIC
# MAGIC ## Why the description, not the transcript
# MAGIC
# MAGIC The official API can only download captions for videos **you own** - that
# MAGIC needs OAuth as the channel owner, so transcripts are off the table. It
# MAGIC doesn't matter: recipe channels put the full ingredient list in the video
# MAGIC description, and `videos.list?part=snippet` returns it for **1 quota unit**.
# MAGIC
# MAGIC ## Quota
# MAGIC
# MAGIC | call | cost | why it matters |
# MAGIC |---|---|---|
# MAGIC | `search.list` | **100 units** | the expensive one - only ~90/day |
# MAGIC | `videos.list` | **1 unit** | effectively free, batches 50 ids per call |
# MAGIC
# MAGIC Default daily quota is 10,000 units. The defaults below use ~20 searches
# MAGIC (2,000 units) plus a few hundred detail lookups, so roughly a quarter of a
# MAGIC day's budget. **Never search at request time** - harvest here, serve from
# MAGIC Postgres.
# MAGIC
# MAGIC ## This is the unstructured-data step
# MAGIC
# MAGIC Descriptions are genuinely messy: mixed Indonesian and English, "2 sdm
# MAGIC kecap manis", "1 ekor ayam potong 8", emoji, timestamps, sponsor links.
# MAGIC Turning that into typed rows with quantities and units is the hard part,
# MAGIC and every recipe carries an `extraction_confidence` so bad parses can be
# MAGIC reviewed rather than silently planned around.
# MAGIC
# MAGIC Requires: `mealplan/youtube-api-key`, `mealplan/lakebase-url`, and
# MAGIC `sql/07_vectors.sql` applied.

# COMMAND ----------

# MAGIC %pip install requests
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("queries", (
    "resep rendang daging,resep soto ayam,resep ayam bakar kecap,"
    "resep gado gado,resep tempe orek,resep sayur lodeh,resep nasi goreng,"
    "resep opor ayam,resep sop buntut,resep pepes tahu,"
    "easy chicken traybake dinner,vegetarian lentil curry recipe,"
    "meal prep salmon dinner,thai green curry recipe,japanese chicken teriyaki"
), "Search queries, comma separated")
dbutils.widgets.text("max_per_query", "25", "Videos per query")
dbutils.widgets.text("staging_volume", "/Volumes/workspace/default/raw", "Volume for raw JSON")
dbutils.widgets.text("llm_endpoint", "databricks-llama-4-maverick", "LLM serving endpoint")
dbutils.widgets.text("min_confidence", "0.5", "Auto-approve recipes at or above this")
dbutils.widgets.text("lakebase_scope", "mealplan", "Lakebase secret scope")
dbutils.widgets.text("lakebase_key", "lakebase-url", "Lakebase secret key")
dbutils.widgets.text("youtube_scope", "mealplan", "YouTube secret scope")
dbutils.widgets.text("youtube_key_name", "youtube-api-key", "YouTube secret key")

QUERIES = [q.strip() for q in dbutils.widgets.get("queries").split(",") if q.strip()]
MAX_PER_QUERY = int(dbutils.widgets.get("max_per_query"))
STAGING_VOLUME = dbutils.widgets.get("staging_volume").rstrip("/")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
MIN_CONFIDENCE = float(dbutils.widgets.get("min_confidence"))

LAKEBASE_URL = dbutils.secrets.get(
    scope=dbutils.widgets.get("lakebase_scope"),
    key=dbutils.widgets.get("lakebase_key"),
)
YOUTUBE_API_KEY = dbutils.secrets.get(
    scope=dbutils.widgets.get("youtube_scope"),
    key=dbutils.widgets.get("youtube_key_name"),
)

print(f"{len(QUERIES)} queries x {MAX_PER_QUERY} videos")
print(f"estimated quota: {len(QUERIES) * 100} units (search) "
      f"+ ~{len(QUERIES) * MAX_PER_QUERY // 50} units (details)")

# COMMAND ----------

import json
import re
import time

import requests
from pyspark.sql import functions as F

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

# COMMAND ----------

# MAGIC %md ## 1. Search (expensive) then batch-fetch details (cheap)

# COMMAND ----------

def search_video_ids(query: str, limit: int) -> list[str]:
    """One search.list call = 100 quota units. Keep this loop short."""
    resp = requests.get(SEARCH_URL, params={
        "key": YOUTUBE_API_KEY,
        "q": query,
        "part": "id",
        "type": "video",
        "maxResults": min(limit, 50),
        "videoEmbeddable": "true",   # must be embeddable for Cook Mode
        "relevanceLanguage": "id" if query.startswith("resep") else "en",
    }, timeout=60)

    if resp.status_code == 403:
        raise RuntimeError(
            "YouTube API returned 403. Either the daily quota is exhausted, or "
            "the YouTube Data API v3 isn't enabled on your Google Cloud project."
        )
    resp.raise_for_status()
    return [i["id"]["videoId"] for i in resp.json().get("items", [])
            if i.get("id", {}).get("videoId")]


def fetch_video_details(video_ids: list[str]) -> list[dict]:
    """videos.list costs 1 unit and accepts 50 ids per call."""
    out = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        resp = requests.get(VIDEOS_URL, params={
            "key": YOUTUBE_API_KEY,
            "id": ",".join(batch),
            "part": "snippet,contentDetails,statistics",
        }, timeout=60)
        resp.raise_for_status()
        out.extend(resp.json().get("items", []))
        time.sleep(0.2)
    return out

# COMMAND ----------

all_ids, seen = [], set()
for query in QUERIES:
    try:
        ids = search_video_ids(query, MAX_PER_QUERY)
    except Exception as exc:
        print(f"  {query!r}: {exc}")
        continue
    fresh = [v for v in ids if v not in seen]
    seen.update(fresh)
    all_ids.extend(fresh)
    print(f"  {query!r}: {len(ids)} results, {len(fresh)} new")
    time.sleep(0.3)

print(f"\n{len(all_ids)} unique videos")
videos = fetch_video_details(all_ids)
print(f"fetched details for {len(videos)}")

# Land the raw payload, same as the OFF pipeline: serverless blocks
# SparkContext access, and an immutable copy means re-parsing never costs
# another 100-unit search.
stamp = time.strftime("%Y%m%d-%H%M%S")
raw_path = f"{STAGING_VOLUME}/youtube_raw_{stamp}.jsonl"
with open(raw_path, "w", encoding="utf-8") as fh:
    for v in videos:
        fh.write(json.dumps(v, ensure_ascii=False) + "\n")
print(f"landed -> {raw_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Filter to plausible recipes (Spark)
# MAGIC
# MAGIC A search for "resep rendang" also returns vlogs, restaurant reviews and
# MAGIC 30-second shorts. Sending those to the LLM wastes tokens, so filter on
# MAGIC structure first: usable duration, and a description long enough to
# MAGIC plausibly contain an ingredient list.

# COMMAND ----------

raw_sdf = spark.read.json(raw_path)
print("columns:", sorted(raw_sdf.columns))


def iso8601_minutes(col):
    """PT1H23M45S -> 83. contentDetails.duration is always this format.

    regexp_extract returns an empty string when the pattern doesn't match, not
    NULL - so coalesce never fires, and under ANSI mode (on by default here)
    casting '' to int raises CAST_INVALID_INPUT instead of yielding NULL. Most
    cooking videos are under an hour, so the missing 'H' group breaks nearly
    every row. The empty string has to be turned into NULL first, and the
    `when` short-circuits so the bad cast is never evaluated.
    """
    def part(pattern):
        raw = F.regexp_extract(col, pattern, 1)
        return F.coalesce(F.when(raw != F.lit(""), raw.cast("int")), F.lit(0))

    return part(r"(\d+)H") * 60 + part(r"(\d+)M") + (part(r"(\d+)S") / 60.0)


candidates = (
    raw_sdf
    .select(
        F.col("id").alias("video_id"),
        F.col("snippet.title").alias("title"),
        F.col("snippet.description").alias("description"),
        F.col("snippet.channelTitle").alias("channel_title"),
        F.col("snippet.thumbnails.high.url").alias("thumbnail_url"),
        F.col("snippet.defaultAudioLanguage").alias("language"),
        iso8601_minutes(F.col("contentDetails.duration")).alias("duration_min"),
    )
    .filter(F.col("description").isNotNull())
    # Shorts are too short to teach a recipe; multi-hour videos are compilations.
    .filter((F.col("duration_min") >= 2) & (F.col("duration_min") <= 90))
    # An ingredient list needs room. Below ~200 chars it's a link dump.
    .filter(F.length("description") >= 200)
    .dropDuplicates(["video_id"])
)

print(f"{candidates.count()} of {raw_sdf.count()} videos look like real recipes")
display(candidates.select("title", "channel_title", "duration_min"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract structured ingredients with an LLM
# MAGIC
# MAGIC The genuinely unstructured part. Descriptions mix languages, units and
# MAGIC formats; the model normalises them into typed rows while keeping the
# MAGIC original text so a bad parse never destroys the source.

# COMMAND ----------

EXTRACTION_PROMPT = """You are reading a cooking video's title and description.

Extract the recipe. Return STRICT JSON, no markdown fence:

{
  "is_recipe": true | false,
  "title": "clean dish name, in English",
  "title_original": "dish name in the original language, or null",
  "cuisine": "indonesian" | "malaysian" | "thai" | "japanese" | "chinese" |
             "indian" | "western" | "middle_eastern" | "other",
  "base_servings": number or null,
  "total_minutes": number or null,
  "instructions": "numbered steps, concise, English",
  "confidence": 0.0-1.0,
  "ingredients": [
    {
      "raw_text": "exactly as written in the description",
      "name": "plain English ingredient name",
      "quantity": number or null,
      "unit": "g" | "kg" | "ml" | "l" | "tbsp" | "tsp" | "piece" | "clove" | null,
      "is_protein_component": true | false,
      "is_optional": true | false
    }
  ]
}

Rules:
- is_recipe false if this is a vlog, review, or has no ingredient list. Then
  return empty ingredients and stop.
- Keep raw_text EXACTLY as written, including Indonesian units (sdm = tbsp,
  sdt = tsp, ekor = whole, siung = clove, secukupnya = to taste).
- is_protein_component true for meat, fish, eggs, tofu, tempeh, legumes -
  these get cooked separately per household member, so they must be
  identifiable.
- Ignore sponsor messages, social links, subscribe requests and timestamps.
- Set confidence low if the ingredient list was implied rather than written.
- Do not invent ingredients that aren't in the text.
"""


def extract_recipe(title: str, description: str) -> dict:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient().serving_endpoints.get_open_ai_client()
    resp = client.chat.completions.create(
        model=LLM_ENDPOINT,
        max_tokens=3000,
        messages=[{
            "role": "user",
            "content": f"{EXTRACTION_PROMPT}\n\nTITLE: {title}\n\nDESCRIPTION:\n{description[:6000]}",
        }],
    )
    text = resp.choices[0].message.content.strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    return json.loads(text)

# COMMAND ----------

rows = candidates.collect()
extracted = []

for i, row in enumerate(rows, 1):
    try:
        parsed = extract_recipe(row["title"], row["description"])
        parsed["_video"] = row.asDict()
        extracted.append(parsed)
        flag = "recipe" if parsed.get("is_recipe") else "not a recipe"
        n_ing = len(parsed.get("ingredients", []))
        print(f"{i:3d}/{len(rows)} {flag:12s} conf={parsed.get('confidence')} "
              f"{n_ing:2d} ing  {row['title'][:44]}")
    except Exception as exc:
        print(f"{i:3d}/{len(rows)} FAILED  {type(exc).__name__}: {exc}  {row['title'][:40]}")

recipes = [r for r in extracted if r.get("is_recipe") and r.get("ingredients")]
print(f"\n{len(recipes)} usable recipes from {len(rows)} candidates")

# COMMAND ----------

# MAGIC %md ## 4. Write recipes and ingredients to Lakebase

# COMMAND ----------

import psycopg2

UPSERT_RECIPE = """
INSERT INTO recipes (
    title, cuisine, language, source, source_ref, video_id, video_url,
    channel_title, thumbnail_url, duration_min, base_servings,
    description, instructions, extraction_confidence, review_status,
    is_vegetarian, is_vegan, contains_pork, contains_gluten, contains_lactose,
    halal_status
) VALUES (%s, %s, %s, 'youtube', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s)
ON CONFLICT (source, source_ref) DO UPDATE SET
    title                 = EXCLUDED.title,
    cuisine               = EXCLUDED.cuisine,
    instructions          = EXCLUDED.instructions,
    extraction_confidence = EXCLUDED.extraction_confidence,
    is_vegetarian         = EXCLUDED.is_vegetarian,
    is_vegan              = EXCLUDED.is_vegan,
    contains_pork         = EXCLUDED.contains_pork,
    contains_gluten       = EXCLUDED.contains_gluten,
    contains_lactose      = EXCLUDED.contains_lactose,
    halal_status          = EXCLUDED.halal_status
RETURNING recipe_id
"""

INSERT_RECIPE_INGREDIENT = """
INSERT INTO recipe_ingredients
    (recipe_id, raw_text, ingredient_name, quantity, unit, scaling_class,
     is_protein_component, is_optional, sort_order)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Spices and oils must not scale linearly when a recipe is tripled - same
# rule the ingredient catalogue uses.
SUBLINEAR = ["salt", "pepper", "chilli", "chili", "spice", "garam", "cumin",
             "oil", "sugar", "cinnamon", "turmeric", "coriander", "paprika"]


def scaling_class_for(name: str) -> str:
    low = (name or "").lower()
    return "sublinear" if any(t in low for t in SUBLINEAR) else "linear"


def safe_num(v):
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Diet + halal flags, derived from the extracted ingredient list.
#
# These columns are what the app filters a household's strict restrictions on.
# Left NULL they are worse than useless: `contains_pork IS NOT TRUE` passes a
# NULL, so a pork recipe would be shown to a halal household as if it were
# fine. Deriving them here is what makes that filter mean something.
#
# Word-boundary matching, not substring - the catalogue pipeline learned this
# the hard way when "rum" matched du-rum-wheat and flagged couscous as
# alcoholic. Indonesian terms are included because the recipes are Indonesian.
# ---------------------------------------------------------------------------

PORK_TOKENS = ["pork", "bacon", "ham", "lard", "gammon", "prosciutto",
               "pancetta", "chorizo", "salami", "pepperoni", "babi", "speck"]
ALCOHOL_TOKENS = ["wine", "beer", "rum", "sake", "mirin", "vodka", "brandy",
                  "sherry", "whisky", "whiskey", "arak", "shaoxing"]
MEAT_TOKENS = ["chicken", "beef", "lamb", "mutton", "goat", "duck", "turkey",
               "veal", "fish", "shrimp", "prawn", "squid", "anchovy", "tuna",
               "salmon", "crab", "clam", "mussel", "oyster", "gelatin",
               "gelatine", "ayam", "sapi", "kambing", "bebek", "ikan",
               "udang", "cumi", "teri", "daging"] + PORK_TOKENS
DAIRY_TOKENS = ["milk", "cheese", "butter", "cream", "yoghurt", "yogurt",
                "ghee", "susu", "keju", "mentega", "krim"]
# Plant foods whose names contain a dairy word. Without these, "coconut milk"
# matches "milk" and almost every Indonesian recipe is wrongly marked as
# containing lactose - which would hide all of them from the lactose-free
# member. Same for the peanut butter in gado-gado and satay sauce.
NON_DAIRY_PHRASES = ["coconut milk", "coconut cream", "santan", "almond milk",
                     "soy milk", "soya milk", "oat milk", "rice milk",
                     "cashew milk", "peanut butter", "almond butter",
                     "nut butter", "cocoa butter", "shea butter",
                     "coconut butter", "susu kelapa", "kelapa"]
EGG_TOKENS = ["egg", "telur", "mayonnaise", "mayo"]
# Wheat-specific. Plain "flour"/"noodle" is ambiguous - rice flour and rice
# noodles are gluten-free - so those are only counted via explicit wheat words.
GLUTEN_TOKENS = ["wheat", "flour", "bread", "pasta", "spaghetti", "macaroni",
                 "barley", "rye", "semolina", "panko", "breadcrumb", "couscous",
                 "soy sauce", "kecap", "terigu", "roti", "mie", "noodle",
                 "seitan", "malt"]
GLUTEN_FREE_QUALIFIERS = ["rice flour", "rice noodle", "glass noodle",
                          "tepung beras", "bihun", "gluten-free", "gluten free",
                          "tamari", "cornflour", "corn flour", "tapioca"]


def _has_token(text: str, tokens) -> bool:
    return any(re.search(rf"(?:^|[^a-z0-9]){re.escape(t)}(?:[^a-z0-9]|$)", text)
               for t in tokens)


def derive_flags(recipe: dict) -> dict:
    """Return the diet/halal columns for one extracted recipe."""
    blob = " ".join([
        recipe.get("title") or "",
        " ".join(str(i.get("name") or "") for i in recipe.get("ingredients", [])),
        " ".join(str(i.get("raw_text") or "") for i in recipe.get("ingredients", [])),
    ]).lower()

    has_pork = _has_token(blob, PORK_TOKENS)
    has_alcohol = _has_token(blob, ALCOHOL_TOKENS)
    has_meat = _has_token(blob, MEAT_TOKENS)
    has_egg = _has_token(blob, EGG_TOKENS)

    # Strip plant-based "milk"/"butter" phrases before looking for dairy, so
    # coconut milk and peanut butter don't read as lactose.
    dairy_blob = blob
    for phrase in NON_DAIRY_PHRASES:
        dairy_blob = dairy_blob.replace(phrase, " ")
    has_dairy = _has_token(dairy_blob, DAIRY_TOKENS)

    gluten = _has_token(blob, GLUTEN_TOKENS)
    if gluten and any(q in blob for q in GLUTEN_FREE_QUALIFIERS):
        # An explicit gluten-free form of an otherwise-glutenous word appears,
        # so we can't tell which one it is. Say so instead of guessing.
        gluten = None

    return {
        "is_vegetarian": not has_meat,
        "is_vegan": not (has_meat or has_dairy or has_egg),
        "contains_pork": has_pork,
        "contains_gluten": gluten,
        "contains_lactose": has_dairy,
        # Deliberately never 'certified' or 'likely_ok'. A YouTube description
        # is not evidence that meat was slaughtered halal, so the best an
        # unflagged recipe can earn here is 'unknown' - the household confirms
        # it, exactly like the ingredient catalogue.
        "halal_status": "contains_flagged" if (has_pork or has_alcohol) else "unknown",
    }


written_recipes = 0
written_ingredients = 0

with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        for r in recipes:
            v = r["_video"]
            confidence = safe_num(r.get("confidence")) or 0.0
            review = "approved" if confidence >= MIN_CONFIDENCE else "pending"
            flags = derive_flags(r)

            cur.execute(UPSERT_RECIPE, (
                (r.get("title") or v["title"])[:300],
                r.get("cuisine"),
                v.get("language") or "en",
                v["video_id"],                       # source_ref
                v["video_id"],
                f"https://www.youtube.com/watch?v={v['video_id']}",
                v.get("channel_title"),
                v.get("thumbnail_url"),
                int(v["duration_min"]) if v.get("duration_min") else None,
                safe_num(r.get("base_servings")) or 4,
                (v.get("description") or "")[:8000],
                r.get("instructions"),
                confidence,
                review,
                flags["is_vegetarian"],
                flags["is_vegan"],
                flags["contains_pork"],
                flags["contains_gluten"],
                flags["contains_lactose"],
                flags["halal_status"],
            ))
            recipe_id = cur.fetchone()[0]
            written_recipes += 1

            # Replace rather than append, so re-running doesn't duplicate rows.
            cur.execute("DELETE FROM recipe_ingredients WHERE recipe_id = %s",
                        (recipe_id,))
            for idx, ing in enumerate(r.get("ingredients", [])):
                cur.execute(INSERT_RECIPE_INGREDIENT, (
                    recipe_id,
                    str(ing.get("raw_text") or ing.get("name") or "")[:500],
                    # Clean English name, kept separately - it is what the
                    # catalogue matcher embeds.
                    (str(ing["name"])[:200] if ing.get("name") else None),
                    safe_num(ing.get("quantity")),
                    ing.get("unit"),
                    scaling_class_for(ing.get("name")),
                    bool(ing.get("is_protein_component")),
                    bool(ing.get("is_optional")),
                    idx,
                ))
                written_ingredients += 1
    conn.commit()

print(f"wrote {written_recipes} recipes, {written_ingredients} ingredient lines")

# COMMAND ----------

with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cuisine, review_status, COUNT(*)
            FROM recipes GROUP BY cuisine, review_status ORDER BY 3 DESC
        """)
        print(f"{'cuisine':16s} {'status':10s} n")
        for cuisine, status, n in cur.fetchall():
            print(f"  {str(cuisine):16s} {status:10s} {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC
# MAGIC Recipes below `min_confidence` land as `pending` rather than `approved`,
# MAGIC so a shaky parse is never silently planned around.
# MAGIC
# MAGIC Next: `embed_content.py` to build the pgvector indexes over these recipes.
