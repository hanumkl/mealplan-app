# Databricks notebook source
# MAGIC %md
# MAGIC # Open Food Facts -> Lakebase ingredient catalog
# MAGIC
# MAGIC Spark pipeline that lands Finnish grocery products and derives the dietary
# MAGIC flags the meal planner constrains against.
# MAGIC
# MAGIC ## Which API this uses, and why
# MAGIC
# MAGIC Open Food Facts has two search facilities, and **the older one is down**:
# MAGIC
# MAGIC | endpoint | status | notes |
# MAGIC |---|---|---|
# MAGIC | `world.openfoodfacts.org/api/v2/search` | ❌ 503 | legacy, heavily overloaded |
# MAGIC | `world.openfoodfacts.org/cgi/search.pl` | ❌ 503 | same backend |
# MAGIC | **`search.openfoodfacts.org/search`** | ✅ | Search-a-licious, what we use |
# MAGIC | `world.openfoodfacts.org/api/v2/product/<code>` | ✅ | single product lookups |
# MAGIC
# MAGIC Search-a-licious also accepts `page_size=1000`, so ~10,000 Finnish products
# MAGIC arrive in about 10 requests instead of 100 — which sidesteps rate limiting
# MAGIC almost entirely.
# MAGIC
# MAGIC **One important difference:** it returns `ingredients_tags` (structured, e.g.
# MAGIC `en:wheat-flour`, `en:salt`) rather than free-text `ingredients_text`. That's
# MAGIC better for our purposes — the allergen and halal scans run against canonical
# MAGIC tags instead of guessing at spelling across three languages.
# MAGIC
# MAGIC ## Source modes
# MAGIC
# MAGIC | mode | what it does | when |
# MAGIC |---|---|---|
# MAGIC | `api` | pages Search-a-licious, filtered by country | first run, no setup |
# MAGIC | `dump` | reads the full OFF Parquet dump from a UC Volume | real Spark scale |
# MAGIC
# MAGIC Both paths converge on the same transform. Dump mode additionally carries
# MAGIC `ingredients_text`, which the flag derivation picks up automatically.
# MAGIC
# MAGIC ## Halal flagging
# MAGIC
# MAGIC Deliberately four-valued, never a bare boolean: `certified` (explicit label) /
# MAGIC `contains_flagged` (pork, gelatine, alcohol or carmine found) / `likely_ok`
# MAGIC (vegetarian or vegan label, nothing flagged) / `unknown`. The reason is stored
# MAGIC alongside and shown in the UI.
# MAGIC
# MAGIC Requires: `LAKEBASE_URL` secret in scope `database`, and the SQL in `sql/` run.

# COMMAND ----------

# MAGIC %pip install requests
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.dropdown("source_mode", "api", ["api", "dump"])
dbutils.widgets.text("countries", "en:finland", "Country tag(s), comma separated")
dbutils.widgets.text("page_size", "1000", "api mode: products per request (max 1000)")
dbutils.widgets.text("max_pages", "10", "api mode: pages per country")
dbutils.widgets.text("request_interval_seconds", "2", "api mode: seconds between requests")
dbutils.widgets.text("max_retries", "4", "api mode: retries per page on 429/5xx")
dbutils.widgets.text("staging_volume", "/Volumes/main/mealplan/raw", "api mode: Volume for raw JSON")
dbutils.widgets.text("dump_path", "/Volumes/main/mealplan/off/food.parquet", "dump mode: Volume path")
dbutils.widgets.text("lakebase_scope", "database", "Secret scope")
dbutils.widgets.text("lakebase_key", "lakebase-url", "Secret key")

SOURCE_MODE = dbutils.widgets.get("source_mode")
COUNTRIES = [c.strip() for c in dbutils.widgets.get("countries").split(",") if c.strip()]
PAGE_SIZE = int(dbutils.widgets.get("page_size"))
MAX_PAGES = int(dbutils.widgets.get("max_pages"))
REQUEST_INTERVAL = float(dbutils.widgets.get("request_interval_seconds"))
MAX_RETRIES = int(dbutils.widgets.get("max_retries"))
STAGING_VOLUME = dbutils.widgets.get("staging_volume").rstrip("/")
DUMP_PATH = dbutils.widgets.get("dump_path")

LAKEBASE_URL = dbutils.secrets.get(
    scope=dbutils.widgets.get("lakebase_scope"),
    key=dbutils.widgets.get("lakebase_key"),
)

# COMMAND ----------

import json
import time

import requests
from pyspark.sql import functions as F

SEARCH_URL = "https://search.openfoodfacts.org/search"
USER_AGENT = "mealplan-capstone/1.0 (databricks bootcamp project)"

# COMMAND ----------

# MAGIC %md ## 1. Extract

# COMMAND ----------

def _get_page(country: str, page: int) -> list[dict] | None:
    """Fetch one page of results, retrying on 429/5xx with exponential backoff.

    Returns None when retries are exhausted, so the caller can keep the pages
    that already succeeded rather than losing the whole run.
    """
    delay = max(REQUEST_INTERVAL, 2.0)
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                SEARCH_URL,
                params={
                    "q": f'countries_tags:"{country}"',
                    "page_size": PAGE_SIZE,
                    "page": page,
                },
                headers={"User-Agent": USER_AGENT},
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json().get("hits", [])

            if resp.status_code in (429, 500, 502, 503, 504):
                wait = float(resp.headers.get("Retry-After", delay))
                print(f"    HTTP {resp.status_code} page {page}, "
                      f"retry {attempt}/{MAX_RETRIES} in {wait:.0f}s")
                time.sleep(wait)
                delay *= 2
                continue

            resp.raise_for_status()

        except requests.RequestException as exc:
            last_error = exc
            print(f"    {type(exc).__name__} page {page}, "
                  f"retry {attempt}/{MAX_RETRIES} in {delay:.0f}s")
            time.sleep(delay)
            delay *= 2

    print(f"    giving up on page {page} ({last_error or 'repeated 5xx'})")
    return None


def fetch_country(country: str) -> list[dict]:
    """Page Search-a-licious for one country tag."""
    products = []
    for page in range(1, MAX_PAGES + 1):
        batch = _get_page(country, page)

        if batch is None:
            print(f"  stopped at page {page}, keeping {len(products)} products")
            break
        if not batch:
            print(f"  {country}: exhausted at page {page}")
            break

        products.extend(batch)
        print(f"  {country} page {page}: +{len(batch)} (total {len(products)})")

        if len(batch) < PAGE_SIZE:
            break
        if page < MAX_PAGES:
            time.sleep(REQUEST_INTERVAL)

    return products

# COMMAND ----------

if SOURCE_MODE == "api":
    print(f"fetching up to {len(COUNTRIES) * MAX_PAGES * PAGE_SIZE} products "
          f"from {SEARCH_URL}\n")

    raw = []
    for country in COUNTRIES:
        raw.extend(fetch_country(country))

    if not raw:
        raise RuntimeError(
            "Search-a-licious returned nothing. Check the country tag includes "
            "its language prefix (en:finland, not finland), or switch "
            "source_mode to 'dump'."
        )

    # Land the untouched payload in a Volume, then read it back with Spark.
    #
    # The obvious `spark.read.json(sc.parallelize(...))` doesn't work here:
    # serverless compute blocks direct SparkContext access. Writing to a Volume
    # first is the supported path - and it doubles as the immutable raw layer,
    # so re-parsing never needs a re-fetch.
    stamp = time.strftime("%Y%m%d-%H%M%S")
    raw_path = f"{STAGING_VOLUME}/off_raw_{stamp}.jsonl"

    try:
        with open(raw_path, "w", encoding="utf-8") as fh:
            for record in raw:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except (FileNotFoundError, OSError) as exc:
        raise RuntimeError(
            f"Could not write to {STAGING_VOLUME}. Create the Volume first "
            f"(Catalog > your schema > Create volume), then set the "
            f"staging_volume widget to its path. Original error: {exc}"
        ) from exc

    print(f"\nlanded raw payload -> {raw_path}")

    raw_sdf = spark.read.json(raw_path)
    print(f"fetched {raw_sdf.count()} products")

else:
    # Full dump. Download once into a UC Volume, e.g. from
    # https://huggingface.co/datasets/openfoodfacts/product-database
    raw_sdf = (
        spark.read.parquet(DUMP_PATH)
        .filter(F.array_contains(F.col("countries_tags"), F.lit(COUNTRIES[0])))
    )
    print(f"read {raw_sdf.count()} products from dump")

AVAILABLE = set(raw_sdf.columns)
print("columns:", sorted(AVAILABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform — derive dietary flags
# MAGIC
# MAGIC The interesting work: turning OFF's tag vocabulary and (in dump mode) free
# MAGIC ingredient text into the boolean constraints the planner needs.

# COMMAND ----------

def col_or_null(name: str, dtype: str = "string"):
    """Reference a column if the source has it, else a typed NULL.

    The API and dump modes return overlapping but different fields, and this
    keeps one transform working for both.
    """
    return F.col(name) if name in AVAILABLE else F.lit(None).cast(dtype)


def arr_or_empty(name: str):
    return F.col(name) if name in AVAILABLE else F.array().cast("array<string>")


# nutriments is a nested struct whose field names contain hyphens, so it needs
# backticks and a check on the *struct's* subfields rather than top-level names.
NUTRIMENT_FIELDS = (
    set(raw_sdf.schema["nutriments"].dataType.fieldNames())
    if "nutriments" in AVAILABLE else set()
)


def nutriment(field: str):
    """Reference nutriments.<field> if present, else a NULL double."""
    if field in NUTRIMENT_FIELDS:
        return F.col(f"nutriments.`{field}`").cast("double")
    return F.lit(None).cast("double")


print("nutriment fields available:", sorted(NUTRIMENT_FIELDS)[:12], "...")


def text_blob(*names):
    """Lowercased searchable text built from the given columns.

    The two source modes disagree on shape: Search-a-licious returns `stores`
    and `ingredients_tags` as ARRAY<STRING>, while the Parquet dump has
    `stores` and `ingredients_text` as plain strings. Checking the actual
    schema keeps one transform working for both.
    """
    parts = []
    for name in names:
        if name not in AVAILABLE:
            continue
        if raw_sdf.schema[name].dataType.typeName() == "array":
            parts.append(F.coalesce(F.array_join(F.col(name), " "), F.lit("")))
        else:
            parts.append(F.coalesce(F.col(name).cast("string"), F.lit("")))
    return F.lower(F.concat_ws(" ", *parts)) if parts else F.lit("")


# One searchable blob per product: structured ingredient tags plus any free
# text. Every token scan below runs against this.
ingredients_blob = text_blob("ingredients_tags", "ingredients_text", "ingredients_text_fi")

# COMMAND ----------

# Tags are canonical English (en:pork), but dump-mode free text is often Finnish
# or Indonesian, so we scan for all three.
PORK_TOKENS = ["pork", "sianliha", "porsaan", "bacon", "pekoni", "ham", "kinkku",
               "lard", "babi", "prosciutto", "salami", "chorizo", "pancetta"]
GELATIN_TOKENS = ["gelatin", "gelatine", "liivate", "gelatiini"]
ALCOHOL_TOKENS = ["alcohol", "alkoholi", "wine", "viini", "beer", "olut", "rum",
                  "brandy", "liqueur", "likoori", "mirin", "sake", "arak"]
CARMINE_TOKENS = ["carmine", "cochineal", "e120", "karmiini"]

GLUTEN_TOKENS = ["wheat", "vehna", "barley", "ohra", "rye", "ruis", "gluten",
                 "gluteeni", "malt", "mallas", "spelt", "terigu"]
LACTOSE_TOKENS = ["milk", "maito", "lactose", "laktoosi", "cream", "kerma",
                  "butter", "voi", "cheese", "juusto", "whey", "hera",
                  "yoghurt", "jogurtti", "susu"]
NUT_TOKENS = ["almond", "manteli", "hazelnut", "walnut", "cashew", "kaju",
              "pistachio", "pistaasi", "peanut", "maapahkina", "pahkina", "kacang"]


def has_token(blob, tokens):
    """True if the blob contains any token as a whole word.

    Substring matching is wrong here and dangerously so: plain `contains("rum")`
    flags couscous as alcoholic because it's made from du-RUM wheat, and
    `contains("ham")` flags anything with grafam/graham flour as pork. For a
    halal filter those false positives matter, so we anchor on non-alphanumeric
    boundaries - which also matches the hyphens and colons in tags like
    `en:durum-wheat-semolina`.
    """
    pattern = r"(?:^|[^a-z0-9])(" + "|".join(tokens) + r")(?:[^a-z0-9]|$)"
    return blob.rlike(pattern)


def has_tag(col, *needles):
    """True if any array element contains any needle (tags look like 'en:halal')."""
    expr = F.lit(False)
    for needle in needles:
        expr = expr | F.exists(
            F.coalesce(col, F.array()),
            lambda x: F.lower(x).contains(needle),
        )
    return expr

# COMMAND ----------

labels = arr_or_empty("labels_tags")
allergens = F.array_union(
    F.coalesce(arr_or_empty("allergens_tags"), F.array()),
    F.coalesce(arr_or_empty("traces_tags"), F.array()),
)

# Negative labels are explicit "free from" claims and outrank a token match:
# a product labelled en:no-gluten shouldn't be flagged because "wheat" appears
# in a "may contain" note.
label_no_gluten = has_tag(labels, "no-gluten", "gluten-free")
label_no_lactose = has_tag(labels, "no-lactose", "lactose-free")
label_vegan = has_tag(labels, "vegan")
label_vegetarian = has_tag(labels, "vegetarian") | label_vegan
label_halal = has_tag(labels, "halal")

pork = has_token(ingredients_blob, PORK_TOKENS)
gelatin = has_token(ingredients_blob, GELATIN_TOKENS)
alcohol = has_token(ingredients_blob, ALCOHOL_TOKENS)
carmine = has_token(ingredients_blob, CARMINE_TOKENS)
flagged = pork | gelatin | alcohol | carmine

halal_reason = (
    F.when(label_halal, F.lit("explicit halal label"))
     .when(pork, F.lit("pork-derived ingredient found"))
     .when(gelatin, F.lit("gelatine of unspecified source"))
     .when(alcohol, F.lit("alcohol ingredient found"))
     .when(carmine, F.lit("carmine / E120 found"))
     .when(label_vegan, F.lit("vegan label, nothing flagged"))
     .when(label_vegetarian, F.lit("vegetarian label, nothing flagged"))
     .otherwise(F.lit("no label and no flagged ingredient - unverified"))
)

halal_status = (
    F.when(label_halal, F.lit("certified"))
     .when(flagged, F.lit("contains_flagged"))
     .when(label_vegetarian, F.lit("likely_ok"))
     .otherwise(F.lit("unknown"))
)

# Which Finnish chain stocks it, from OFF's crowd-sourced `stores` field.
# Values are inconsistently cased, e.g. ["S-market", "prisma"].
stores_lower = text_blob("stores", "stores_tags")
store_name = (
    F.when(stores_lower.contains("prisma"), F.lit("Prisma"))
     .when(stores_lower.contains("lidl"), F.lit("Lidl"))
     .when(stores_lower.contains("s-market"), F.lit("S-market"))
     .when(stores_lower.contains("k-market"), F.lit("K-Market"))
     .otherwise(F.lit(None).cast("string"))
)

product_name = F.trim(F.coalesce(
    col_or_null("product_name"),
    col_or_null("product_name_fi"),
    col_or_null("product_name_en"),
))

# COMMAND ----------

curated = (
    raw_sdf
    .filter(F.col("code").isNotNull())
    .withColumn("canonical_name", product_name)
    .filter(F.col("canonical_name").isNotNull() & (F.length("canonical_name") > 1))
    .select(
        F.col("code").cast("string").alias("off_code"),
        F.col("canonical_name"),
        F.trim(col_or_null("product_name_fi")).alias("name_fi"),
        F.element_at(F.coalesce(arr_or_empty("categories_tags"), F.array()), -1).alias("category"),
        store_name.alias("store_name"),

        nutriment("energy-kcal_100g").alias("kcal_per_100g"),
        nutriment("proteins_100g").alias("protein_g_per_100g"),
        nutriment("carbohydrates_100g").alias("carb_g_per_100g"),
        nutriment("fat_100g").alias("fat_g_per_100g"),

        label_vegetarian.alias("is_vegetarian"),
        label_vegan.alias("is_vegan"),
        pork.alias("contains_pork"),
        alcohol.alias("contains_alcohol"),
        # An explicit "free from" label beats a token match.
        (~label_no_gluten & (has_tag(allergens, "gluten")
                             | has_token(ingredients_blob, GLUTEN_TOKENS))).alias("contains_gluten"),
        (~label_no_lactose & (has_tag(allergens, "milk")
                              | has_token(ingredients_blob, LACTOSE_TOKENS))).alias("contains_lactose"),
        (has_tag(allergens, "nuts", "peanut")
         | has_token(ingredients_blob, NUT_TOKENS)).alias("contains_nuts"),

        halal_status.alias("halal_status"),
        halal_reason.alias("halal_reason"),
    )
    .withColumn("is_protein_source", F.col("protein_g_per_100g") >= F.lit(10.0))
    # Spices and oils must not scale linearly when a recipe is tripled.
    .withColumn(
        "scaling_class",
        F.when(
            has_token(F.lower(F.col("canonical_name")),
                      ["salt", "suola", "pepper", "pippuri", "chilli", "chili",
                       "spice", "mauste", "garam", "cumin", "kumina", "oil", "oljy"]),
            F.lit("sublinear"),
        ).otherwise(F.lit("linear")),
    )
    .dropDuplicates(["off_code"])
)

curated.cache()
print(f"curated: {curated.count()} products")
display(curated.groupBy("halal_status").count().orderBy(F.desc("count")))

# COMMAND ----------

# Sanity check before loading - how much of the catalog is actually usable?
display(
    curated.agg(
        F.count("*").alias("products"),
        F.sum(F.col("kcal_per_100g").isNotNull().cast("int")).alias("with_kcal"),
        F.sum(F.col("protein_g_per_100g").isNotNull().cast("int")).alias("with_protein"),
        F.sum(F.col("name_fi").isNotNull().cast("int")).alias("with_finnish_name"),
        F.sum(F.col("store_name").isNotNull().cast("int")).alias("with_store"),
        F.sum(F.col("is_protein_source").cast("int")).alias("protein_sources"),
    )
)

# COMMAND ----------

# MAGIC %md ## 3. Load into Lakebase

# COMMAND ----------

def lakebase_jdbc(url: str) -> tuple[str, dict]:
    """Turn a postgresql:// URL into JDBC url + connection properties."""
    from urllib.parse import urlparse

    p = urlparse(url)
    jdbc = f"jdbc:postgresql://{p.hostname}:{p.port or 5432}{p.path}?sslmode=require"
    return jdbc, {
        "user": p.username,
        "password": p.password,
        "driver": "org.postgresql.Driver",
    }


JDBC_URL, JDBC_PROPS = lakebase_jdbc(LAKEBASE_URL)

# Staging table, then an idempotent upsert. Writing straight into `ingredients`
# would clobber manually corrected rows.
(
    curated.write
    .mode("overwrite")
    .option("truncate", "true")
    .jdbc(JDBC_URL, "stg_off_ingredients", properties=JDBC_PROPS)
)
print("staged to stg_off_ingredients")

# COMMAND ----------

import psycopg2

UPSERT_SQL = """
INSERT INTO ingredients (
    canonical_name, name_fi, category, off_code,
    kcal_per_100g, protein_g_per_100g, carb_g_per_100g, fat_g_per_100g,
    is_vegetarian, is_vegan, contains_pork, contains_alcohol,
    contains_gluten, contains_lactose, contains_nuts,
    halal_status, halal_reason, is_protein_source, scaling_class,
    source, updated_at
)
SELECT DISTINCT ON (lower(canonical_name))
       canonical_name, name_fi, category, off_code,
       kcal_per_100g, protein_g_per_100g, carb_g_per_100g, fat_g_per_100g,
       is_vegetarian, is_vegan, contains_pork, contains_alcohol,
       contains_gluten, contains_lactose, contains_nuts,
       halal_status, halal_reason, is_protein_source, scaling_class,
       'openfoodfacts', now()
FROM stg_off_ingredients
WHERE canonical_name IS NOT NULL AND length(trim(canonical_name)) > 1
ORDER BY lower(canonical_name), kcal_per_100g NULLS LAST
ON CONFLICT (canonical_name) DO UPDATE SET
    name_fi            = COALESCE(EXCLUDED.name_fi, ingredients.name_fi),
    category           = COALESCE(EXCLUDED.category, ingredients.category),
    off_code           = COALESCE(EXCLUDED.off_code, ingredients.off_code),
    kcal_per_100g      = COALESCE(EXCLUDED.kcal_per_100g, ingredients.kcal_per_100g),
    protein_g_per_100g = COALESCE(EXCLUDED.protein_g_per_100g, ingredients.protein_g_per_100g),
    carb_g_per_100g    = COALESCE(EXCLUDED.carb_g_per_100g, ingredients.carb_g_per_100g),
    fat_g_per_100g     = COALESCE(EXCLUDED.fat_g_per_100g, ingredients.fat_g_per_100g),
    halal_status       = EXCLUDED.halal_status,
    halal_reason       = EXCLUDED.halal_reason,
    updated_at         = now();
"""

with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute(UPSERT_SQL)
        upserted = cur.rowcount
        cur.execute("SELECT COUNT(*) FROM ingredients")
        total = cur.fetchone()[0]
        cur.execute("""
            SELECT halal_status, COUNT(*)
            FROM ingredients GROUP BY halal_status ORDER BY 2 DESC
        """)
        breakdown = cur.fetchall()
    conn.commit()

print(f"upserted {upserted} rows -> ingredients now has {total}\n")
for status, n in breakdown:
    print(f"  {status:20s} {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC
# MAGIC Open the app's **Catalog** tab to see the ingredients, and **Pipeline** for
# MAGIC the halal-coverage breakdown.
# MAGIC
# MAGIC Next: `extract_receipts.py` to attach real prices from your own receipts.
