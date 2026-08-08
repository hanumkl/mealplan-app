# Databricks notebook source
# MAGIC %md
# MAGIC # Open Food Facts -> Lakebase ingredient catalog
# MAGIC
# MAGIC Spark pipeline that lands Finnish grocery products and derives the dietary
# MAGIC flags the meal planner constrains against.
# MAGIC
# MAGIC **Two source modes** (`source_mode` widget):
# MAGIC
# MAGIC | mode | what it does | when to use |
# MAGIC |---|---|---|
# MAGIC | `api` | pages the free OFF search API filtered to Finland | first run, no setup |
# MAGIC | `dump` | reads the full OFF Parquet dump from a UC Volume | real Spark scale |
# MAGIC
# MAGIC Start with `api`. Switch to `dump` once you've downloaded the dump — it's the
# MAGIC same downstream code, just millions of rows instead of thousands.
# MAGIC
# MAGIC **Halal flagging is deliberately four-valued**, never a bare boolean:
# MAGIC `certified` (explicit label) / `contains_flagged` (pork, gelatine, alcohol,
# MAGIC carmine found) / `likely_ok` (vegetarian or vegan label, nothing flagged) /
# MAGIC `unknown`. The app shows the reason and tells the user to check packaging.
# MAGIC
# MAGIC Requires: `LAKEBASE_URL` secret in scope `database`, and the SQL in `sql/` run.

# COMMAND ----------

# MAGIC %pip install requests
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.dropdown("source_mode", "api", ["api", "dump"])
dbutils.widgets.text("countries", "finland", "OFF country tag(s), comma separated")
dbutils.widgets.text("max_pages", "25", "api mode: pages to fetch (100 products each)")
dbutils.widgets.text("dump_path", "/Volumes/main/mealplan/off/food.parquet", "dump mode: Volume path")
dbutils.widgets.text("lakebase_scope", "database", "Secret scope")
dbutils.widgets.text("lakebase_key", "lakebase-url", "Secret key")

SOURCE_MODE = dbutils.widgets.get("source_mode")
COUNTRIES = [c.strip() for c in dbutils.widgets.get("countries").split(",") if c.strip()]
MAX_PAGES = int(dbutils.widgets.get("max_pages"))
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
from pyspark.sql import types as T

USER_AGENT = "mealplan-capstone/1.0 (databricks bootcamp project)"

# Fields we need. Asking for a subset keeps the API responses small.
OFF_FIELDS = [
    "code", "product_name", "product_name_fi", "brands", "categories_tags",
    "countries_tags", "stores", "stores_tags", "quantity",
    "ingredients_text", "ingredients_text_fi", "allergens_tags", "traces_tags",
    "labels_tags", "nutriments", "nova_group", "image_small_url",
]

# COMMAND ----------

# MAGIC %md ## 1. Extract

# COMMAND ----------

def fetch_off_api(country: str, max_pages: int) -> list[dict]:
    """Page the Open Food Facts search API for one country tag.

    Free, no API key. We sleep between pages - OFF asks clients to be gentle and
    a capstone job has no reason to hammer a volunteer-run service.
    """
    products, url = [], "https://world.openfoodfacts.org/api/v2/search"
    for page in range(1, max_pages + 1):
        resp = requests.get(
            url,
            params={
                "countries_tags": country,
                "fields": ",".join(OFF_FIELDS),
                "page_size": 100,
                "page": page,
            },
            headers={"User-Agent": USER_AGENT},
            timeout=60,
        )
        resp.raise_for_status()
        batch = resp.json().get("products", [])
        if not batch:
            break
        products.extend(batch)
        print(f"  {country} page {page}: +{len(batch)} (total {len(products)})")
        time.sleep(1.0)
    return products


if SOURCE_MODE == "api":
    raw = []
    for country in COUNTRIES:
        raw.extend(fetch_off_api(country, MAX_PAGES))

    # json round-trip so Spark infers a stable schema across ragged records
    raw_sdf = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in raw]))
    print(f"fetched {raw_sdf.count()} products via API")

else:
    # Full dump. Download once into a UC Volume, e.g.:
    #   https://huggingface.co/datasets/openfoodfacts/product-database
    #   (or the JSONL export at https://static.openfoodfacts.org/data/)
    raw_sdf = (
        spark.read.parquet(DUMP_PATH)
        .filter(F.array_contains(F.col("countries_tags"), F.lit(COUNTRIES[0])))
    )
    print(f"read {raw_sdf.count()} products from dump")

raw_sdf.createOrReplaceTempView("off_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform — derive dietary flags
# MAGIC
# MAGIC The interesting work: turning free-text ingredient lists and OFF's tag
# MAGIC vocabulary into the boolean constraints the planner needs.

# COMMAND ----------

# Substrings scanned in ingredients_text. Finnish, English and Indonesian, since
# the recipe side of the project is trilingual.
PORK_TOKENS = ["pork", "sianliha", "porsaan", "bacon", "pekoni", "ham", "kinkku",
               "lard", "babi", "prosciutto", "salami", "chorizo", "pancetta"]
GELATIN_TOKENS = ["gelatin", "gelatine", "liivate", "gelatiini"]
ALCOHOL_TOKENS = ["alcohol", "alkoholi", "wine", "viini", "beer", "olut", "rum",
                  "brandy", "liqueur", "likööri", "mirin", "sake", "arak"]
CARMINE_TOKENS = ["carmine", "cochineal", "e120", "karmiini"]

GLUTEN_TOKENS = ["wheat", "vehnä", "barley", "ohra", "rye", "ruis", "gluten",
                 "gluteeni", "malt", "mallas", "spelt", "terigu"]
LACTOSE_TOKENS = ["milk", "maito", "lactose", "laktoosi", "cream", "kerma",
                  "butter", "voi", "cheese", "juusto", "whey", "hera", "yoghurt",
                  "jogurtti", "susu"]
NUT_TOKENS = ["almond", "manteli", "hazelnut", "hasselpähkinä", "walnut",
              "saksanpähkinä", "cashew", "kaju", "pistachio", "pistaasi",
              "peanut", "maapähkinä", "pähkinä", "kacang"]


def any_token(col, tokens):
    """True if the lowercased column contains any of the tokens."""
    lowered = F.lower(F.coalesce(col, F.lit("")))
    expr = F.lit(False)
    for tok in tokens:
        expr = expr | lowered.contains(tok)
    return expr


def has_tag(col, *needles):
    """True if any array element contains any needle (OFF tags are like 'en:halal')."""
    expr = F.lit(False)
    for needle in needles:
        expr = expr | F.exists(
            F.coalesce(col, F.array()),
            lambda x: F.lower(x).contains(needle),
        )
    return expr

# COMMAND ----------

ingredients_col = F.coalesce(F.col("ingredients_text_fi"), F.col("ingredients_text"))
labels = F.col("labels_tags")
allergens = F.array_union(
    F.coalesce(F.col("allergens_tags"), F.array()),
    F.coalesce(F.col("traces_tags"), F.array()),
)

flagged = (
    any_token(ingredients_col, PORK_TOKENS)
    | any_token(ingredients_col, GELATIN_TOKENS)
    | any_token(ingredients_col, ALCOHOL_TOKENS)
    | any_token(ingredients_col, CARMINE_TOKENS)
)

# Why a product got its halal status - surfaced verbatim in the UI.
halal_reason = (
    F.when(has_tag(labels, "halal"), F.lit("explicit halal label"))
     .when(any_token(ingredients_col, PORK_TOKENS), F.lit("pork-derived ingredient found"))
     .when(any_token(ingredients_col, GELATIN_TOKENS),
           F.lit("gelatine of unspecified source"))
     .when(any_token(ingredients_col, ALCOHOL_TOKENS), F.lit("alcohol ingredient found"))
     .when(any_token(ingredients_col, CARMINE_TOKENS), F.lit("carmine / E120 found"))
     .when(has_tag(labels, "vegan"), F.lit("vegan label, nothing flagged"))
     .when(has_tag(labels, "vegetarian"), F.lit("vegetarian label, nothing flagged"))
     .otherwise(F.lit("no label and no flagged ingredient - unverified"))
)

halal_status = (
    F.when(has_tag(labels, "halal"), F.lit("certified"))
     .when(flagged, F.lit("contains_flagged"))
     .when(has_tag(labels, "vegan") | has_tag(labels, "vegetarian"), F.lit("likely_ok"))
     .otherwise(F.lit("unknown"))
)

# Which Finnish chain stocks it, from OFF's crowd-sourced `stores` field.
store_name = (
    F.when(F.lower(F.coalesce(F.col("stores"), F.lit(""))).contains("prisma"), F.lit("Prisma"))
     .when(F.lower(F.coalesce(F.col("stores"), F.lit(""))).contains("lidl"), F.lit("Lidl"))
     .when(F.lower(F.coalesce(F.col("stores"), F.lit(""))).contains("s-market"), F.lit("S-market"))
     .when(F.lower(F.coalesce(F.col("stores"), F.lit(""))).contains("k-market"), F.lit("K-Market"))
     .otherwise(F.lit(None).cast("string"))
)

curated = (
    raw_sdf
    .filter(F.col("code").isNotNull())
    .filter(F.coalesce(F.col("product_name"), F.col("product_name_fi")).isNotNull())
    .select(
        F.col("code").cast("string").alias("off_code"),
        F.trim(F.coalesce(F.col("product_name"), F.col("product_name_fi")))
            .alias("canonical_name"),
        F.trim(F.col("product_name_fi")).alias("name_fi"),
        F.element_at(F.coalesce(F.col("categories_tags"), F.array()), -1).alias("category"),
        store_name.alias("store_name"),

        F.col("nutriments.energy-kcal_100g").cast("double").alias("kcal_per_100g"),
        F.col("nutriments.proteins_100g").cast("double").alias("protein_g_per_100g"),
        F.col("nutriments.carbohydrates_100g").cast("double").alias("carb_g_per_100g"),
        F.col("nutriments.fat_100g").cast("double").alias("fat_g_per_100g"),

        (has_tag(labels, "vegetarian") | has_tag(labels, "vegan")).alias("is_vegetarian"),
        has_tag(labels, "vegan").alias("is_vegan"),
        any_token(ingredients_col, PORK_TOKENS).alias("contains_pork"),
        any_token(ingredients_col, ALCOHOL_TOKENS).alias("contains_alcohol"),
        (has_tag(allergens, "gluten") | any_token(ingredients_col, GLUTEN_TOKENS))
            .alias("contains_gluten"),
        (has_tag(allergens, "milk") | any_token(ingredients_col, LACTOSE_TOKENS))
            .alias("contains_lactose"),
        (has_tag(allergens, "nuts", "peanut") | any_token(ingredients_col, NUT_TOKENS))
            .alias("contains_nuts"),

        halal_status.alias("halal_status"),
        halal_reason.alias("halal_reason"),
        ingredients_col.alias("ingredients_text"),
    )
    # A protein source if it actually carries meaningful protein.
    .withColumn("is_protein_source", F.col("protein_g_per_100g") >= F.lit(10.0))
    # Spices and oils must not scale linearly when a recipe is tripled.
    .withColumn(
        "scaling_class",
        F.when(
            any_token(F.col("canonical_name"),
                      ["salt", "suola", "pepper", "pippuri", "chilli", "chili",
                       "spice", "mauste", "garam", "cumin", "kumina", "oil", "öljy"]),
            F.lit("sublinear"),
        ).otherwise(F.lit("linear")),
    )
    .dropDuplicates(["off_code"])
)

print(f"curated: {curated.count()} products")
display(curated.groupBy("halal_status").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md ## 3. Load into Lakebase

# COMMAND ----------

JDBC_URL, JDBC_PROPS = None, None


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
# would clobber the review state and any manually corrected rows.
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
        cur.execute(
            """
            SELECT halal_status, COUNT(*)
            FROM ingredients GROUP BY halal_status ORDER BY 2 DESC
            """
        )
        breakdown = cur.fetchall()
    conn.commit()

print(f"upserted {upserted} rows -> ingredients now has {total}")
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
