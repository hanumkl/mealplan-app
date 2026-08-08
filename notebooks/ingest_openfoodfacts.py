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
dbutils.widgets.text("staging_volume", "/Volumes/workspace/default/raw", "api mode: Volume for raw JSON")
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
    """True if any array element contains any needle (tags look like 'en:halal').

    Substring matching, for vocabularies where the same concept appears with
    different prefixes and spellings (`en:no-gluten` vs `en:gluten-free`).
    """
    expr = F.lit(False)
    for needle in needles:
        expr = expr | F.exists(
            F.coalesce(col, F.array()),
            lambda x: F.lower(x).contains(needle),
        )
    return expr


def has_exact_tag(col, *values):
    """True if the array contains any of these tags exactly.

    Required for the vegan/vegetarian vocabularies, where substring matching
    is actively wrong: `contains("vegan")` also matches `en:non-vegan` and
    `en:vegan-status-unknown`, i.e. it would read "definitely not vegan" as
    "vegan".
    """
    expr = F.lit(False)
    for value in values:
        expr = expr | F.array_contains(F.coalesce(col, F.array()), F.lit(value))
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
label_halal = has_tag(labels, "halal")
label_vegan = has_exact_tag(labels, "en:vegan")
label_vegetarian = has_exact_tag(labels, "en:vegetarian") | label_vegan

# OFF computes its own vegan/vegetarian verdict from the parsed ingredient
# list, independent of whether the producer applied a label. Ignoring this
# was the single biggest cause of everything reading "unknown".
analysis = arr_or_empty("ingredients_analysis_tags")
analysis_vegan = has_exact_tag(analysis, "en:vegan")
analysis_vegetarian = has_exact_tag(analysis, "en:vegetarian")

is_vegan_flag = label_vegan | analysis_vegan
is_vegetarian_flag = label_vegetarian | analysis_vegetarian | is_vegan_flag

pork = has_token(ingredients_blob, PORK_TOKENS)
gelatin = has_token(ingredients_blob, GELATIN_TOKENS)
alcohol = has_token(ingredients_blob, ALCOHOL_TOKENS)
carmine = has_token(ingredients_blob, CARMINE_TOKENS)
flagged = pork | gelatin | alcohol | carmine

# Note on what is deliberately NOT flagged: `en:non-vegan` and
# `en:non-vegetarian` say nothing about halal status. Milk, eggs and properly
# slaughtered beef are all non-vegan and all halal. Treating them as flagged
# would reject most of the meat and dairy this household actually eats.

# ---------------------------------------------------------------------------
# Plain plant staples
#
# 62% of Finnish OFF records carry no labels, no analysis, no ingredients and
# no category - just a name. That leaves rice, pasta and flour sitting at
# "unknown", which is useless for planning. For single-ingredient plant foods
# there is genuinely nothing to be uncertain about, so we infer from the name.
#
# This is guarded hard, because a wrong "likely_ok" on a halal filter is much
# worse than a wrong "unknown": the name must contain a staple word AND must
# not contain any animal, seafood, or prepared-dish word. That keeps
# "Basmatiriisi" while rejecting "Riisi ja kana" and "Kebab riisillä".
# ---------------------------------------------------------------------------
STAPLE_TOKENS = [
    "riisi", "rice", "basmati", "jasmiini", "pasta", "makaroni", "spagetti",
    "spaghetti", "nuudeli", "noodle", "couscous", "bulgur", "quinoa",
    "penne", "fusilli", "tagliatelle", "lasagne", "farfalle", "risotto",
    "jauho", "flour", "kaura", "oat", "hirssi", "tattari", "mannasuurimo",
    "linssi", "lentil", "papu", "bean", "herne", "pea", "kikherne", "chickpea",
    "peruna", "potato", "porkkana", "carrot", "tomaatti", "tomato",
    "sipuli", "onion", "valkosipuli", "garlic", "kurkku", "cucumber",
    "paprika", "parsakaali", "broccoli", "pinaatti", "spinach", "kaali",
    "omena", "apple", "banaani", "banana", "appelsiini", "orange",
    "sokeri", "sugar", "suola", "salt",
]

# Words that may appear alongside a staple without changing what it is:
# brand-ish qualifiers, sizes, and quality adjectives.
SAFE_MODIFIERS = [
    "xtra", "extra", "luomu", "organic", "premium", "classic", "original",
    "iso", "isot", "pieni", "suuri", "taysjyva", "valkoinen", "tumma",
    "gluteeniton", "laktoositon", "maustamaton", "kuivattu", "pikA",
    "kotimainen", "puhdas", "natural", "naturell", "fin", "hieno", "karkea",
    "ja", "and", "of", "the", "with",
]

# Still used as a hard veto, with plain substring matching (not word-boundary):
# Finnish compounds words without spaces, so "pastakastike" hides "kastike"
# and "shrimps" hides "shrimp". Over-rejecting here is free - the product just
# stays "unknown", which is where it already was.
NON_STAPLE_TOKENS = [
    "kana", "chicken", "liha", "meat", "nauta", "beef", "possu", "sika",
    "kala", "fish", "lohi", "salmon", "tonnikala", "tuna", "katkarapu",
    "shrimp", "ayriai", "kebab", "makkara", "sausage", "nakki", "pekoni",
    "kinkku", "broileri", "kalkkuna", "turkey", "muna", "egg",
    "juusto", "cheese", "kerma", "cream", "maito", "milk", "jogurtti",
    "kastike", "sauce", "keitto", "soup", "ateria", "carbonara", "bolognese",
    "valmis", "pizza", "burger", "wok", "curry", "salaatti", "salad",
    "puuro", "porridge", "leipa", "bread", "piirakka", "pulla", "kakku",
]


def has_substring(blob, tokens):
    """Plain substring match - deliberately aggressive, for the veto list."""
    expr = F.lit(False)
    for tok in tokens:
        expr = expr | blob.contains(tok)
    return expr


name_lower = F.lower(F.col("canonical_name"))

# Whitelist rather than blocklist. A blocklist can't enumerate every dish name
# on earth - "Pasta carbonara" passed one until it was spotted. Instead every
# word must be recognisable: a staple, a harmless modifier, or a size like
# "500g". One unknown word ("carbonara", "kebab", "shrimps") blocks inference.
name_words = F.filter(
    F.split(F.regexp_replace(name_lower, r"[^\w\s]", " "), r"\s+"),
    lambda w: F.length(w) > 0,
)


def _word_is_staple(w):
    expr = F.lit(False)
    for tok in STAPLE_TOKENS:
        expr = expr | w.contains(tok)
    return expr


def _word_is_modifier(w):
    expr = w.rlike(r"^\d+[a-z]{0,2}$")           # 500g, 1kg, 5
    for tok in SAFE_MODIFIERS:
        expr = expr | (w == F.lit(tok))
    return expr


# `flagged` above is derived from the ingredient list, which these bare records
# don't have - so the name has to be screened separately. Without this,
# "Chorizopasta" reads as a plain pasta staple: one word, contains "pasta",
# no ingredient data to contradict it.
name_has_haram = has_substring(
    name_lower, PORK_TOKENS + GELATIN_TOKENS + ALCOHOL_TOKENS + CARMINE_TOKENS
)

plant_staple = (
    F.exists(name_words, _word_is_staple)
    & F.forall(name_words, lambda w: _word_is_staple(w) | _word_is_modifier(w))
    & ~has_substring(name_lower, NON_STAPLE_TOKENS)
    & ~name_has_haram
    & ~flagged
)

halal_reason = (
    F.when(label_halal, F.lit("explicit halal label"))
     .when(pork, F.lit("pork-derived ingredient found"))
     .when(gelatin, F.lit("gelatine of unspecified source"))
     .when(alcohol, F.lit("alcohol ingredient found"))
     .when(carmine, F.lit("carmine / E120 found"))
     .when(label_vegan, F.lit("vegan label, nothing flagged"))
     .when(label_vegetarian, F.lit("vegetarian label, nothing flagged"))
     .when(analysis_vegan, F.lit("ingredients analysed as vegan"))
     .when(analysis_vegetarian, F.lit("ingredients analysed as vegetarian"))
     .when(plant_staple, F.lit("plain plant staple inferred from product name"))
     .otherwise(F.lit("no label, ingredients or category in Open Food Facts"))
)

halal_status = (
    F.when(label_halal, F.lit("certified"))
     .when(flagged, F.lit("contains_flagged"))
     .when(is_vegetarian_flag, F.lit("likely_ok"))
     .when(plant_staple, F.lit("likely_ok"))
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

# OFF categories run general -> specific, so the last element is the most
# precise one ("en:plant-based-foods" ... "en:basmati-rice").
#
# The size() guard is load-bearing: serverless runs with ANSI mode on, where
# element_at() on an empty array raises INVALID_ARRAY_INDEX instead of
# returning NULL. Plenty of products have no categories at all.
categories = F.coalesce(arr_or_empty("categories_tags"), F.array())
category = F.when(F.size(categories) > 0, F.element_at(categories, -1))

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
        F.trim(col_or_null("product_name_en")).alias("name_en"),
        category.alias("category"),
        # OFF category tags are always English regardless of the product's
        # language, so "Grillattu broileri" still yields "roast chicken".
        # For a Finnish catalogue you can't read, that's often the only clue
        # to what the thing actually is.
        F.when(
            category.isNotNull(),
            F.regexp_replace(F.regexp_replace(category, r"^[a-z]{2}:", ""), "-", " "),
        ).alias("category_en"),
        store_name.alias("store_name"),

        nutriment("energy-kcal_100g").alias("kcal_per_100g"),
        nutriment("proteins_100g").alias("protein_g_per_100g"),
        nutriment("carbohydrates_100g").alias("carb_g_per_100g"),
        nutriment("fat_100g").alias("fat_g_per_100g"),

        is_vegetarian_flag.alias("is_vegetarian"),
        is_vegan_flag.alias("is_vegan"),
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
    # coalesce matters: NULL >= 10.0 is NULL in SQL, not false, and roughly
    # 15% of OFF products have no protein figure. The column is NOT NULL, and
    # a DEFAULT only applies when a column is omitted - not when NULL is
    # passed explicitly. Unknown protein content means "not a protein source".
    .withColumn(
        "is_protein_source",
        F.coalesce(F.col("protein_g_per_100g") >= F.lit(10.0), F.lit(False)),
    )
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

# MAGIC %md
# MAGIC ## 3. Load into Lakebase
# MAGIC
# MAGIC Serverless compute only allows DML writes through a fixed connector
# MAGIC allowlist (csv/json/parquet/delta/mysql/snowflake/redshift/...) - plain
# MAGIC `postgresql` via generic JDBC isn't on it, so `curated.write.jdbc(...)`
# MAGIC fails with `UNSUPPORTED_DATA_SOURCE_WRITE` no matter what table it targets.
# MAGIC
# MAGIC At ~10k rows there's no reason to fight that restriction: dedupe in Spark
# MAGIC (still the real transformation work), collect to the driver, and upsert
# MAGIC with `psycopg2` - a plain Postgres client, not a Spark data source, so the
# MAGIC allowlist doesn't apply to it.

# COMMAND ----------

from pyspark.sql.window import Window

# Two OFF products can share a display name (e.g. two "Milk" entries with
# different barcodes). Keep one row per case-insensitive name, preferring
# whichever has calorie data - same intent as the old
# `DISTINCT ON (lower(canonical_name)) ORDER BY ... kcal_per_100g NULLS LAST`.
dedupe_window = (
    Window.partitionBy(F.lower(F.col("canonical_name")))
    .orderBy(F.col("kcal_per_100g").isNull(), F.col("off_code"))
)

deduped = (
    curated
    .withColumn("_rank", F.row_number().over(dedupe_window))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

rows = [r.asDict() for r in deduped.collect()]
print(f"{len(rows)} deduplicated rows ready to upsert")

# Pre-flight: fail loudly here rather than part-way through the batch with a
# raw NotNullViolation. These are the NOT NULL columns in `ingredients` that
# this notebook supplies explicitly (the ones it omits fall back to their
# DEFAULT, which is why they're not listed).
REQUIRED_NOT_NULL = [
    "canonical_name", "halal_status", "is_protein_source", "scaling_class",
]

problems = {
    col: sum(1 for r in rows if r.get(col) is None)
    for col in REQUIRED_NOT_NULL
}
problems = {c: n for c, n in problems.items() if n}

if problems:
    example = next(r for r in rows if r.get(next(iter(problems))) is None)
    raise ValueError(
        f"NULLs in NOT NULL columns: {problems}\n"
        f"example row: {example}"
    )
print(f"pre-flight OK - no NULLs in {', '.join(REQUIRED_NOT_NULL)}")

# COMMAND ----------

import psycopg2
from psycopg2.extras import execute_values

UPSERT_SQL = """
INSERT INTO ingredients (
    canonical_name, name_fi, name_en, category, category_en, off_code,
    kcal_per_100g, protein_g_per_100g, carb_g_per_100g, fat_g_per_100g,
    is_vegetarian, is_vegan, contains_pork, contains_alcohol,
    contains_gluten, contains_lactose, contains_nuts,
    halal_status, halal_reason, is_protein_source, scaling_class,
    source, updated_at
) VALUES %s
ON CONFLICT (canonical_name) DO UPDATE SET
    name_fi            = COALESCE(EXCLUDED.name_fi, ingredients.name_fi),
    name_en            = COALESCE(EXCLUDED.name_en, ingredients.name_en),
    category           = COALESCE(EXCLUDED.category, ingredients.category),
    category_en        = COALESCE(EXCLUDED.category_en, ingredients.category_en),
    off_code           = COALESCE(EXCLUDED.off_code, ingredients.off_code),
    kcal_per_100g      = COALESCE(EXCLUDED.kcal_per_100g, ingredients.kcal_per_100g),
    protein_g_per_100g = COALESCE(EXCLUDED.protein_g_per_100g, ingredients.protein_g_per_100g),
    carb_g_per_100g    = COALESCE(EXCLUDED.carb_g_per_100g, ingredients.carb_g_per_100g),
    fat_g_per_100g     = COALESCE(EXCLUDED.fat_g_per_100g, ingredients.fat_g_per_100g),
    -- A hand-confirmed halal status outranks anything derived here. Without
    -- this guard every pipeline run would silently wipe the household's own
    -- decisions and send those products back to 'unknown'.
    halal_status       = CASE WHEN ingredients.halal_source = 'user_confirmed'
                              THEN ingredients.halal_status
                              ELSE EXCLUDED.halal_status END,
    halal_reason       = CASE WHEN ingredients.halal_source = 'user_confirmed'
                              THEN ingredients.halal_reason
                              ELSE EXCLUDED.halal_reason END,
    updated_at         = now();
"""

# Named placeholders so we can hand execute_values the row dicts directly -
# no need to reorder each row into a tuple by hand.
VALUE_TEMPLATE = """(
    %(canonical_name)s, %(name_fi)s, %(name_en)s, %(category)s, %(category_en)s, %(off_code)s,
    %(kcal_per_100g)s, %(protein_g_per_100g)s, %(carb_g_per_100g)s, %(fat_g_per_100g)s,
    %(is_vegetarian)s, %(is_vegan)s, %(contains_pork)s, %(contains_alcohol)s,
    %(contains_gluten)s, %(contains_lactose)s, %(contains_nuts)s,
    %(halal_status)s, %(halal_reason)s, %(is_protein_source)s, %(scaling_class)s,
    'openfoodfacts', now()
)"""

with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ingredients")
        before = cur.fetchone()[0]

        # execute_values batches into multiple statements at this row count,
        # so cur.rowcount after the call would only reflect the last batch -
        # a before/after count is what's actually accurate here.
        execute_values(cur, UPSERT_SQL, rows, template=VALUE_TEMPLATE, page_size=500)

        cur.execute("SELECT COUNT(*) FROM ingredients")
        after = cur.fetchone()[0]
        cur.execute("""
            SELECT halal_status, COUNT(*)
            FROM ingredients GROUP BY halal_status ORDER BY 2 DESC
        """)
        breakdown = cur.fetchall()
    conn.commit()

print(f"submitted {len(rows)} rows -> ingredients went from {before} to {after} "
      f"({after - before} new, {len(rows) - (after - before)} updated existing)\n")
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
