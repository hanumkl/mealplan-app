# Databricks notebook source
# MAGIC %md
# MAGIC # Receipt photos -> real Finnish grocery prices
# MAGIC
# MAGIC Reads receipt images from a Unity Catalog Volume, extracts line items with a
# MAGIC vision model, matches them to the ingredient catalog, and writes prices into
# MAGIC Lakebase.
# MAGIC
# MAGIC **Why this exists.** Prisma and K-ruoka have no usable public price API
# MAGIC (both sit behind bot challenges, and Kesko's developer portal is Azure-AD
# MAGIC partner-only). Your own receipts are better data anyway: real prices, for the
# MAGIC products your family actually buys, with no terms-of-service question.
# MAGIC
# MAGIC It also satisfies the capstone's unstructured-data requirement with **images**
# MAGIC rather than text, which is a stronger answer than embedding tidy API strings.
# MAGIC
# MAGIC ### Before running
# MAGIC 1. Create a Volume, e.g. `main.mealplan.receipts`
# MAGIC 2. Upload 10-15 receipt photos (JPG/PNG/HEIC-converted) into it
# MAGIC 3. Set `vision_endpoint` below to a vision-capable serving endpoint in your
# MAGIC    workspace (**Serving** in the sidebar lists what you have)

# COMMAND ----------

# MAGIC %pip install openai pillow
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("volume_path", "/Volumes/main/mealplan/receipts", "Receipt image volume")
dbutils.widgets.text("vision_endpoint", "databricks-claude-sonnet-4-5", "Vision serving endpoint")
dbutils.widgets.text("default_store", "Prisma", "Store when the receipt doesn't say")
dbutils.widgets.text("lakebase_scope", "database", "Secret scope")
dbutils.widgets.text("lakebase_key", "lakebase-url", "Secret key")
dbutils.widgets.dropdown("reprocess", "false", ["true", "false"], "Re-extract already-done receipts")

VOLUME_PATH = dbutils.widgets.get("volume_path")
VISION_ENDPOINT = dbutils.widgets.get("vision_endpoint")
DEFAULT_STORE = dbutils.widgets.get("default_store")
REPROCESS = dbutils.widgets.get("reprocess") == "true"

LAKEBASE_URL = dbutils.secrets.get(
    scope=dbutils.widgets.get("lakebase_scope"),
    key=dbutils.widgets.get("lakebase_key"),
)

# COMMAND ----------

import base64
import json
import re

import psycopg2
from psycopg2.extras import RealDictCursor
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md ## 1. Find receipt images

# COMMAND ----------

images_sdf = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.{jpg,jpeg,png,JPG,JPEG,PNG}")
    .load(VOLUME_PATH)
    .select("path", "length", "modificationTime", "content")
)

print(f"found {images_sdf.count()} receipt images in {VOLUME_PATH}")
display(images_sdf.select("path", "length", "modificationTime"))

# COMMAND ----------

# Skip receipts already extracted, unless explicitly reprocessing.
with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT image_path FROM raw_receipts WHERE extraction_status = 'extracted'")
        done = {r[0] for r in cur.fetchall()}

pending = images_sdf.collect() if REPROCESS else [
    r for r in images_sdf.collect() if r["path"] not in done
]
print(f"{len(pending)} receipts to process ({len(done)} already extracted)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Vision extraction
# MAGIC
# MAGIC Finnish receipts abbreviate aggressively — `BROIL.FILE 700G`, `MAITO KEVYT 1L`.
# MAGIC We keep the raw printed text verbatim in `raw_text` and let the model also
# MAGIC give a normalised guess, so a bad guess never destroys the original.

# COMMAND ----------

EXTRACTION_PROMPT = """You are reading a Finnish grocery receipt.

Extract every purchased line item. Return STRICT JSON, no markdown fence, shaped:

{
  "store": "Prisma" | "S-market" | "Lidl" | "K-Market" | "Alanya" | null,
  "purchased_on": "YYYY-MM-DD" or null,
  "total_eur": number or null,
  "confidence": 0.0-1.0,
  "items": [
    {
      "raw_text": "exactly as printed on the receipt",
      "normalised_name": "plain English ingredient name, e.g. chicken breast",
      "quantity": number or null,
      "unit": "g" | "kg" | "ml" | "l" | "piece" | null,
      "price_eur": number,
      "confidence": 0.0-1.0
    }
  ]
}

Rules:
- Keep raw_text EXACTLY as printed, including Finnish abbreviations.
- price_eur is what was actually paid for that line, after any discount shown.
- Skip deposit lines (PANTTI), bag purchases, loyalty discounts and the total row.
- If a pack size appears in the name (e.g. "700G"), put it in quantity/unit.
- Set a low confidence on any line you had to guess at. Do not invent items.
"""


def extract_receipt(image_bytes: bytes) -> dict:
    """Send one receipt image to the vision endpoint and parse the JSON reply."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient().serving_endpoints.get_open_ai_client()
    b64 = base64.b64encode(image_bytes).decode("utf-8")

    response = client.chat.completions.create(
        model=VISION_ENDPOINT,
        max_tokens=4096,
        messages=[{
            "role": "user",
            "content": [
                {"type": "text", "text": EXTRACTION_PROMPT},
                {"type": "image_url",
                 "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
            ],
        }],
    )
    text = response.choices[0].message.content.strip()
    # Models sometimes wrap JSON in a fence despite instructions.
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    return json.loads(text)

# COMMAND ----------

results = []
for row in pending:
    path = row["path"]
    try:
        parsed = extract_receipt(row["content"])
        parsed["_path"] = path
        parsed["_status"] = "extracted"
        n = len(parsed.get("items", []))
        print(f"OK   {path.split('/')[-1]:40s} {n:3d} items  conf={parsed.get('confidence')}")
    except Exception as exc:
        parsed = {"_path": path, "_status": "failed", "_error": str(exc), "items": []}
        print(f"FAIL {path.split('/')[-1]:40s} {exc}")
    results.append(parsed)

print(f"\n{sum(1 for r in results if r['_status'] == 'extracted')}/{len(results)} succeeded")

# COMMAND ----------

# MAGIC %md ## 3. Write receipts and line items to Lakebase

# COMMAND ----------

INSERT_RECEIPT = """
INSERT INTO raw_receipts
    (image_path, store_hint, purchased_on, total_eur, payload,
     extraction_status, extraction_confidence)
VALUES (%s, %s, %s, %s, %s, %s, %s)
RETURNING receipt_id
"""

INSERT_LINE = """
INSERT INTO receipt_line_items
    (receipt_id, raw_text, quantity, unit, price_eur, confidence)
VALUES (%s, %s, %s, %s, %s, %s)
"""

receipt_ids = []
with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        for r in results:
            cur.execute(INSERT_RECEIPT, (
                r["_path"],
                r.get("store") or DEFAULT_STORE,
                r.get("purchased_on"),
                r.get("total_eur"),
                json.dumps(r),
                r["_status"],
                r.get("confidence"),
            ))
            receipt_id = cur.fetchone()[0]
            receipt_ids.append(receipt_id)

            for item in r.get("items", []):
                cur.execute(INSERT_LINE, (
                    receipt_id,
                    item.get("raw_text", "")[:500],
                    item.get("quantity"),
                    item.get("unit"),
                    item.get("price_eur"),
                    item.get("confidence", 0.5),
                ))
    conn.commit()

print(f"wrote {len(receipt_ids)} receipts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Match line items to ingredients (Spark)
# MAGIC
# MAGIC `BROIL.FILE 700G` has to become the `chicken breast` row in the catalog. We
# MAGIC do a conservative normalise-and-join here and mark everything `auto`; Stage 2
# MAGIC replaces this with embedding similarity, which handles the Finnish/English gap
# MAGIC far better than string matching can.

# COMMAND ----------

with psycopg2.connect(LAKEBASE_URL, cursor_factory=RealDictCursor) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT line_id, raw_text, quantity, unit, price_eur, confidence, receipt_id
            FROM receipt_line_items WHERE match_status = 'unmatched'
        """)
        line_rows = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT ingredient_id, canonical_name, name_fi FROM ingredients")
        ing_rows = [dict(r) for r in cur.fetchall()]

print(f"{len(line_rows)} unmatched lines vs {len(ing_rows)} catalog ingredients")

# COMMAND ----------

if line_rows and ing_rows:
    lines_sdf = spark.createDataFrame(line_rows)
    ings_sdf = spark.createDataFrame(ing_rows)

    def normalise(col):
        """Strip pack sizes, punctuation and Finnish abbreviation dots."""
        c = F.lower(col)
        c = F.regexp_replace(c, r"\d+\s*(g|kg|ml|l|kpl|pcs)\b", " ")   # pack sizes
        c = F.regexp_replace(c, r"[^a-zåäö\s]", " ")                    # punctuation/digits
        return F.trim(F.regexp_replace(c, r"\s+", " "))

    lines_norm = lines_sdf.withColumn("norm", normalise(F.col("raw_text")))
    ings_norm = (
        ings_sdf
        .withColumn("norm_en", normalise(F.col("canonical_name")))
        .withColumn("norm_fi", normalise(F.coalesce(F.col("name_fi"), F.lit(""))))
    )

    # Match when either catalog name is a substring of the normalised receipt text.
    # Deliberately conservative: a wrong price on the wrong ingredient is worse
    # than an unmatched line the UI can show for review.
    matched = (
        lines_norm.alias("l")
        .join(
            ings_norm.alias("i"),
            (F.col("l.norm").contains(F.col("i.norm_en")) & (F.length("i.norm_en") >= 4))
            | (F.col("l.norm").contains(F.col("i.norm_fi")) & (F.length("i.norm_fi") >= 4)),
            "inner",
        )
        .withColumn(
            "match_len",
            F.greatest(F.length("i.norm_en"), F.length("i.norm_fi")),
        )
        # longest catalog name wins - "chicken breast" beats "chicken"
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("l.line_id").orderBy(F.desc("match_len"))
            ),
        )
        .filter(F.col("rank") == 1)
        .select("l.line_id", "i.ingredient_id", "l.quantity", "l.unit",
                "l.price_eur", "l.confidence")
    )

    match_rows = [r.asDict() for r in matched.collect()]
    print(f"matched {len(match_rows)} / {len(line_rows)} lines")
else:
    match_rows = []
    print("nothing to match - run ingest_openfoodfacts.py first")

# COMMAND ----------

# MAGIC %md ## 5. Turn matched lines into priced catalog entries

# COMMAND ----------

def to_unit_price(quantity, unit, price):
    """Normalise to EUR per kg / per litre / per piece."""
    if price is None:
        return None, None
    if not quantity or not unit:
        return None, "piece"
    q, u = float(quantity), unit.lower()
    if u == "g":
        return float(price) / (q / 1000.0), "kg"
    if u == "kg":
        return float(price) / q, "kg"
    if u == "ml":
        return float(price) / (q / 1000.0), "l"
    if u == "l":
        return float(price) / q, "l"
    return float(price) / q, "piece"


INSERT_PRICE = """
INSERT INTO ingredient_prices
    (ingredient_id, store_id, price_eur, quantity, unit, unit_price_eur,
     unit_basis, source, source_ref, confidence, captured_at)
SELECT %s, s.store_id, %s, %s, %s, %s, %s, 'receipt', %s, %s,
       COALESCE(r.purchased_on, CURRENT_DATE)
FROM raw_receipts r
LEFT JOIN stores s ON s.name = r.store_hint
WHERE r.receipt_id = %s
"""

written = 0
with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        for m in match_rows:
            unit_price, basis = to_unit_price(m["quantity"], m["unit"], m["price_eur"])
            cur.execute("SELECT receipt_id FROM receipt_line_items WHERE line_id = %s",
                        (m["line_id"],))
            receipt_id = cur.fetchone()[0]

            cur.execute(INSERT_PRICE, (
                m["ingredient_id"], m["price_eur"], m["quantity"], m["unit"],
                unit_price, basis, f"line:{m['line_id']}", m["confidence"], receipt_id,
            ))
            cur.execute(
                """UPDATE receipt_line_items
                   SET ingredient_id = %s, match_status = 'auto'
                   WHERE line_id = %s""",
                (m["ingredient_id"], m["line_id"]),
            )
            written += 1
    conn.commit()

print(f"wrote {written} prices from receipts")

# COMMAND ----------

with psycopg2.connect(LAKEBASE_URL, cursor_factory=RealDictCursor) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source, COUNT(*) AS n, ROUND(AVG(confidence), 2) AS avg_conf
            FROM ingredient_prices GROUP BY source ORDER BY n DESC
        """)
        for row in cur.fetchall():
            print(f"  {row['source']:16s} {row['n']:5d}  avg confidence {row['avg_conf']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC
# MAGIC The app's **Pipeline** tab now shows the price provenance bar. Unmatched
# MAGIC receipt lines stay in `receipt_line_items` with `match_status = 'unmatched'`
# MAGIC — Stage 2's embedding search picks up most of what string matching missed.
