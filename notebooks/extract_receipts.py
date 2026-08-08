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
# MAGIC 1. Create a Volume, e.g. `workspace.default.receipts`
# MAGIC 2. Upload receipts into it — **JPG, PNG and PDF all work**
# MAGIC    (Catalog → the volume → *Upload to this volume*)
# MAGIC 3. Set `vision_endpoint` to a model in your workspace. The next cell
# MAGIC    prints the ones you actually have, so you don't have to guess.

# COMMAND ----------

# MAGIC %pip install openai pillow pymupdf
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("volume_path", "/Volumes/workspace/default/receipts", "Receipt volume (jpg/png/pdf)")
dbutils.widgets.text("vision_endpoint", "databricks-llama-4-maverick", "Vision serving endpoint")
dbutils.widgets.text("default_store", "Prisma", "Store when the receipt doesn't say")
dbutils.widgets.text("lakebase_scope", "mealplan", "Secret scope")
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

# MAGIC %md
# MAGIC ### Which model can read a receipt?
# MAGIC
# MAGIC A "serving endpoint" is just a model your workspace can call over HTTP.
# MAGIC This one has to be **vision-capable** (able to accept an image), unless
# MAGIC every receipt you upload is a text-layer PDF.
# MAGIC
# MAGIC Run the cell below and copy a name into the `vision_endpoint` widget.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Names that indicate a model can accept images. Text-only models will error
# on an image payload, so it's worth checking before a long run.
VISION_HINTS = ("claude", "gpt-4", "gpt-5", "gemini", "llama-4", "pixtral", "vision")

print(f"{'endpoint':52s} likely vision?")
print("-" * 70)
for ep in WorkspaceClient().serving_endpoints.list():
    name = ep.name or ""
    likely = "yes" if any(h in name.lower() for h in VISION_HINTS) else "-"
    print(f"  {name:50s} {likely}")

print(f"\ncurrently configured: {VISION_ENDPOINT}")

# COMMAND ----------

import base64
import json
import re

import psycopg2
from psycopg2.extras import RealDictCursor
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Find receipt files (images and PDFs)
# MAGIC
# MAGIC Digital receipts from S-mobiili and K-Ruoka download as PDFs, while
# MAGIC photographed paper receipts are images. Both are handled:
# MAGIC
# MAGIC - **PDF with a text layer** (a real digital receipt) - the text is read
# MAGIC   directly and sent to the model as text. More accurate and much cheaper
# MAGIC   than looking at a picture of it, because nothing has to be recognised.
# MAGIC - **PDF without text** (a scan) - each page is rendered to PNG and goes
# MAGIC   down the vision path.
# MAGIC - **JPG / PNG** - straight to the vision path.

# COMMAND ----------

files_sdf = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.{jpg,jpeg,png,JPG,JPEG,PNG,pdf,PDF}")
    .load(VOLUME_PATH)
    .select("path", "length", "modificationTime", "content")
)

total = files_sdf.count()
if total == 0:
    raise RuntimeError(
        f"No receipts found in {VOLUME_PATH}. Upload .jpg/.png/.pdf files there "
        f"first (Catalog > your volume > Upload to this volume)."
    )

print(f"found {total} receipt files in {VOLUME_PATH}")
display(
    files_sdf
    .withColumn("kind", F.when(F.lower(F.col("path")).endswith(".pdf"), "pdf")
                         .otherwise("image"))
    .select("path", "kind", "length", "modificationTime")
)

# COMMAND ----------

# Skip receipts already extracted, unless explicitly reprocessing.
with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT image_path FROM raw_receipts WHERE extraction_status = 'extracted'")
        done = {r[0] for r in cur.fetchall()}

pending = files_sdf.collect() if REPROCESS else [
    r for r in files_sdf.collect() if r["path"] not in done
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

Extract every purchased line item. Return STRICT JSON only - no prose, no
markdown fence - shaped exactly:

{
  "store": "the shop name exactly as printed at the top, or null",
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

NUMBERS AND DATES - these are Finnish conventions and get this wrong often:
- Prices use a decimal COMMA. "3,07" means 3.07 - emit it as the JSON number
  3.07, never as a string and never as 307.
- Thousands may use a space or dot: "1 234,56" means 1234.56.
- Dates print as D.M.YYYY. "27.6.2026" must become "2026-06-27".

ITEM LAYOUT - most Finnish tills print ONE ITEM ACROSS TWO LINES:

    Chicken feet 1kg                 <- line 1: the product name
    1,00X    3,30            3,30    <- line 2: qty X unit-price, then total

    Kanan Rintafile
    104165      1,325X   8,49   11,25

  On the second line a leading number is a product/barcode code - ignore it.
  "1,325X  8,49  11,25" means 1.325 units at 8.49 each, 11.25 paid.

  price_eur is ALWAYS the RIGHTMOST number (what was actually paid), never
  the unit price. For "1,325X 8,49 11,25" -> price_eur 11.25, quantity 1.325.

WEIGHT-PRICED ITEMS:
  A fractional quantity (1,325X / 2,385X / 0,445X) means the item was weighed
  and priced per kilo. Set quantity to that number and unit to "kg".
  A whole quantity (1,00X / 2,000X) means pieces - if the NAME contains a pack
  size ("800g", "1kg", "6kpl", "1L"), use that as quantity/unit instead.

DISCOUNTS:
  A line like "-50,00 %" or "-50,%" applies to the item above it. Always use
  the final amount paid, not the pre-discount price.

WHICH LINES ARE ITEMS:
- Items sit between the dashed separator lines.
- The store name is on the first line - copy it as printed, e.g.
  "Alanya Market Itis" or "S-MARKET ITAKESKUS".

NEVER treat these as items - Finnish receipts are full of them:
- YHTEENSA (total), VALISUMMA (subtotal)
- ALV / VEROTON / VERO / VEROLLINEN (the VAT breakdown table)
- BONUSTA KERRYTTAVAT OSTOT, Jasennumero, Bonustapahtuma (loyalty)
- CARD TRANSACTION and everything after it: Card, Application, Tr.Nr/Auth,
  Payee/business, Reference, Debit/Charge, PayPass
- PANTTI (bottle deposit), MUOVIKASSI / KASSI (carrier bag)
- Opening hours, phone numbers, business IDs (Y-tunnus), marketing text
- Policy notices printed BETWEEN items, e.g. "Lahjatavaroilla ei ole vaihto-
  eika palautus oikeutta!" - these interrupt the item list, skip them and
  carry on with the next real item

OTHER RULES:
- Keep raw_text EXACTLY as printed, including Finnish abbreviations and
  capitalisation ("BROIL.FILE 700G").
- price_eur is what was actually paid for that line, after any discount.
- If a pack size appears in the name ("700G", "1L"), put it in quantity/unit.
  Many receipt lines have no size at all - then use null, don't guess.
- Set a low confidence on any line you had to guess at. Never invent items.
- If the receipt has no purchasable items, return "items": [].
"""


def pdf_text_and_images(pdf_bytes: bytes) -> tuple[str, list[bytes]]:
    """Return (embedded text, page PNGs) for a PDF.

    PyMuPDF is used rather than pdf2image because it needs no system packages -
    poppler isn't installed on serverless compute.
    """
    try:
        import pymupdf as fitz          # current name
    except ImportError:
        import fitz                     # older releases

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    text = "\n".join(page.get_text() for page in doc)
    images = []
    if len(text.strip()) < 120:                # no usable text layer -> scan
        for page in doc:
            # 2x zoom: receipts print small and the default 72dpi loses digits
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            images.append(pix.tobytes("png"))
    doc.close()
    return text, images


class EndpointMissing(RuntimeError):
    """The configured serving endpoint doesn't exist in this workspace."""


def _call_model(content) -> dict:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient().serving_endpoints.get_open_ai_client()
    try:
        return _do_call(client, content)
    except Exception as exc:
        # A wrong endpoint name fails identically on all 23 receipts. Stop on
        # the first one and say what's actually available.
        if "ENDPOINT_NOT_FOUND" in str(exc) or "404" in str(exc):
            available = [e.name for e in WorkspaceClient().serving_endpoints.list()]
            raise EndpointMissing(
                f"Serving endpoint {VISION_ENDPOINT!r} does not exist in this "
                f"workspace.\nAvailable: {', '.join(available)}\n"
                f"Set the vision_endpoint widget to one of these - on Databricks "
                f"Free Edition, 'databricks-llama-4-maverick' is the multimodal one."
            ) from exc
        raise


def _do_call(client, content) -> dict:
    import time as _time

    # Free Edition throttles serving endpoints, and 23 sequential receipts can
    # trip it. Back off rather than losing the rest of the batch.
    for attempt in range(4):
        try:
            return _parse_response(client, content)
        except Exception as exc:
            if "429" in str(exc) or "rate limit" in str(exc).lower():
                wait = 5 * (attempt + 1)
                print(f"    rate limited, retrying in {wait}s")
                _time.sleep(wait)
                continue
            raise
    raise RuntimeError("rate limited on every retry")


def _parse_response(client, content) -> dict:
    response = client.chat.completions.create(
        model=VISION_ENDPOINT,
        max_tokens=4096,
        messages=[{"role": "user", "content": content}],
    )
    text = response.choices[0].message.content.strip()
    # Models sometimes wrap JSON in a fence despite instructions.
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    return json.loads(text)


def extract_from_images(images: list[bytes]) -> dict:
    """Vision path - one call carrying every page of the receipt."""
    content = [{"type": "text", "text": EXTRACTION_PROMPT}]
    for img in images:
        b64 = base64.b64encode(img).decode("utf-8")
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}"},
        })
    return _call_model(content)


def extract_from_text(text: str) -> dict:
    """Text path - for PDFs that already carry their text.

    Cheaper and more reliable than the vision path: the characters are exact
    rather than recognised, so prices and pack sizes can't be misread.
    """
    return _call_model(
        f"{EXTRACTION_PROMPT}\n\nRECEIPT TEXT:\n{text[:12000]}"
    )


def extract_receipt(path: str, content: bytes) -> dict:
    """Route a receipt to the text or vision path based on what it actually is."""
    if path.lower().endswith(".pdf"):
        text, page_images = pdf_text_and_images(content)
        if page_images:
            return extract_from_images(page_images) | {"_mode": "pdf-scan-vision"}
        return extract_from_text(text) | {"_mode": "pdf-text"}
    return extract_from_images([content]) | {"_mode": "image-vision"}

# COMMAND ----------

results = []
for row in pending:
    path = row["path"]
    try:
        parsed = extract_receipt(path, row["content"])
        parsed["_path"] = path
        parsed["_status"] = "extracted"
        n = len(parsed.get("items", []))
        print(f"OK   {path.split('/')[-1]:36s} [{parsed.get('_mode','?'):16s}] "
              f"{n:3d} items  conf={parsed.get('confidence')}")
    except EndpointMissing:
        raise                      # configuration error - stop, don't retry 22 more
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

# Receipts name shops in ways the stores table doesn't ("S-MARKET ITAKESKUS",
# "Alanya Market Itis"), and you shop at places that were never seeded at all.
# Falling back to DEFAULT_STORE would file an Asian grocer's prices under
# Prisma, quietly corrupting the provenance the whole design rests on.
STORE_ALIASES = [
    ("prisma", "Prisma"),
    ("s-market", "S-market"),
    ("smarket", "S-market"),
    ("alanya", "Alanya"),
    ("lidl", "Lidl"),
    ("k-market", "K-Market"),
    ("k-supermarket", "K-Market"),
    ("k-citymarket", "K-Market"),
]


def resolve_store(printed_name: str | None) -> str:
    """Map a printed shop name to a stores.name, creating it if it's new."""
    if not printed_name:
        return DEFAULT_STORE

    low = printed_name.strip().lower()
    for needle, canonical in STORE_ALIASES:
        if needle in low:
            return canonical

    # Unknown shop: register it rather than mis-filing its prices. halal_certified
    # stays false - only you can assert that, via the app.
    clean = " ".join(printed_name.split())[:80]
    with psycopg2.connect(LAKEBASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO stores (name, chain, city, notes)
                   VALUES (%s, NULL, NULL, 'auto-added from a receipt')
                   ON CONFLICT (name) DO NOTHING""",
                (clean,),
            )
        conn.commit()
    print(f"    registered new store: {clean}")
    return clean

INSERT_LINE = """
INSERT INTO receipt_line_items
    (receipt_id, raw_text, quantity, unit, price_eur, confidence)
VALUES (%s, %s, %s, %s, %s, %s)
"""


# A vision model returns free-form JSON, so anything it produces has to be
# treated as untrusted input. One malformed date ("2026-13-45", "last Tuesday")
# would otherwise abort the whole receipt batch.
def safe_date(value):
    """Parse a date, accepting ISO and the Finnish D.M.YYYY form.

    Receipts print "27.6.2026", so ISO-only parsing would throw away the
    purchase date on every single one - which in turn makes captured_at wrong
    and breaks any 'price as of' claim in the UI.
    """
    from datetime import date, datetime

    if not value:
        return None
    text = str(value).strip()

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    try:
        return date.fromisoformat(text[:10])
    except (ValueError, TypeError):
        print(f"    ignoring unparseable date: {value!r}")
        return None


def safe_number(value):
    """Parse a number, accepting the European decimal comma.

    Finnish receipts print "3,07" and "1 234,56". float("3,07") raises, so
    without this every price would come back None and the line would be
    dropped as priceless - silent, total data loss.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    # strip currency symbols, spaces and non-breaking spaces used as separators
    text = re.sub(r"[€$£\s ]", "", text)
    if not text:
        return None

    if "," in text:
        # "1.234,56" -> thousands dot; "3,07" -> decimal comma
        if "." in text and text.rfind(".") < text.rfind(","):
            text = text.replace(".", "")
        text = text.replace(",", ".")

    try:
        return float(text)
    except (ValueError, TypeError):
        return None

receipt_ids = []
with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        for r in results:
            cur.execute(INSERT_RECEIPT, (
                r["_path"],
                resolve_store(r.get("store")),
                safe_date(r.get("purchased_on")),
                safe_number(r.get("total_eur")),
                json.dumps(r),
                r["_status"],
                safe_number(r.get("confidence")),
            ))
            receipt_id = cur.fetchone()[0]
            receipt_ids.append(receipt_id)

            for item in r.get("items", []):
                cur.execute(INSERT_LINE, (
                    receipt_id,
                    str(item.get("raw_text") or "")[:500],
                    safe_number(item.get("quantity")),
                    item.get("unit"),
                    safe_number(item.get("price_eur")),
                    # .get(key, default) still returns None when the key exists
                    # with a null value, which the model does emit.
                    safe_number(item.get("confidence")) or 0.5,
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
    # Explicit schemas rather than inference. psycopg2 hands back Decimal for
    # NUMERIC columns, and if a column happens to be entirely NULL in this
    # batch (very common for `quantity`) Spark can't infer a type at all and
    # createDataFrame fails outright.
    line_schema = T.StructType([
        T.StructField("line_id", T.IntegerType()),
        T.StructField("raw_text", T.StringType()),
        T.StructField("quantity", T.DoubleType()),
        T.StructField("unit", T.StringType()),
        T.StructField("price_eur", T.DoubleType()),
        T.StructField("confidence", T.DoubleType()),
        T.StructField("receipt_id", T.IntegerType()),
    ])
    ing_schema = T.StructType([
        T.StructField("ingredient_id", T.IntegerType()),
        T.StructField("canonical_name", T.StringType()),
        T.StructField("name_fi", T.StringType()),
    ])

    def _clean(rows, fields):
        """Decimal -> float so the declared schema accepts the values."""
        out = []
        for r in rows:
            row = {}
            for name, kind in fields:
                v = r.get(name)
                row[name] = float(v) if kind == "num" and v is not None else v
            out.append(row)
        return out

    lines_sdf = spark.createDataFrame(
        _clean(line_rows, [("line_id", "int"), ("raw_text", "str"),
                           ("quantity", "num"), ("unit", "str"),
                           ("price_eur", "num"), ("confidence", "num"),
                           ("receipt_id", "int")]),
        schema=line_schema,
    )
    ings_sdf = spark.createDataFrame(
        _clean(ing_rows, [("ingredient_id", "int"), ("canonical_name", "str"),
                          ("name_fi", "str")]),
        schema=ing_schema,
    )

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
    # Say which side is actually empty - these have very different causes.
    if not ing_rows:
        print("no ingredients in the catalogue - run ingest_openfoodfacts.py first")
    else:
        with psycopg2.connect(LAKEBASE_URL, cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                      (SELECT count(*) FROM raw_receipts)                            AS receipts,
                      (SELECT count(*) FROM raw_receipts WHERE extraction_status='extracted') AS extracted,
                      (SELECT count(*) FROM raw_receipts WHERE extraction_status='failed')    AS failed,
                      (SELECT count(*) FROM receipt_line_items)                      AS line_items,
                      (SELECT count(*) FROM receipt_line_items WHERE match_status='unmatched') AS unmatched
                """)
                d = dict(cur.fetchone())
        print(f"no unmatched receipt lines to match. {ing_rows and len(ing_rows)} ingredients exist, so:")
        print(f"  receipts rows:   {d['receipts']} "
              f"({d['extracted']} extracted, {d['failed']} failed)")
        print(f"  line items:      {d['line_items']} ({d['unmatched']} unmatched)")
        if d["receipts"] == 0:
            print("  -> no receipts were written. Did section 3 run?")
        elif d["line_items"] == 0:
            print("  -> receipts exist but produced no line items. Inspect the model "
                  "output:  SELECT image_path, extraction_status, payload FROM raw_receipts;")
        else:
            print("  -> every line is already matched; nothing new to do.")

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

# ingredient_prices.price_eur and .confidence are both NOT NULL, and a vision
# model will occasionally return a line with no price or an explicit null
# confidence. Drop the priceless ones and default the confidence rather than
# letting one bad line abort the whole batch.
priced = [m for m in match_rows if m.get("price_eur") is not None]
skipped = len(match_rows) - len(priced)
if skipped:
    print(f"skipping {skipped} matched lines with no price")

written = 0
with psycopg2.connect(LAKEBASE_URL) as conn:
    with conn.cursor() as cur:
        for m in priced:
            if m.get("confidence") is None:
                m["confidence"] = 0.5
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
