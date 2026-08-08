# Ruokasuunnitelma — AI family meal planner

Databricks AI bootcamp capstone. Plans a week of Finnish family cooking around
each member's macro targets and dietary restrictions, prices the grocery list
from real receipt data, and learns from what you actually cooked.

**The cooking model:** one cooking session each morning, and that dish covers
lunch and dinner the same day. Seven sessions a week. One pot, different plates —
each member gets a portion multiplier plus protein add-ons sized to their own
target.

---

## Capstone requirements

| Requirement | How it's met |
|---|---|
| Data pipeline in Spark | `notebooks/ingest_openfoodfacts.py` (product catalog + dietary flag derivation), `notebooks/extract_receipts.py` (image extraction + fuzzy ingredient matching) |
| Third-party API | Open Food Facts (free, no key) — plus YouTube Data API v3 in Stage 2 |
| Unstructured data | **Receipt photographs** via vision extraction, plus YouTube video descriptions → structured ingredient lines in Stage 2 |
| Databricks App with frontend | `web/` — Flask + a hand-built frontend, no Streamlit |
| AI agent that does stuff | Stage 3: MCP server with read *and* write tools, driven by Agent Bricks |

---

## Build stages

Mirrors the bootcamp's three days. Each stage ends in something demoable.

- **Stage 1 — App + Lakebase** ✅
  Household setup, ingredient catalog, price provenance, pipeline status.
- **Stage 2 — Context engineering + vectors** ← *you are here*
  pgvector in Lakebase, YouTube recipe harvest, LLM ingredient extraction,
  semantic recipe search.
- **Stage 3 — Agent**
  FastMCP server as a second Databricks App, Agent Bricks agent, cook mode,
  cooking log, and the Spark behaviour job over Lakebase CDF history.

---

## Layout

```
sql/                    schema, run once in file-number order
  01_core.sql             households, members, goals, restrictions
  02_catalog.sql          stores, ingredients, prices, raw landing tables
  03_planning.sql         recipes, plans, portions, grocery lists, cooking log
  04_seed.sql             a starting household — edit before running
notebooks/              Spark jobs, run as Databricks Workflows
  ingest_openfoodfacts.py
  extract_receipts.py
web/                    the Databricks App (Stage 1)
  app.py                  Flask: JSON API + page
  lakebase.py             Postgres connection helper
  nutrition.py            Mifflin-St Jeor targets + recipe scaling
  app.yaml                Databricks Apps config
  templates/, static/     frontend
setup_secrets.py        one-time secret setup
```

Stage 3's `mcp_server/` becomes a sibling of `web/` — each Databricks App
deploys from its own folder, which is why the split exists now.

---

## Setup

### 1. Lakebase instance

Catalog → Lakebase → **Create database instance**. Wait for **Available**.
Then Roles & Databases → enable **native password authentication** → create a
role with password auth → copy the connection URL:

```
postgresql://<role>:<password>@<host>.database.cloud.databricks.com:5432/databricks_postgres?sslmode=require
```

Do this first — provisioning takes real wall-clock time.

### 2. Store secrets

Once the repo is in a Databricks Git folder (step 4), run from a notebook in
that folder:

```python
%sh python setup_secrets.py
```

Prompts for the Lakebase URL, and optionally the YouTube key (Stage 2 — press
Enter to skip).

If you'd rather not wait for the Git folder, paste this into any notebook cell
instead — it does the same thing:

```python
import getpass
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

w = WorkspaceClient()
try:
    w.secrets.create_scope(scope="database")
except Exception as e:
    print(e)
w.secrets.put_secret(scope="database", key="lakebase-url",
                     string_value=getpass.getpass("Lakebase URL: "))
w.secrets.put_acl(scope="database", principal="users",
                  permission=workspace.AclPermission.READ)
```

### 3. Create the schema

Run each file in `sql/` in order against your Lakebase database, using the
Databricks SQL editor or `psql`. Edit `04_seed.sql` first so the members are
your actual family.

### 4. Deploy the app

1. **Workspace → Create → Git folder**, pointing at this repo.
2. **Compute → Apps → Create app** → Custom.
3. For source, select the Git folder and browse to the **`web/`** subfolder
   (the one containing `app.yaml`).
4. Deploy, then open the URL. The sidebar shows a green dot when Lakebase is
   connected.

To redeploy after changes: pull in the Git folder, click Deploy again.

### 5. Run the pipelines

**Create a Volume first** (Catalog → your schema → Create volume), e.g.
`main.mealplan.raw`. Serverless compute blocks direct `SparkContext` access, so
the notebook lands its raw JSON in a Volume and reads it back with Spark. That
also gives you the immutable raw layer — re-parsing never needs a re-fetch.

**Open Food Facts** — Workflows → Create Job → Notebook task pointing at
`notebooks/ingest_openfoodfacts.py`. Set `staging_volume` to the Volume you just
created; the other defaults (`source_mode=api`, `countries=en:finland`) work as
is. Roughly 10,000 Finnish products arrive in ~10 requests.

Country tags need their language prefix — `en:finland`, not `finland`. To widen
coverage, use `en:finland,en:sweden,en:estonia`; those products are on Finnish
shelves anyway.

**Receipts** — create a Volume (e.g. `main.mealplan.receipts`), upload 10–15
receipt photos, set `vision_endpoint` to a vision-capable serving endpoint in
your workspace, and run `notebooks/extract_receipts.py`.

On Databricks Free Edition there are no Claude models; `databricks-llama-4-maverick`
is the only multimodal endpoint, and it is the default. Text-layer PDFs skip the
vision path entirely and are read exactly.

### 6. Stage 2 — vectors and recipes

1. Run `sql/07_vectors.sql`. It needs the `vector` extension, which Lakebase
   provides — `CREATE EXTENSION IF NOT EXISTS vector` is the first statement.
2. Get a **YouTube Data API v3** key (Google Cloud Console → enable the API →
   create an API key) and store it as `mealplan/youtube-api-key`.
3. Run `notebooks/harvest_youtube_recipes.py`. Searching costs 100 quota units
   a call against a 10,000/day budget, so it harvests once and serves from
   Postgres — never search at request time.
4. Run `notebooks/embed_content.py`. It reads the `*_needing_embedding` views,
   so re-running only embeds what's new.
5. Redeploy the app. The **Recipes** tab lights up.

**The one rule for embeddings:** content and query must use the *same* model.
Two models produce vectors of the same length that mean different things —
nothing errors, the results are just silently meaningless. `/api/search/status`
checks for this and the search endpoints return a `warning` if they disagree.

The app embeds queries in-process with
`paraphrase-multilingual-MiniLM-L12-v2` (384-dim). Multilingual is the whole
point here: the catalog is Finnish, the recipes are Indonesian, the household
types English, and an English-only model embeds *Broilerin fileesuikale* as
near-noise. That needs torch in the app image; if it can't load, search falls
back to keyword matching rather than failing, and `/api/search/status` says why.
To use a Databricks embedding endpoint instead, see the header of
`web/embeddings.py` — it's a four-step switch, all of it or none.

---

## Local development

```bash
cp .env.example .env       # paste your Lakebase URL into LAKEBASE_URL
pip install -r web/requirements.txt
python web/app.py          # http://localhost:8000
```

`lakebase.py` prefers `LAKEBASE_URL` from the environment, so local runs never
need workspace auth configured.

---

## Design decisions worth knowing

**No medallion layering.** At this data volume bronze/silver/gold is ceremony.
There are two layers because they do different jobs: immutable `raw_*` landing
tables (so re-parsing never needs a re-fetch) and the curated tables the app
queries. Spark earns its place on the Open Food Facts dump and the Stage 3
behaviour job, not on ceremony.

**Every price carries provenance.** `source` ∈ `{receipt, lidl_scrape,
open_prices, manual_survey}`, plus `captured_at` and `confidence`. The app
reports a grocery total *and* its source breakdown. Prices are estimates from a
capture date, and the UI says so.

**Halal is four-valued, never a boolean.** `certified` (explicit label) /
`likely_ok` (vegetarian or vegan label, nothing flagged) / `contains_flagged`
(pork, gelatine, alcohol or carmine found in the ingredient text) / `unknown`.
The reason is stored alongside and shown in the UI, and anything below
`certified` tells the user to check the packaging. Same discipline for allergens.

**Strict vs preference restrictions.** Halal and allergies are hard constraints
the agent may never violate. "Low spice" is a preference it can trade off.
Collapsing these into one flag would make the agent either too rigid or unsafe.

**Split-protein planning.** A vegetarian and someone bulking on chicken can't
share one pot. When any member is vegetarian, the base dish goes vegetarian and
protein is cooked separately per member — one session, one pot plus one pan.
`/api/households/<id>/constraints` returns `requires_split_protein` for this.

**Sub-linear scaling.** Tripling a recipe and tripling the chilli makes it
inedible. Ingredients carry a `scaling_class`: `linear` (proteins, vegetables,
rice), `sublinear` (salt, spices, oil — scaled by `factor^0.8`), `fixed` (a bay
leaf stays one bay leaf). See `scale_quantity()` in `web/nutrition.py`.

**Child targets are labelled estimates.** Mifflin-St Jeor is validated for
adults. The API returns `is_estimate_only` for under-18s and the UI says so
rather than presenting a confident number.

---

## Data sources, and why

| Source | Role | Status |
|---|---|---|
| Open Food Facts | Product catalog, nutrition, allergens, store tags | Free API + Parquet dump, no key |
| Your receipts | Real Finnish prices, revealed preferences | Vision extraction from photos |
| YouTube Data API v3 | Recipes, Indonesian coverage, cook-along video | Stage 2; free key, 10k units/day |
| Lidl FI | Secondary price scrape | `robots.txt` permits it, no bot wall |
| Manual survey | Staples missing from receipts | Entered through the catalog UI |

**Not used, and why:** Prisma / S-kaupat sits behind a Vercel bot checkpoint;
K-ruoka behind Cloudflare; Kesko's developer portal requires Azure AD partner
credentials. Getting past the first two means defeating an anti-bot control,
and it would be the flakiest thing in the stack. Your own receipts are better
data anyway.

---

## Attribution

Product data from [Open Food Facts](https://world.openfoodfacts.org), available
under the Open Database License. Recipe videos are embedded via YouTube's player
with channel attribution — never re-hosted.
