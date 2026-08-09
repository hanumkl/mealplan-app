# Ruokasuunnitelma — AI family meal planner

Databricks AI bootcamp capstone. Plans a week of Finnish family cooking around
each member's macro targets and dietary restrictions, prices the grocery list
from real receipt data, and learns from what you actually cooked.

**The cooking model:** one cooking session each morning, and that dish covers
lunch and dinner the same day. Seven sessions a week. One pot, different plates —
each member gets a portion multiplier plus protein add-ons sized to their own
target.

---

## What's actually in there

Real numbers from the running system, not targets:

| | |
|---|---|
| Products in the catalog | **8,491** Finnish items from Open Food Facts |
| Receipts extracted | **20** PDFs and photos, 0.90–1.00 confidence |
| Recipes harvested | **239** from YouTube, avg extraction confidence **0.91** |
| Recipe ingredient lines | **4,265**, of which **~70%** matched to the priced catalog at ≥0.72 similarity |
| Recipes flagged not-halal | **18** (2 pork, 16 alcohol) — caught automatically |
| Vector embeddings | ingredients, recipes, recipe step-chunks, cooking log |

---

## Architecture

```
  Open Food Facts API ─┐
  YouTube Data API v3 ─┼─► Spark notebooks ─► Volume (raw JSON, immutable)
  Receipt PDFs/photos ─┘         │
                                 ▼
                        Lakebase (Postgres + pgvector)
                    ┌────────────┴────────────┐
                    │                         │
         curated tables                embedding tables
    households, members, goals      ingredient / recipe /
    ingredients, prices, stores     chunk / cooking-log
    recipes, plans, cooking_log       VECTOR(384) + HNSW
                    │                         │
                    └────────────┬────────────┘
                                 │
              ┌──────────────────┴──────────────────┐
              ▼                                     ▼
   web/  Databricks App                mcp_server/  Databricks App
   Flask + hand-built UI               FastMCP over streamable HTTP
   + in-process agent                  9 tools (3 write)
   (web/agent.py)                      → Agent Bricks
```

Both apps read the same Lakebase. The agent exists twice on purpose: in-process
in the web app so there's a working UI to demo, and as an MCP server so Agent
Bricks can drive the same tools.

**The loop this closes:** plan → cook → log what actually happened → that free
text gets embedded → next week's planning retrieves it as context.

---

## Capstone requirements

| Requirement | How it's met |
|---|---|
| Data pipeline in Spark | `notebooks/ingest_openfoodfacts.py` (product catalog + dietary flag derivation), `notebooks/extract_receipts.py` (image extraction + fuzzy ingredient matching) |
| Third-party API | Open Food Facts (free, no key) — plus YouTube Data API v3 in Stage 2 |
| Unstructured data | **Receipt photographs** via vision extraction, plus YouTube video descriptions → structured ingredient lines in Stage 2 |
| Databricks App with frontend | `web/` — Flask + a hand-built frontend, no Streamlit |
| AI agent that does stuff | `mcp_server/` — FastMCP server as its own Databricks App, 9 tools of which 3 write, for Agent Bricks. Also driven in-app by `web/agent.py` (Planner tab), which shows which calls were writes. |

---

## Build stages

Mirrors the bootcamp's three days. Each stage ends in something demoable.

- **Stage 1 — App + Lakebase** ✅
  Household setup, ingredient catalog, price provenance, pipeline status.
- **Stage 2 — Context engineering + vectors** ✅
  pgvector in Lakebase, YouTube recipe harvest, LLM ingredient extraction,
  semantic recipe search.
- **Stage 3 — Agent** ✅
  A planning agent with tools that read the catalog **and write to it**:
  it commits the week's plan, logs what was actually cooked, and builds a
  priced grocery list. Runs in-process in the app (`web/agent.py`) on a
  Databricks Foundation Model endpoint.

---

## Layout

```
sql/                          schema, run once in file-number order
  01_core.sql                   households, members, goals, restrictions
  02_catalog.sql                stores, ingredients, prices, raw landing tables
  03_planning.sql               recipes, plans, portions, grocery, cooking log
  04_seed.sql                   a starting household — edit before running
  05_add_english_names.sql      English name + category for a Finnish catalog
  06_halal_confirmation.sql     household override, outranks the pipeline
  07_vectors.sql                pgvector tables, VECTOR(384) + HNSW
  08_recipe_matching.sql        recipe → catalog link + match provenance

notebooks/                    Spark jobs, run as Databricks Workflows
  ingest_openfoodfacts.py       product catalog + dietary flag derivation
  extract_receipts.py           PDF text + vision extraction from photos
  harvest_youtube_recipes.py    video → structured ingredients via LLM
  embed_content.py              everything → pgvector
  match_recipe_ingredients.py   semantic recipe → catalog matching

web/                          Databricks App #1 — the UI
  app.py                        Flask: JSON API + page
  agent.py                      the in-process agent, 7 tools, 3 write
  embeddings.py                 query-side embedding, two backends
  lakebase.py                   Postgres connection helper
  nutrition.py                  Mifflin-St Jeor targets + recipe scaling
  units.py                      recipe units → grams, honest about failures
  app.yaml                      Databricks Apps config
  templates/, static/           frontend, no build step

mcp_server/                   Databricks App #2 — the MCP server
  mealplan_mcp_server.py        thin @mcp.tool wrappers, docstrings only
  mealplan_store.py             adapter: all SQL and derived logic
  README.md                     tools, setup, Agent Bricks system prompt

screenshots/                  demo evidence
setup_secrets.py              one-time secret setup
```

Each Databricks App deploys from its own folder, which is why `web/` and
`mcp_server/` are siblings. The MCP split follows Day 3's
`alpaca_mcp_server.py` / `alpaca_broker.py` pattern: **no database calls inside
the tool functions**.

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

> **Use the `mealplan` scope, not `database`.** Every project in the bootcamp
> defaults to `database/lakebase-url`, so setting up a second one overwrites
> this project's connection. The symptom is baffling: notebooks report
> `relation "recipes" does not exist` for tables you can plainly see in the
> catalog browser, because they're connected to a different Lakebase entirely.
> Print `current_database()` and the connection user if that ever happens.

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
    w.secrets.create_scope(scope="mealplan")
except Exception as e:
    print(e)
w.secrets.put_secret(scope="mealplan", key="lakebase-url",
                     string_value=getpass.getpass("Lakebase URL: "))
w.secrets.put_acl(scope="mealplan", principal="users",
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
5. Run `sql/08_recipe_matching.sql`, then `notebooks/match_recipe_ingredients.py`.
6. Redeploy the app. The **Recipes** tab lights up.

### 7. Why matching matters

Step 5 is what turns a recipe from text into something plannable. The recipe
says `chicken thigh`; the catalog says `Broilerin koipireisi`. Until those are
linked there are no calories, no protein and no euro cost — which also means no
grocery list total.

String matching can't bridge Indonesian recipe text to a Finnish product
catalog, which is exactly why the embeddings are multilingual. The matcher
takes the 10 nearest catalog entries and prefers one that actually carries
nutrition data, since a marginally-closer product with no calories is useless
here.

Below the similarity threshold it leaves the row **unmatched rather than
guessing**. A wrong match silently skews a week of nutrition; a missing one
shows up in the app as `unmatched`, and you can fix it by clicking it. Your
choice is recorded as `manual` and the notebook never overwrites it — same rule
as halal confirmation.

The recipe view shows totals next to their coverage (“based on 8 of 11
ingredients”), and marks spoon and volume measures as estimates. A partial
total presented as complete is how someone ends up bulking on a plan that's
800 kcal short.

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

## The agent

Seven tools in the app, nine in the MCP server. Three of them **write**.

| Tool | Writes | What it does |
|---|---|---|
| `get_household` | | Members, targets, strict vs. preference restrictions |
| `search_recipes` | | Semantic search; strict restrictions applied **in SQL** |
| `get_recipe` | | Ingredients scaled to servings, nutrition, cost, coverage |
| `get_cooking_history` | | What was really cooked, and why plans were abandoned |
| `suggest_todays_meal` *(MCP)* | | Derived judgement — see below |
| `create_meal_plan` | ✍️ | Commits a week to `meal_plans` + `meal_plan_items` |
| `log_cooked` | ✍️ | Records reality into `cooking_log` |
| `build_grocery_list` | ✍️ | Aggregates and prices a week's shopping |

**Restrictions are enforced in SQL, not in the prompt.** A model can be talked
out of an instruction; a `WHERE` clause can't. For a halal household that's the
difference between a guardrail and a suggestion.

**Invented IDs are refused.** `create_meal_plan` rejects `recipe_id`s that don't
exist rather than writing a plan pointing at phantom recipes.

**Narrated calls are caught.** Llama 4 Maverick sometimes replies *"I will
commit a week's plan with create_meal_plan(...)"* as plain text without calling
anything. The loop detects that and pushes back, because a described write that
the user believes happened is the worst possible failure for a write tool.

`suggest_todays_meal` is a judgement call, not a passthrough: strict
restrictions in SQL, exclude anything cooked in the last N days, honour a time
cap, then rank by closeness to the household's per-meal calorie need — and it
reports which rules fired.

---

## Screenshots

In [`screenshots/`](screenshots/):

| | |
|---|---|
| `1_saved_plan.png` | Agent commits a week — green **write** chip, 7 days, plan panel filled |
| `2_mealplan_adjusted.png` | Replanning the same week |
| `3_cook_adjustment.png` | Logging a deviation → writes to `cooking_log` |
| `4_ingredients_catalog.png` | 8,491 products, English hints, halal badges |
| `5_recipes_embedding.png` | Semantic recipe search with match percentages |
| `6_recipes_connect_video.png` | Recipe linked to its YouTube source |
| `7_recipe.png` | Scaled ingredients, nutrition, cost, coverage |
| `8_portion_calculation.png` | Per-member portions from one shared pot |
| `9_household_goal.png` | Members, goals, strict vs. preference restrictions |
| `10_pipeline_status.png` | Row counts proving the Spark jobs landed data |

---

## Known limitations

Named here rather than left for a reviewer to find.

**Quantity extraction is the weak link.** The LLM reading a video description
sometimes invents quantities — 500 g of garlic in a gado-gado. Matching is
solid (≥0.72 similarity, non-food lines excluded); it's the numbers next to the
ingredients that are unreliable. The fix is a stricter extraction prompt forcing
`quantity: null` unless a number is explicitly written, then a re-harvest.
Consequence: some recipes show inflated calories, which is why every total
carries its coverage and why portion multipliers are flagged when incomplete.

**Duplicate dishes.** "Tempe Orek" and "Tempeh Orek" are the same dish with
different spellings, and a week's plan can contain both. Real dedupe needs
recipe-level identity — clustering on the ingredient vector — not title
matching.

**Halal is never auto-approved.** A video description can prove a recipe
contains pork; it can never prove how the meat was slaughtered. Clean recipes
read `unknown`, not "halal". The household confirms, and that confirmation
outranks the pipeline and survives re-ingestion.

**Prices are a floor.** They come from the household's own receipts, so they're
real but dated, and anything unpriced is excluded from the total rather than
guessed at.

**Agent Bricks on Free Edition** is unverified. The MCP server deploys fine;
whether Agent Bricks is offered on Free Edition should be checked. The in-app
agent covers the same tools either way.

---

## Attribution

Product data from [Open Food Facts](https://world.openfoodfacts.org), available
under the Open Database License. Recipe videos are embedded via YouTube's player
with channel attribution — never re-hosted.
