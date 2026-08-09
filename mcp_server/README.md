# Mealplan MCP Server

FastMCP server exposing the meal-planning tools over streamable HTTP, deployed
as its own Databricks App and registered with Agent Bricks as an external MCP
server — the Day 3 pattern.

```
mcp_server/
  mealplan_mcp_server.py   thin @mcp.tool wrappers, docstrings only
  mealplan_store.py        adapter: all SQL, unit conversion, ranking logic
  requirements.txt
  app.yaml
```

The split mirrors Day 3's `alpaca_mcp_server.py` / `alpaca_broker.py`: **no
database calls inside the tool functions**. Everything talks to Lakebase
through the adapter.

---

## Tools

Nine tools. Five read, three write, one health check.

| Tool | Kind | What it does |
|---|---|---|
| `get_household_profile` | read | Members, calorie/protein targets, strict vs. preference restrictions |
| `search_recipes` | read | Semantic search over harvested YouTube recipes; strict restrictions applied in SQL |
| `get_recipe_details` | read | Ingredients scaled to servings, with nutrition, cost and **coverage** |
| `get_cooking_history` | read | What was really cooked, and why plans were abandoned |
| `suggest_todays_meal` | read | **Derived judgement** — see below |
| `create_meal_plan` | **write** | Commits a week to `meal_plans` + `meal_plan_items` |
| `log_cooked_meal` | **write** | Records reality into `cooking_log` |
| `build_grocery_list` | **write** | Aggregates and prices a week into `grocery_lists` + `grocery_items` |
| `health_check` | read | Distinguishes "can't connect" from "database is empty" |

### `suggest_todays_meal` is not a passthrough

It applies explicit rules and reports which ones fired:

1. Strict dietary restrictions — enforced **in SQL**, so a forbidden dish never
   enters the model's context at all.
2. Excludes anything cooked in the last N days. Repetition is the usual reason
   a meal plan gets abandoned.
3. Drops recipes over `max_minutes`, if given.
4. Ranks the rest by how closely one serving matches the household's per-meal
   calorie need — one dish covers lunch and dinner, so roughly 65% of a day.

If no candidate has complete nutrition data it says so and picks on constraints
alone, rather than implying the calories were checked.

---

## Design notes

**Restrictions are enforced in SQL, not in the prompt.** A model can be talked
out of an instruction. A `WHERE` clause can't. For a halal household this is
the difference between a guardrail and a suggestion.

**Nothing claims to be halal-certified.** A YouTube description can prove a
recipe contains pork; it can never prove how the meat was slaughtered. Clean
recipes come back `unknown`, and the tool docstrings tell the agent to say so.

**Numbers arrive with their coverage.** `get_recipe_details` returns
`is_complete` and a `coverage` sentence. A recipe whose chicken didn't match
the catalogue would otherwise return a confident-looking small number and
someone bulking would plan around it.

**Invented IDs are refused.** `create_meal_plan` rejects `recipe_id`s that
don't exist rather than writing a plan pointing at phantom recipes.

**Errors return `{"error": "..."}`**, never a stack trace — the agent can act
on that, ask the user to clarify, or try another tool.

**Writes are idempotent.** Re-planning a week replaces its days; re-logging a
date updates it. Neither duplicates.

---

## Setup

### 1. Secrets

Reuses the main app's secret. Nothing is hardcoded:

```
mealplan/lakebase-url
```

Resolved via `WorkspaceClient().secrets.get_secret()` — see `_lakebase_url()`.

> Use the `mealplan` scope, not `database`. Every bootcamp project defaults to
> `database/lakebase-url`, so a second project silently repoints this one.

### 2. Deploy as a Databricks App

1. **Compute → Apps → Create app → Custom**
2. Source: this Git folder, then browse to the **`mcp_server/`** subfolder
   (the one containing `app.yaml`)
3. Deploy, and note the app URL

The MCP endpoint is at `https://<app-url>/mcp`.

### 3. Register with Agent Bricks

**Agent Bricks → Create agent → Tools → Add external MCP server**, pointing at
`https://<app-url>/mcp`.

---

## Agent system prompt

```
You are the meal planner for a family in Finland. You have tools that read
their catalogue and write to it. Use them — never answer from memory, and
never invent a recipe, an ingredient, a price or a calorie count.

THE COOKING MODEL, which is not negotiable:
One cooking session each morning. That single dish covers BOTH lunch and
dinner the same day. A week is 7 dishes, not 21. Never propose separate lunch
and dinner meals.

ONE POT, DIFFERENT PLATES:
Members have different calorie goals. Do not cook different meals for them —
vary the portion size and add protein per person. If someone is vegetarian and
someone is bulking on meat, the base dish is vegetarian and the meat is a
per-person add-on.

TOOL ORDER:
1. get_household_profile first, always, before planning anything.
2. get_cooking_history before planning a week, so you don't repeat dishes or
   re-plan something they already abandoned.
3. search_recipes or suggest_todays_meal to find candidates. Plan only
   recipe_ids these returned.
4. get_recipe_details to check calories and cost before committing.
5. create_meal_plan to commit. Say what you're about to write, in one line,
   before you write it.
6. build_grocery_list only after a plan exists for that week.

GUARDRAILS:
- Strict restrictions are absolute. Never plan a dish that violates one and
  never argue with one. They are already filtered out of search results — if a
  search returns nothing, say so rather than loosening the filter.
- A recipe marked 'halal unverified' has no pork or alcohol in its ingredient
  list, but nothing confirms how its meat was sourced. Say that plainly when
  it matters. Never describe it as halal-approved.
- When get_recipe_details reports is_complete = false, the nutrition is
  partial. Quote the number and say what it's missing. Do not present it as
  the full figure.
- Grocery totals are estimates from past receipts and exclude anything that
  couldn't be priced. Call the total a floor, not a bill.
- If a tool returns {"error": ...}, tell the user what failed and what would
  fix it. Do not retry silently and do not guess the answer.
- If unenforceable_restrictions is non-empty, name those restrictions and tell
  the user to check the ingredients themselves.

STYLE:
Brief and concrete. They are cooking, not reading.
```

---

## Databricks Free Edition

Apps and Lakebase work on Free Edition, so the MCP server deploys normally.
**Agent Bricks availability on Free Edition is worth verifying before you rely
on it** — if it isn't offered, the same tools are already driven by the
in-app agent at `web/agent.py` (the **Planner** tab), which calls
`databricks-llama-4-maverick` directly. That gives you a working agent demo
either way, and the MCP server still stands as the deployable tool surface.

Model note: Free Edition has no Claude models; `databricks-llama-4-maverick`
is the only multimodal endpoint and is the default throughout this project.

---

## Testing without Agent Bricks

```bash
curl -N -H "Authorization: Bearer $DATABRICKS_TOKEN" \
     -H "Content-Type: application/json" \
     -H "Accept: application/json, text/event-stream" \
     -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
     https://<app-url>/mcp
```

Should list all nine tools with their schemas. Then call `health_check` to
confirm it can reach Lakebase and see your data.
