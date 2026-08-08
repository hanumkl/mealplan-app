-- =============================================================================
-- 03_planning.sql - Recipes, meal plans, portions, grocery lists, cooking log.
--
-- These tables are created in Stage 1 so Stages 2 and 3 never need a migration,
-- but only `cooking_log` and `recipes` get written to before Stage 2.
--
-- Cooking model: ONE cooking session per day (in the morning), and that dish
-- covers lunch and dinner the same day. So a week = 7 sessions, and each day
-- has one base dish plus per-member protein add-ons.
-- =============================================================================

CREATE TABLE IF NOT EXISTS recipes (
    recipe_id             SERIAL PRIMARY KEY,
    title                 TEXT NOT NULL,
    cuisine               TEXT,
    language              TEXT DEFAULT 'en',

    source                TEXT NOT NULL DEFAULT 'manual'
                          CHECK (source IN ('youtube', 'themealdb', 'manual')),
    source_ref            TEXT,
    video_id              TEXT,
    video_url             TEXT,
    channel_title         TEXT,
    thumbnail_url         TEXT,
    duration_min          INTEGER,

    base_servings         NUMERIC(5, 2) NOT NULL DEFAULT 4,
    description           TEXT,               -- raw video description / blurb
    instructions          TEXT,
    transcript            TEXT,               -- optional, Stage 2

    -- Derived from the ingredient list once ingredients are matched.
    is_vegetarian         BOOLEAN,
    is_vegan              BOOLEAN,
    contains_pork         BOOLEAN,
    contains_gluten       BOOLEAN,
    contains_lactose      BOOLEAN,
    halal_status          TEXT NOT NULL DEFAULT 'unknown'
                          CHECK (halal_status IN
                              ('certified', 'likely_ok', 'contains_flagged', 'unknown')),

    -- LLM extraction is imperfect and this says so out loud. Only 'approved'
    -- recipes are plannable; the rest sit in the review queue in the UI.
    extraction_confidence NUMERIC(3, 2),
    review_status         TEXT NOT NULL DEFAULT 'pending'
                          CHECK (review_status IN ('pending', 'approved', 'rejected')),

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_ref)
);

CREATE INDEX IF NOT EXISTS idx_recipes_review ON recipes(review_status);
CREATE INDEX IF NOT EXISTS idx_recipes_cuisine ON recipes(cuisine);


CREATE TABLE IF NOT EXISTS recipe_ingredients (
    ri_id                SERIAL PRIMARY KEY,
    recipe_id            INTEGER NOT NULL REFERENCES recipes(recipe_id) ON DELETE CASCADE,
    ingredient_id        INTEGER REFERENCES ingredients(ingredient_id),
    raw_text             TEXT NOT NULL,       -- "2 sdm kecap manis", "1 ekor ayam"
    quantity             NUMERIC(10, 3),
    unit                 TEXT,
    scaling_class        TEXT NOT NULL DEFAULT 'linear'
                         CHECK (scaling_class IN ('linear', 'sublinear', 'fixed')),
    -- Protein components are cooked separately per member, so one pot can serve
    -- a vegetarian and someone bulking on chicken from the same base dish.
    is_protein_component BOOLEAN NOT NULL DEFAULT FALSE,
    is_optional          BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order           INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_recipe ON recipe_ingredients(recipe_id);


CREATE TABLE IF NOT EXISTS meal_plans (
    plan_id      SERIAL PRIMARY KEY,
    household_id INTEGER NOT NULL REFERENCES households(household_id) ON DELETE CASCADE,
    week_start   DATE NOT NULL,
    status       TEXT NOT NULL DEFAULT 'draft'
                 CHECK (status IN ('draft', 'active', 'archived')),
    rationale    TEXT,                        -- the agent explains itself here
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (household_id, week_start)
);

CREATE TABLE IF NOT EXISTS meal_plan_items (
    item_id       SERIAL PRIMARY KEY,
    plan_id       INTEGER NOT NULL REFERENCES meal_plans(plan_id) ON DELETE CASCADE,
    plan_date     DATE NOT NULL,
    recipe_id     INTEGER REFERENCES recipes(recipe_id),
    base_servings NUMERIC(5, 2) NOT NULL DEFAULT 4,
    notes         TEXT,
    UNIQUE (plan_id, plan_date)
);


-- One shared pot, different plates. This is the per-member portion matrix.
CREATE TABLE IF NOT EXISTS portion_assignments (
    pa_id              SERIAL PRIMARY KEY,
    item_id            INTEGER NOT NULL REFERENCES meal_plan_items(item_id) ON DELETE CASCADE,
    member_id          INTEGER NOT NULL REFERENCES members(member_id) ON DELETE CASCADE,
    portion_multiplier NUMERIC(4, 2) NOT NULL DEFAULT 1.00,
    -- e.g. [{"ingredient":"chicken thigh","qty":150,"unit":"g"},
    --       {"ingredient":"egg","qty":2,"unit":"piece"}]
    addons             JSONB NOT NULL DEFAULT '[]'::jsonb,
    est_kcal           INTEGER,
    est_protein_g      INTEGER,
    UNIQUE (item_id, member_id)
);


CREATE TABLE IF NOT EXISTS grocery_lists (
    list_id      SERIAL PRIMARY KEY,
    plan_id      INTEGER NOT NULL REFERENCES meal_plans(plan_id) ON DELETE CASCADE,
    total_eur    NUMERIC(9, 2),
    -- {"receipt": 0.61, "lidl_scrape": 0.24, "manual_survey": 0.15}
    provenance   JSONB,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (plan_id)
);

CREATE TABLE IF NOT EXISTS grocery_items (
    gitem_id       SERIAL PRIMARY KEY,
    list_id        INTEGER NOT NULL REFERENCES grocery_lists(list_id) ON DELETE CASCADE,
    ingredient_id  INTEGER REFERENCES ingredients(ingredient_id),
    display_name   TEXT NOT NULL,
    quantity       NUMERIC(10, 3),
    unit           TEXT,
    store_id       INTEGER REFERENCES stores(store_id),
    est_price_eur  NUMERIC(8, 2),
    price_source   TEXT,
    is_checked     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_grocery_items_list ON grocery_items(list_id);


-- ---------------------------------------------------------------------------
-- The behaviour loop: what was actually cooked, versus what was planned.
--
-- Enable Lakebase CDF on this table (REPLICA IDENTITY FULL) so the Stage 3
-- Spark job can mine lb_cooking_log_history for deviation patterns.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cooking_log (
    log_id            SERIAL PRIMARY KEY,
    household_id      INTEGER NOT NULL REFERENCES households(household_id) ON DELETE CASCADE,
    cooked_date       DATE NOT NULL,
    planned_recipe_id INTEGER REFERENCES recipes(recipe_id),
    actual_recipe_id  INTEGER REFERENCES recipes(recipe_id),
    actual_freetext   TEXT,                   -- when it wasn't a known recipe at all
    was_planned       BOOLEAN NOT NULL DEFAULT TRUE,
    deviation_reason  TEXT,                   -- free text -> embedded in Stage 2
    mood_note         TEXT,                   -- free text -> embedded in Stage 2
    rating            INTEGER CHECK (rating BETWEEN 1 AND 5),
    cook_minutes      INTEGER,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (household_id, cooked_date)
);

CREATE INDEX IF NOT EXISTS idx_cooking_log_household_date
    ON cooking_log(household_id, cooked_date DESC);
