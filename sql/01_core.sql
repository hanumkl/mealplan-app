-- =============================================================================
-- 01_core.sql - Households, members, goals, restrictions, weekly preferences
-- Run once against your Lakebase Postgres database, in file-number order.
-- =============================================================================

CREATE TABLE IF NOT EXISTS households (
    household_id  SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    city          TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS members (
    member_id      SERIAL PRIMARY KEY,
    household_id   INTEGER NOT NULL REFERENCES households(household_id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    -- role is descriptive only; targets come from member_goals
    role           TEXT CHECK (role IN ('adult', 'teen', 'child')) DEFAULT 'adult',
    birth_year     INTEGER,
    sex            TEXT CHECK (sex IN ('male', 'female', 'unspecified')) DEFAULT 'unspecified',
    weight_kg      NUMERIC(5, 1),
    height_cm      NUMERIC(5, 1),
    activity_level TEXT CHECK (activity_level IN
                       ('sedentary', 'light', 'moderate', 'active', 'very_active'))
                   DEFAULT 'moderate',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_members_household ON members(household_id);


-- Daily macro targets. One active row per member; older rows are kept as history
-- so the Stage 3 behaviour job can see how targets changed over time.
CREATE TABLE IF NOT EXISTS member_goals (
    goal_id          SERIAL PRIMARY KEY,
    member_id        INTEGER NOT NULL REFERENCES members(member_id) ON DELETE CASCADE,
    goal_type        TEXT NOT NULL CHECK (goal_type IN
                         ('bulking', 'cutting', 'maintain', 'growth')),
    target_kcal      INTEGER,
    target_protein_g INTEGER,
    target_carb_g    INTEGER,
    target_fat_g     INTEGER,
    -- 'calculated' = derived by the app from height/weight/activity,
    -- 'manual'     = the user overrode it
    target_source    TEXT NOT NULL DEFAULT 'manual'
                     CHECK (target_source IN ('calculated', 'manual')),
    effective_from   DATE NOT NULL DEFAULT CURRENT_DATE,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_member_goals_active
    ON member_goals(member_id) WHERE is_active;


-- Dietary restrictions.
--
-- severity matters to the agent: 'strict' constraints must never be violated
-- (halal, coeliac, allergy), 'preference' ones are soft and can be traded off.
CREATE TABLE IF NOT EXISTS member_restrictions (
    restriction_id SERIAL PRIMARY KEY,
    member_id      INTEGER NOT NULL REFERENCES members(member_id) ON DELETE CASCADE,
    restriction    TEXT NOT NULL CHECK (restriction IN (
                       'halal', 'vegetarian', 'vegan', 'no_pork', 'no_alcohol',
                       'lactose_free', 'gluten_free', 'nut_allergy',
                       'shellfish_allergy', 'egg_allergy', 'low_spice')),
    severity       TEXT NOT NULL DEFAULT 'strict'
                   CHECK (severity IN ('strict', 'preference')),
    note           TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (member_id, restriction)
);

CREATE INDEX IF NOT EXISTS idx_member_restrictions_member
    ON member_restrictions(member_id);


-- Free-text "what do we feel like this week" note.
-- Stage 2 embeds free_text into pgvector and uses it as the retrieval query.
CREATE TABLE IF NOT EXISTS weekly_preferences (
    pref_id      SERIAL PRIMARY KEY,
    household_id INTEGER NOT NULL REFERENCES households(household_id) ON DELETE CASCADE,
    week_start   DATE NOT NULL,
    free_text    TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (household_id, week_start)
);
