-- =============================================================================
-- 08_recipe_matching.sql - link recipe ingredients to the priced catalogue.
--
-- `recipe_ingredients.ingredient_id` already exists but nothing ever filled it
-- in, so every recipe was a wall of text: no calories, no protein, no price.
-- These columns record HOW a link was made, so a bad match can be found and
-- corrected instead of quietly skewing a week's nutrition numbers.
--
-- Matching is semantic (pgvector over ingredient_embeddings), because the
-- recipe says "chicken thigh" and the catalogue says "Broilerin koipireisi".
-- No amount of string matching bridges that; this is the payoff for Stage 2.
--
-- Safe to re-run.
-- =============================================================================

-- The LLM already produces a clean English name per ingredient ("sweet soy
-- sauce") alongside the raw line ("2 sdm kecap manis"). Only the raw line was
-- being stored, which made matching much harder than it needed to be - the
-- quantity and Indonesian unit are noise in the embedding.
ALTER TABLE recipe_ingredients
    ADD COLUMN IF NOT EXISTS ingredient_name TEXT;

ALTER TABLE recipe_ingredients
    ADD COLUMN IF NOT EXISTS match_confidence NUMERIC(4, 3);

ALTER TABLE recipe_ingredients
    ADD COLUMN IF NOT EXISTS match_method TEXT NOT NULL DEFAULT 'none';

ALTER TABLE recipe_ingredients
    ADD COLUMN IF NOT EXISTS matched_at TIMESTAMPTZ;

DO $$
BEGIN
    ALTER TABLE recipe_ingredients
        ADD CONSTRAINT recipe_ingredients_match_method_check
        CHECK (match_method IN ('none', 'vector', 'manual'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_ingredient
    ON recipe_ingredients (ingredient_id);


-- ---------------------------------------------------------------------------
-- What still needs matching. A manual correction is never re-matched - the
-- household outranks the vector search, same rule as halal confirmation.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW recipe_ingredients_needing_match AS
SELECT ri.ri_id, ri.recipe_id, ri.raw_text, ri.ingredient_name,
       ri.unit, ri.quantity
FROM recipe_ingredients ri
WHERE ri.ingredient_id IS NULL
  AND ri.match_method <> 'manual';


-- ---------------------------------------------------------------------------
-- Per-recipe match coverage, so the app can say "nutrition covers 8 of 11
-- ingredients" instead of presenting a partial total as if it were complete.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW recipe_match_coverage AS
SELECT r.recipe_id,
       COUNT(ri.ri_id)                                        AS total_ingredients,
       COUNT(ri.ingredient_id)                                AS matched_ingredients,
       COUNT(*) FILTER (WHERE i.kcal_per_100g IS NOT NULL)    AS with_nutrition,
       ROUND(AVG(ri.match_confidence), 3)                     AS avg_match_confidence
FROM recipes r
LEFT JOIN recipe_ingredients ri ON ri.recipe_id = r.recipe_id
LEFT JOIN ingredients i ON i.ingredient_id = ri.ingredient_id
GROUP BY r.recipe_id;
