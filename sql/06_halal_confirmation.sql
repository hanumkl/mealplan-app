-- =============================================================================
-- 06_halal_confirmation.sql
--
-- Lets the household confirm or override a product's halal status by hand.
--
-- Why this exists: 62% of Finnish Open Food Facts records carry no labels, no
-- ingredient analysis and no category, so no derivation rule can classify them
-- honestly - they sit at 'unknown' forever. For a halal filter the family's own
-- judgment should be the authority anyway, not a heuristic over crowd-sourced
-- data. A confirmation here outranks anything the pipeline derives.
--
-- Safe to re-run.
-- =============================================================================

ALTER TABLE ingredients
    ADD COLUMN IF NOT EXISTS halal_source TEXT NOT NULL DEFAULT 'derived';

ALTER TABLE ingredients
    ADD COLUMN IF NOT EXISTS halal_confirmed_at TIMESTAMPTZ;

ALTER TABLE ingredients
    ADD COLUMN IF NOT EXISTS halal_note TEXT;

-- Added separately so re-running the script doesn't fail on an existing
-- constraint (Postgres has no ADD CONSTRAINT IF NOT EXISTS).
DO $$
BEGIN
    ALTER TABLE ingredients
        ADD CONSTRAINT ingredients_halal_source_check
        CHECK (halal_source IN ('derived', 'user_confirmed'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- The agent will filter on this constantly once meal planning starts.
CREATE INDEX IF NOT EXISTS idx_ingredients_halal
    ON ingredients (halal_status, halal_source);

-- Convenience view: what still needs a human decision, most-used first once
-- there's purchase history to order by.
CREATE OR REPLACE VIEW ingredients_needing_halal_review AS
SELECT ingredient_id,
       canonical_name,
       name_en,
       category_en,
       halal_status,
       halal_reason
FROM ingredients
WHERE halal_source = 'derived'
  AND halal_status = 'unknown'
ORDER BY canonical_name;
