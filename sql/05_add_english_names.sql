-- =============================================================================
-- 05_add_english_names.sql
--
-- Adds English-language columns to `ingredients`.
--
-- Why: the catalogue is Finnish, and a Finnish product name is unreadable if
-- you don't speak Finnish. Open Food Facts carries an English product name for
-- roughly half the catalogue, and its category tags are always English
-- regardless of the product's language - so "Grillattu broileri" still yields
-- "roast chicken", which is often the only clue to what a product actually is.
--
-- Safe to re-run.
-- =============================================================================

ALTER TABLE ingredients ADD COLUMN IF NOT EXISTS name_en     TEXT;
ALTER TABLE ingredients ADD COLUMN IF NOT EXISTS category_en TEXT;

-- Backfill category_en for rows already loaded: strip the "en:" language
-- prefix and turn hyphens into spaces ("en:roast-chicken" -> "roast chicken").
UPDATE ingredients
SET category_en = replace(regexp_replace(category, '^[a-z]{2}:', ''), '-', ' ')
WHERE category IS NOT NULL AND category_en IS NULL;

-- Searching by the English name should be as fast as by the Finnish one.
CREATE INDEX IF NOT EXISTS idx_ingredients_name_en
    ON ingredients (lower(name_en));
