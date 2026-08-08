-- =============================================================================
-- 02_catalog.sql - Stores, ingredients, prices, and the raw landing tables
--                  fed by the Spark notebooks.
--
-- Design note: every price row carries `source` + `captured_at` + `confidence`.
-- The app never shows a total without being able to say where it came from.
-- =============================================================================

CREATE TABLE IF NOT EXISTS stores (
    store_id        SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    chain           TEXT,
    city            TEXT,
    halal_certified BOOLEAN NOT NULL DEFAULT FALSE,
    notes           TEXT
);

INSERT INTO stores (name, chain, city, halal_certified, notes) VALUES
    ('Prisma',  'S-ryhma', 'Helsinki', FALSE, 'Main weekly shop'),
    ('S-market','S-ryhma', 'Helsinki', FALSE, NULL),
    ('Lidl',    'Lidl',    'Helsinki', FALSE, 'Cheaper staples; open product sitemap'),
    ('K-Market','Kesko',   'Helsinki', FALSE, NULL),
    ('Alanya',  'Alanya',  'Helsinki', TRUE,  'Halal butcher - source all meat here')
ON CONFLICT (name) DO NOTHING;


-- The canonical ingredient list the whole app plans against.
--
-- scaling_class drives the recipe scaler:
--   linear    - proteins, vegetables, rice, liquids  -> scale 1:1
--   sublinear - salt, chilli, strong spices, oil     -> scale by factor^0.8
--   fixed     - bay leaf, pandan leaf, a splash of X -> do not scale
CREATE TABLE IF NOT EXISTS ingredients (
    ingredient_id       SERIAL PRIMARY KEY,
    canonical_name      TEXT NOT NULL UNIQUE,
    name_fi             TEXT,
    name_id             TEXT,              -- Indonesian, for YouTube recipe matching
    category            TEXT,
    default_unit        TEXT NOT NULL DEFAULT 'g',
    scaling_class       TEXT NOT NULL DEFAULT 'linear'
                        CHECK (scaling_class IN ('linear', 'sublinear', 'fixed')),
    is_protein_source   BOOLEAN NOT NULL DEFAULT FALSE,

    kcal_per_100g       NUMERIC(7, 2),
    protein_g_per_100g  NUMERIC(6, 2),
    carb_g_per_100g     NUMERIC(6, 2),
    fat_g_per_100g      NUMERIC(6, 2),

    is_vegetarian       BOOLEAN,
    is_vegan            BOOLEAN,
    contains_pork       BOOLEAN,
    contains_alcohol    BOOLEAN,
    contains_gluten     BOOLEAN,
    contains_lactose    BOOLEAN,
    contains_nuts       BOOLEAN,

    -- Deliberately four-valued, never a bare boolean. The UI shows the reason
    -- and tells the user to verify packaging for anything below 'certified'.
    halal_status        TEXT NOT NULL DEFAULT 'unknown'
                        CHECK (halal_status IN
                            ('certified', 'likely_ok', 'contains_flagged', 'unknown')),
    halal_reason        TEXT,

    off_code            TEXT,              -- Open Food Facts barcode, if matched
    source              TEXT NOT NULL DEFAULT 'manual'
                        CHECK (source IN ('openfoodfacts', 'receipt', 'lidl', 'manual')),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ingredients_name
    ON ingredients USING gin (to_tsvector('simple', canonical_name));
CREATE INDEX IF NOT EXISTS idx_ingredients_category ON ingredients(category);


CREATE TABLE IF NOT EXISTS ingredient_prices (
    price_id       SERIAL PRIMARY KEY,
    ingredient_id  INTEGER NOT NULL REFERENCES ingredients(ingredient_id) ON DELETE CASCADE,
    store_id       INTEGER REFERENCES stores(store_id),
    price_eur      NUMERIC(8, 2) NOT NULL,
    quantity       NUMERIC(10, 3),           -- pack size the price refers to
    unit           TEXT,
    unit_price_eur NUMERIC(10, 4),           -- normalised EUR per kg / per litre / per piece
    unit_basis     TEXT CHECK (unit_basis IN ('kg', 'l', 'piece')),

    -- Provenance. This is the point of the table.
    source         TEXT NOT NULL CHECK (source IN
                       ('receipt', 'lidl_scrape', 'open_prices', 'manual_survey')),
    source_ref     TEXT,                     -- receipt id, product url, ...
    confidence     NUMERIC(3, 2) NOT NULL DEFAULT 1.00,
    captured_at    DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_prices_ingredient ON ingredient_prices(ingredient_id);
CREATE INDEX IF NOT EXISTS idx_prices_captured ON ingredient_prices(captured_at DESC);


-- Most recent price per ingredient/store, which is what the grocery list uses.
CREATE OR REPLACE VIEW latest_ingredient_prices AS
SELECT DISTINCT ON (p.ingredient_id, p.store_id)
       p.ingredient_id,
       i.canonical_name,
       p.store_id,
       s.name AS store_name,
       p.price_eur,
       p.unit_price_eur,
       p.unit_basis,
       p.source,
       p.confidence,
       p.captured_at
FROM ingredient_prices p
JOIN ingredients i ON i.ingredient_id = p.ingredient_id
LEFT JOIN stores s ON s.store_id = p.store_id
ORDER BY p.ingredient_id, p.store_id, p.captured_at DESC, p.price_id DESC;


-- ---------------------------------------------------------------------------
-- Raw landing tables. Written by the Spark notebooks, never edited by the app.
-- Keeping the untouched payload means re-parsing never needs a re-fetch.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_off_products (
    off_code   TEXT PRIMARY KEY,
    payload    JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_receipts (
    receipt_id            SERIAL PRIMARY KEY,
    image_path            TEXT NOT NULL,
    store_hint            TEXT,
    purchased_on          DATE,
    total_eur             NUMERIC(8, 2),
    payload               JSONB,             -- full vision-model response
    extraction_status     TEXT NOT NULL DEFAULT 'pending'
                          CHECK (extraction_status IN
                              ('pending', 'extracted', 'failed', 'reviewed')),
    extraction_confidence NUMERIC(3, 2),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS receipt_line_items (
    line_id       SERIAL PRIMARY KEY,
    receipt_id    INTEGER NOT NULL REFERENCES raw_receipts(receipt_id) ON DELETE CASCADE,
    raw_text      TEXT NOT NULL,             -- exactly as printed, e.g. "BROILERIN FILE 700G"
    ingredient_id INTEGER REFERENCES ingredients(ingredient_id),
    quantity      NUMERIC(10, 3),
    unit          TEXT,
    price_eur     NUMERIC(8, 2),
    confidence    NUMERIC(3, 2),
    match_status  TEXT NOT NULL DEFAULT 'unmatched'
                  CHECK (match_status IN ('unmatched', 'auto', 'confirmed', 'rejected'))
);

CREATE INDEX IF NOT EXISTS idx_receipt_lines_receipt ON receipt_line_items(receipt_id);
