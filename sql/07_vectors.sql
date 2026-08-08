-- =============================================================================
-- 07_vectors.sql - pgvector tables for semantic search (Stage 2)
--
-- Embedding model: sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
--   - 384 dimensions (same as the course's all-MiniLM-L6-v2, so the schema is
--     unchanged), but multilingual.
--   - This matters here specifically: the catalogue is Finnish, the recipes are
--     Indonesian and English, and the queries are English. An English-only
--     model embeds "Basmatiriisi" and "ayam bakar" as near-noise, so "chicken"
--     would never retrieve "broileri".
--
-- If you change models, change EMBEDDING_DIM everywhere - a VECTOR(n) column
-- rejects any vector of a different length.
--
-- Safe to re-run.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS vector;


-- ---------------------------------------------------------------------------
-- Ingredients: lets "chicken" find "Broilerin fileesuikale", and gives the
-- receipt matcher a fallback for lines plain string matching can't resolve.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingredient_embeddings (
    ingredient_id INTEGER PRIMARY KEY
                  REFERENCES ingredients(ingredient_id) ON DELETE CASCADE,
    -- the exact text that was embedded, kept so you can see what the vector
    -- actually represents when a search result looks wrong
    source_text   TEXT NOT NULL,
    embedding     VECTOR(384) NOT NULL,
    model_name    TEXT NOT NULL,
    embedded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ingredient_embeddings_vec
    ON ingredient_embeddings USING hnsw (embedding vector_cosine_ops);


-- ---------------------------------------------------------------------------
-- Recipes: the main retrieval surface. Embeds title + cuisine + ingredient
-- list + instructions, so a free-text weekly note can find real matches.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recipe_embeddings (
    recipe_id   INTEGER PRIMARY KEY
                REFERENCES recipes(recipe_id) ON DELETE CASCADE,
    source_text TEXT NOT NULL,
    embedding   VECTOR(384) NOT NULL,
    model_name  TEXT NOT NULL,
    embedded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recipe_embeddings_vec
    ON recipe_embeddings USING hnsw (embedding vector_cosine_ops);


-- ---------------------------------------------------------------------------
-- Recipe step chunks. A long video transcript blows past the model's context
-- window as one blob, so instructions are chunked and embedded separately -
-- this is what makes "which step do I add the coconut milk?" answerable, and
-- it carries the timestamp for jumping into the video at the right moment.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recipe_chunk_embeddings (
    chunk_id     SERIAL PRIMARY KEY,
    recipe_id    INTEGER NOT NULL REFERENCES recipes(recipe_id) ON DELETE CASCADE,
    chunk_index  INTEGER NOT NULL,
    chunk_text   TEXT NOT NULL,
    start_second INTEGER,
    embedding    VECTOR(384) NOT NULL,
    model_name   TEXT NOT NULL,
    embedded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (recipe_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_recipe_chunk_embeddings_vec
    ON recipe_chunk_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_recipe_chunk_recipe
    ON recipe_chunk_embeddings (recipe_id);


-- ---------------------------------------------------------------------------
-- Cooking history. The free-text deviation reasons and mood notes are what
-- make "last three times you planned soto ayam on a weekday you swapped it for
-- something faster" answerable in Stage 3.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cooking_log_embeddings (
    log_id      INTEGER PRIMARY KEY REFERENCES cooking_log(log_id) ON DELETE CASCADE,
    source_text TEXT NOT NULL,
    embedding   VECTOR(384) NOT NULL,
    model_name  TEXT NOT NULL,
    embedded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cooking_log_embeddings_vec
    ON cooking_log_embeddings USING hnsw (embedding vector_cosine_ops);


-- ---------------------------------------------------------------------------
-- What still needs embedding. The notebooks read this instead of re-embedding
-- everything on every run - re-embedding 8,500 ingredients each time would be
-- slow and pointless.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ingredients_needing_embedding AS
SELECT i.ingredient_id, i.canonical_name, i.name_fi, i.name_en, i.category_en
FROM ingredients i
LEFT JOIN ingredient_embeddings e ON e.ingredient_id = i.ingredient_id
WHERE e.ingredient_id IS NULL;

CREATE OR REPLACE VIEW recipes_needing_embedding AS
SELECT r.recipe_id, r.title, r.cuisine, r.description, r.instructions
FROM recipes r
LEFT JOIN recipe_embeddings e ON e.recipe_id = r.recipe_id
WHERE e.recipe_id IS NULL;
