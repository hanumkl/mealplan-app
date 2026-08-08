"""
Query-side embedding for semantic search (Stage 2).

The notebooks embed the *content* (ingredients, recipes, recipe steps); this
module embeds the *query* the user types, so both land in the same vector space
and pgvector's `<=>` can compare them.

THE ONE RULE
------------
Content and query must be embedded by the SAME model. Two models produce
vectors of the same length that mean completely different things - nothing
errors, the results are just silently meaningless. Never mix them, and never
reshape a vector to make the dimensions line up (see `_from_endpoint`).

TWO BACKENDS
------------
`local` (default) - sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2,
    384-dim, runs in-process. Chosen because the catalogue is Finnish, the
    recipes are Indonesian and the household types English: an English-only
    model embeds "Broilerin fileesuikale" and "ayam bakar" as near-noise, so
    "chicken" would never retrieve either. Needs torch (~800MB).

`databricks` - a Foundation Model serving endpoint. No torch, but the endpoints
    available on Free Edition (gte-large-en, bge-large-en) are English-only and
    1024-dim, so the vector tables must be rebuilt at VECTOR(1024) and Finnish
    retrieval gets noticeably worse.

To switch: set EMBEDDING_BACKEND=databricks and EMBEDDING_DIM=1024, change
VECTOR(384) to VECTOR(1024) in sql/07_vectors.sql, drop the embedding tables,
and re-run notebooks/embed_content.py with the same backend. All four steps or
none - a half-switched setup returns nonsense.
"""

from __future__ import annotations

import logging
import os
from typing import Sequence

logger = logging.getLogger("mealplan.embeddings")

BACKEND = os.environ.get("EMBEDDING_BACKEND", "local").strip().lower()

EMBEDDING_MODEL_NAME = os.environ.get(
    "EMBEDDING_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)
DATABRICKS_ENDPOINT = os.environ.get("DATABRICKS_EMBEDDING_ENDPOINT", "databricks-gte-large-en")

# Must match the VECTOR(n) columns in sql/07_vectors.sql.
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "384"))

# What gets written to / compared against `model_name` in the embedding tables.
ACTIVE_MODEL = EMBEDDING_MODEL_NAME if BACKEND == "local" else DATABRICKS_ENDPOINT

_model = None
_load_error: str | None = None


# ---------------------------------------------------------------- local ----

def _get_local_model():
    """Load the sentence-transformers model once."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        logger.info("Loading embedding model %s ...", EMBEDDING_MODEL_NAME)
        model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        dim = model.get_sentence_embedding_dimension()
        if dim != EMBEDDING_DIM:
            raise RuntimeError(
                f"{EMBEDDING_MODEL_NAME} produces {dim}-dim vectors but the schema "
                f"is VECTOR({EMBEDDING_DIM}). Fix EMBEDDING_DIM and the SQL together."
            )
        _model = model
        logger.info("Embedding model ready (%s-dim)", dim)
    return _model


def _from_local(text: str) -> list[float]:
    # normalize_embeddings=True matches notebooks/embed_content.py. Cosine
    # distance is scale-invariant so ranking is identical either way, but with
    # unit vectors `1 - (a <=> b)` is a true cosine similarity in [0, 1], which
    # is what the UI shows as a match percentage.
    vector = _get_local_model().encode(
        [text], batch_size=1, show_progress_bar=False, normalize_embeddings=True
    )[0]
    return vector.tolist()


# ----------------------------------------------------------- databricks ----

def _from_endpoint(text: str) -> list[float]:
    """Embed via a Databricks Foundation Model endpoint."""
    from databricks.sdk import WorkspaceClient

    response = WorkspaceClient().serving_endpoints.query(
        name=DATABRICKS_ENDPOINT, input=text
    )
    if not getattr(response, "data", None):
        raise RuntimeError(
            f"embedding endpoint '{DATABRICKS_ENDPOINT}' returned no data - "
            f"check the endpoint exists and is running"
        )
    vector = list(response.data[0].embedding)

    # Deliberately NOT truncated or zero-padded to fit. Embedding dimensions
    # aren't ranked by importance, so slicing one destroys its meaning and
    # padding invents coordinates - either way the vector still *inserts*
    # cleanly and every search result afterwards is quietly wrong. Better to
    # fail here, loudly, than to serve nonsense.
    if len(vector) != EMBEDDING_DIM:
        raise RuntimeError(
            f"'{DATABRICKS_ENDPOINT}' returns {len(vector)}-dim vectors but the "
            f"schema is VECTOR({EMBEDDING_DIM}). Rebuild the embedding tables at "
            f"VECTOR({len(vector)}) and set EMBEDDING_DIM={len(vector)}, or use a "
            f"different endpoint. Do not reshape the vector to fit."
        )
    return vector


# ------------------------------------------------------------- public ------

def available() -> bool:
    """True if semantic search can run. Lets the API fall back to keyword
    search instead of 500-ing when the backend is unusable."""
    global _load_error
    if _load_error is not None:
        return False
    try:
        if BACKEND == "local":
            _get_local_model()
        else:
            _from_endpoint("healthcheck")
        return True
    except Exception as exc:  # noqa: BLE001
        # Cache the failure: retrying a missing torch on every request costs
        # seconds each time and the answer won't change.
        _load_error = f"embedding backend '{BACKEND}' unavailable: {exc}"
        logger.warning(_load_error)
        return False


def load_error() -> str | None:
    return _load_error


def embed_query(query: str) -> list[float]:
    """Embed one query string with the active backend."""
    return _from_local(query) if BACKEND == "local" else _from_endpoint(query)


def vector_literal(embedding: Sequence[float]) -> str:
    """Format a Python list as a Postgres vector literal: '[v1,v2,...]'."""
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


def warm_model() -> bool:
    """Pre-load at startup so the first search isn't a 10-second cold start."""
    return available()
