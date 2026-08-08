"""
Recipe units -> grams, so a text ingredient line can be costed and counted.

The catalogue stores nutrition per 100g and prices per kg/litre/piece. Recipes
say "2 sdm kecap manis" or "1 ekor ayam". Bridging those is unavoidably
approximate, and this module's main job is to be honest about which numbers are
solid and which are estimates.

Three outcomes, never a silent guess:
  exact       - g, kg. Real weights.
  approximate - ml, tbsp, tsp, cup. Volume assumed water-like at 1 ml = 1 g,
                and spoon sizes are nominal. Fine for a kcal estimate, wrong
                for baking.
  unknown     - piece, clove, ekor, "secukupnya". Returns None; the caller
                reports these as uncovered rather than treating them as zero.

Treating unknowns as zero is the specific failure worth avoiding: a recipe
whose only protein is "1 ekor ayam" (one whole chicken) would otherwise report
~200 kcal and look like a cutting meal.
"""

from __future__ import annotations

# Real mass units.
EXACT_GRAMS = {
    "g": 1.0, "gr": 1.0, "gram": 1.0, "grams": 1.0, "gramme": 1.0,
    "kg": 1000.0, "kilo": 1000.0, "kilogram": 1000.0, "kilograms": 1000.0,
    "mg": 0.001,
    "oz": 28.35, "ounce": 28.35, "ounces": 28.35,
    "lb": 453.6, "pound": 453.6, "pounds": 453.6,
}

# Volume and spoon measures. Converted at water density, which is close enough
# for stock, milk and coconut milk, and overstates oil by ~8%.
APPROX_GRAMS = {
    "ml": 1.0, "milliliter": 1.0, "millilitre": 1.0, "cc": 1.0,
    "l": 1000.0, "liter": 1000.0, "litre": 1000.0, "dl": 100.0, "cl": 10.0,
    "tbsp": 15.0, "tablespoon": 15.0, "tablespoons": 15.0, "sdm": 15.0,
    "tsp": 5.0, "teaspoon": 5.0, "teaspoons": 5.0, "sdt": 5.0,
    "cup": 240.0, "cups": 240.0, "gelas": 240.0,
    "pinch": 0.4, "sejumput": 0.4,
}

# Recognised but not convertible without knowing the ingredient. Listed
# explicitly so "piece" is reported as unconvertible rather than unrecognised -
# the two need different messages in the UI.
COUNT_UNITS = {
    "piece", "pieces", "pcs", "buah", "biji", "butir",
    "clove", "cloves", "siung",
    "ekor", "whole", "head", "bunch", "ikat", "batang", "lembar", "leaf",
    "slice", "slices", "lembar", "can", "cans", "kaleng", "pack", "sachet",
    "secukupnya", "to taste", "sdt/sdm",
}


def to_grams(quantity, unit) -> tuple[float | None, str]:
    """Convert a recipe quantity to grams.

    Returns (grams, quality) where quality is 'exact', 'approximate',
    'count' (a countable unit we can't weigh) or 'unknown'. grams is None
    whenever the conversion isn't possible - never 0.0, which would silently
    read as "this ingredient contributes nothing".
    """
    if quantity is None:
        return None, "unknown"
    try:
        qty = float(quantity)
    except (TypeError, ValueError):
        return None, "unknown"
    if qty <= 0:
        return None, "unknown"

    key = (unit or "").strip().lower().rstrip(".")

    # A bare number with no unit is almost always a count ("2 eggs"), not grams.
    if not key:
        return None, "count"
    if key in EXACT_GRAMS:
        return qty * EXACT_GRAMS[key], "exact"
    if key in APPROX_GRAMS:
        return qty * APPROX_GRAMS[key], "approximate"
    if key in COUNT_UNITS:
        return None, "count"
    return None, "unknown"


def price_for_grams(grams: float | None, unit_price_eur, unit_basis: str | None):
    """Cost of `grams` of an ingredient, from its normalised catalogue price.

    Returns None when it can't be computed honestly - a per-piece price says
    nothing about what 250g costs, so that case is dropped rather than guessed.
    """
    if grams is None or unit_price_eur is None:
        return None
    try:
        price = float(unit_price_eur)
    except (TypeError, ValueError):
        return None

    if unit_basis == "kg":
        return price * (grams / 1000.0)
    if unit_basis == "l":
        # Same water-density assumption as the volume conversions above.
        return price * (grams / 1000.0)
    # 'piece' or missing basis: not convertible from a weight.
    return None
