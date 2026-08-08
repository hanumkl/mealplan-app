"""
Daily macro target calculation.

Uses Mifflin-St Jeor for BMR, an activity multiplier for TDEE, then a goal
adjustment. Protein is set per kg of bodyweight (the part that actually matters
for a bulking member), fat is a fixed share of calories, and carbohydrate takes
whatever calories are left.

Caveat worth keeping visible in the UI: Mifflin-St Jeor is validated for
adults, not children. Targets for a child are rough guidance only - the app
labels them as such rather than pretending otherwise.
"""

from datetime import date

ACTIVITY_FACTORS = {
    "sedentary": 1.20,
    "light": 1.375,
    "moderate": 1.55,
    "active": 1.725,
    "very_active": 1.90,
}

# kcal adjustment applied to TDEE, and protein grams per kg bodyweight
GOAL_SETTINGS = {
    "bulking": {"kcal_factor": 1.15, "protein_g_per_kg": 2.0, "fat_pct": 0.25},
    "cutting": {"kcal_factor": 0.80, "protein_g_per_kg": 2.2, "fat_pct": 0.25},
    "maintain": {"kcal_factor": 1.00, "protein_g_per_kg": 1.6, "fat_pct": 0.30},
    "growth": {"kcal_factor": 1.00, "protein_g_per_kg": 1.5, "fat_pct": 0.30},
}


def calculate_targets(
    *,
    weight_kg: float,
    height_cm: float,
    birth_year: int,
    sex: str,
    activity_level: str,
    goal_type: str,
) -> dict:
    """Return {target_kcal, target_protein_g, target_carb_g, target_fat_g, ...}.

    Raises ValueError if the inputs needed for the calculation are missing.
    """
    if not weight_kg or not height_cm or not birth_year:
        raise ValueError("weight_kg, height_cm and birth_year are all required")

    settings = GOAL_SETTINGS.get(goal_type)
    if settings is None:
        raise ValueError(f"unknown goal_type: {goal_type}")

    age = max(1, date.today().year - int(birth_year))
    weight_kg = float(weight_kg)
    height_cm = float(height_cm)

    # Mifflin-St Jeor
    bmr = (10 * weight_kg) + (6.25 * height_cm) - (5 * age)
    if sex == "male":
        bmr += 5
    elif sex == "female":
        bmr -= 161
    else:
        bmr -= 78  # midpoint when sex is unspecified

    tdee = bmr * ACTIVITY_FACTORS.get(activity_level, 1.55)
    kcal = tdee * settings["kcal_factor"]

    protein_g = weight_kg * settings["protein_g_per_kg"]
    fat_g = (kcal * settings["fat_pct"]) / 9.0
    carb_kcal = kcal - (protein_g * 4) - (fat_g * 9)
    carb_g = max(0.0, carb_kcal / 4.0)

    return {
        "target_kcal": int(round(kcal)),
        "target_protein_g": int(round(protein_g)),
        "target_carb_g": int(round(carb_g)),
        "target_fat_g": int(round(fat_g)),
        "bmr": int(round(bmr)),
        "tdee": int(round(tdee)),
        "age": age,
        "is_estimate_only": age < 18,
    }


def scale_quantity(quantity: float, factor: float, scaling_class: str) -> float:
    """Scale one recipe ingredient.

    Not everything scales linearly: tripling a recipe and tripling the chilli
    makes it inedible, so spices/salt/oil scale by factor**0.8 and things like
    a bay leaf don't scale at all.
    """
    if quantity is None:
        return None
    if scaling_class == "fixed":
        return float(quantity)
    if scaling_class == "sublinear":
        return float(quantity) * (factor ** 0.8)
    return float(quantity) * factor
