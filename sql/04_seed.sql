-- =============================================================================
-- 04_seed.sql - A starting household so the app has something to render.
-- Edit the names, weights and goals to match your family, then run once.
-- Safe to re-run: it does nothing if household_id 1 already exists.
-- =============================================================================

INSERT INTO households (household_id, name, city)
VALUES (1, 'Home', 'Helsinki')
ON CONFLICT (household_id) DO NOTHING;

-- Keep the sequence ahead of the explicit id above.
SELECT setval('households_household_id_seq',
              GREATEST((SELECT MAX(household_id) FROM households), 1));


INSERT INTO members (household_id, name, role, birth_year, sex,
                     weight_kg, height_cm, activity_level)
SELECT * FROM (VALUES
    (1, 'Husband', 'adult', 1993, 'male', 65.0, 173.0, 'active'),
    (1, 'Wife',    'adult', 1996, 'female', 58.0, 158.0, 'light'),
    (1, 'Child',   'child', 2021, 'female',   23.0, 105.0, 'active')
) AS v(household_id, name, role, birth_year, sex, weight_kg, height_cm, activity_level)
WHERE NOT EXISTS (SELECT 1 FROM members WHERE household_id = 1);


-- Halal applies to everyone in the household; the vegetarian preference on one
-- member is what triggers split-protein planning.
INSERT INTO member_restrictions (member_id, restriction, severity, note)
SELECT m.member_id, 'halal', 'strict', 'Meat sourced from Alanya'
FROM members m WHERE m.household_id = 1
ON CONFLICT (member_id, restriction) DO NOTHING;

INSERT INTO member_restrictions (member_id, restriction, severity, note)
SELECT m.member_id, 'lactose_free', 'preference', 'Mild - small amounts are fine'
FROM members m WHERE m.household_id = 1 AND m.name = 'Wife'
ON CONFLICT (member_id, restriction) DO NOTHING;

INSERT INTO member_restrictions (member_id, restriction, severity, note)
SELECT m.member_id, 'low_spice', 'preference', NULL
FROM members m WHERE m.household_id = 1 AND m.role = 'child'
ON CONFLICT (member_id, restriction) DO NOTHING;


-- Goals: leave targets NULL so you can generate them from the UI with the
-- "Calculate from body stats" button and see the calculator work.
INSERT INTO member_goals (member_id, goal_type, target_source)
SELECT m.member_id,
       CASE WHEN m.role = 'child' THEN 'growth'
            WHEN m.name = 'Husband' THEN 'bulking'
            WHEN m.name = 'Wife' THEN 'cutting' 
            ELSE 'maintain' END,
       'manual'
FROM members m
WHERE m.household_id = 1
  AND NOT EXISTS (SELECT 1 FROM member_goals g WHERE g.member_id = m.member_id);
