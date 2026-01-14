-- ======================================
-- SURVIVORSHIP CONFIGURATION
-- ======================================

CREATE TABLE IF NOT EXISTS gold.survivorship_rules (
    attribute_name TEXT NOT NULL,
    source_system  TEXT NOT NULL,
    priority_rank  INT  NOT NULL,
    PRIMARY KEY (attribute_name, source_system)
);

CREATE TABLE IF NOT EXISTS gold.survivorship_strategies (
    attribute_name TEXT PRIMARY KEY,
    strategy TEXT NOT NULL
);

-- --------------------------------------
-- STRATEGIES
-- --------------------------------------

INSERT INTO gold.survivorship_strategies (attribute_name, strategy)
VALUES
    ('email', 'priority'),
    ('phone', 'priority'),
    ('name',  'most_frequent')
ON CONFLICT DO NOTHING;

-- --------------------------------------
-- EMAIL PRIORITY
-- --------------------------------------

INSERT INTO gold.survivorship_rules
(attribute_name, source_system, priority_rank)
VALUES
    ('email', 'sales',     1),
    ('email', 'support',   2),
    ('email', 'marketing', 3)
ON CONFLICT DO NOTHING;

-- --------------------------------------
-- PHONE PRIORITY
-- --------------------------------------

INSERT INTO gold.survivorship_rules
(attribute_name, source_system, priority_rank)
VALUES
    ('phone', 'marketing', 1),
    ('phone', 'sales',     2)
ON CONFLICT DO NOTHING;
