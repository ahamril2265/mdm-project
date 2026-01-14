CREATE TABLE IF NOT EXISTS gold.survivorship_strategies (
    attribute_name TEXT PRIMARY KEY,
    strategy TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.survivorship_rules (
    attribute_name TEXT NOT NULL,
    source_system TEXT NOT NULL,
    priority_rank INT NOT NULL,
    PRIMARY KEY (attribute_name, source_system)
);

INSERT INTO gold.survivorship_strategies VALUES
('email','priority'),
('phone','priority'),
('name','most_frequent')
ON CONFLICT DO NOTHING;

INSERT INTO gold.survivorship_rules VALUES
('email','sales',1),
('email','support',2),
('email','marketing',3),
('phone','marketing',1),
('phone','sales',2)
ON CONFLICT DO NOTHING;
