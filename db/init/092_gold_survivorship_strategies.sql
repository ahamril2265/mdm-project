CREATE TABLE IF NOT EXISTS gold.survivorship_strategies (
    attribute_name TEXT PRIMARY KEY,
    strategy TEXT NOT NULL
    -- examples: 'priority', 'most_frequent', 'most_recent'
);

INSERT INTO gold.survivorship_strategies VALUES
('email', 'priority'),
('phone', 'priority'),
('name', 'most_frequent')
ON CONFLICT DO NOTHING;
