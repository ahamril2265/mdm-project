INSERT INTO gold.match_confidence_metrics
SELECT
    CURRENT_DATE,
    COUNT(*),
    COUNT(*) FILTER (WHERE decision = 'AUTO_MERGE'),
    COUNT(*) FILTER (WHERE decision = 'FLAG_REVIEW'),
    AVG(confidence_score)
FROM identity.customer_identity_map;
