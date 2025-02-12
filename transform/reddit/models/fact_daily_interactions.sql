with interactions AS(
    SELECT
        user_id,
        interaction_type,
        DATE_TRUNC('day', created_at) AS interaction_date,
        COUNT(*) AS interaction_count,
        SUM(score) AS total_score
    FROM {{ ref('fact_user_interactions') }}
    GROUP BY 1, 2, 3
)
SELECT
    interaction_date,
    user_id,
    interaction_type,
    interaction_count,
    total_score,
    CONCAT(interaction_date, '_', user_id, '_', interaction_type) AS date_user_type
FROM interactions