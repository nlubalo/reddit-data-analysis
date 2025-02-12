SELECT
	DATE_TRUNC('week', interaction_date) as week_start_date,
	user_id,
	interaction_type,
	SUM(interaction_count) as weekly_interaction_count,
	SUM(total_score) as weekly_score_count
FROM 
  reddit.main.fact_daily_interactions
GROUP BY 
  ALL
ORDER BY 
  week_start_date