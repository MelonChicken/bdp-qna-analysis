-- @view first_answers
SELECT q.site, q.id,
 MIN(CASE WHEN a.creation_date >= q.creation_date
          THEN a.creation_date - q.creation_date END) AS first_answer_delay
FROM posts q LEFT JOIN posts a
 ON q.site = a.site AND q.id = a.parent_id AND a.post_type_id = 2
WHERE q.post_type_id = 1
GROUP BY q.site, q.id
-- @view q_month
SELECT p.site, p.year, p.month, COUNT(*) AS questions_count,
 SUM(CASE WHEN f.first_answer_delay <= 604800 THEN 1 ELSE 0 END) AS answered_questions_count
FROM posts p JOIN first_answers f ON p.site=f.site AND p.id=f.id
WHERE p.post_type_id=1 GROUP BY p.site,p.year,p.month
-- @view u_month
SELECT site,year,month,COUNT(*) AS new_users_count FROM users
WHERE id <> -1 GROUP BY site,year,month
-- @view v_month
SELECT site,year,month,
 SUM(CASE WHEN vote_type_id=2 THEN 1 ELSE 0 END) AS upvotes_count,
 SUM(CASE WHEN vote_type_id=3 THEN 1 ELSE 0 END) AS downvotes_count
FROM votes GROUP BY site,year,month
-- @view metric_base
SELECT c.site,c.domain_group,c.is_meta,c.parent_site,c.month_key AS month,
 c.complete_month,c.answer_mature,
 COALESCE(q.questions_count,CAST(0 AS BIGINT)) AS questions_count,
 COALESCE(q.answered_questions_count,CAST(0 AS BIGINT)) AS answered_questions_count,
 COALESCE(u.new_users_count,CAST(0 AS BIGINT)) AS new_users_count,
 COALESCE(v.upvotes_count,CAST(0 AS BIGINT)) AS upvotes_count,
 COALESCE(v.downvotes_count,CAST(0 AS BIGINT)) AS downvotes_count
FROM analysis_calendar c
LEFT JOIN q_month q ON q.site=c.site AND q.year=c.year AND q.month=c.month_number
LEFT JOIN u_month u ON u.site=c.site AND u.year=c.year AND u.month=c.month_number
LEFT JOIN v_month v ON v.site=c.site AND v.year=c.year AND v.month=c.month_number
-- @view monthly_audit
SELECT site,month,domain_group,is_meta,parent_site,complete_month,answer_mature,
 CASE WHEN complete_month THEN questions_count END AS questions_count,
 CASE WHEN complete_month THEN new_users_count END AS new_users_count,
 CASE WHEN answer_mature AND questions_count > 0
 THEN CAST(answered_questions_count AS DOUBLE)/questions_count END AS answered_rate,
 CASE WHEN complete_month AND upvotes_count+downvotes_count > 0
 THEN CAST(downvotes_count AS DOUBLE)/(upvotes_count+downvotes_count) END AS downvote_ratio,
 CASE WHEN answer_mature THEN answered_questions_count END AS answered_questions_count,
 CASE WHEN complete_month THEN upvotes_count END AS upvotes_count,
 CASE WHEN complete_month THEN downvotes_count END AS downvotes_count
FROM metric_base
