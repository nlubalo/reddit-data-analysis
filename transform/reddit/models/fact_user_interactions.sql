with posts as (
    select p.post_id as interaction_id,
           p.user_id,
           p.subreddit_id,
           p.post_id,
           NULL AS comment_id,
          'post' as interaction_type,
           p.created_at,
           p.score
    from {{ ref('dim_posts') }} p
),
comments as (
    select c.comment_id as interaction_id,
           c.user_id,
           c.subreddit_id,
           c.post_id,
           c.comment_id,
           'comment' as interaction_type,
           c.created_at,
           c.score
    FROM {{ ref('dim_comments') }} c
    JOIN {{ ref('dim_posts') }} p ON c.post_id = p.post_id


),
comment_replies as (
    select r.reply_id as interaction_id,
           r.user_id,
           r.subreddit_id,
           r.post_id,
           r.parent_comment_id as comment_id,
           'reply' AS interaction_type,
           r.created_at,
           r.score
    FROM {{ ref('dim_comment_replies') }} r
    JOIN {{ ref('dim_posts') }} p ON r.post_id = p.post_id
)

SELECT * FROM posts
UNION ALL
SELECT * FROM comments
UNION ALL
SELECT * FROM comment_replies