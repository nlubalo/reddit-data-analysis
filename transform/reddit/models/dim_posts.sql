with source as (
    select * from {{source('reddit', 'reddit_posts')}}
),
transformed as (
    select
        post_id,
        author_id as user_id,
        subreddit_id,
        title,
        subreddit_type,
        upvote_ratio,
        num_comments,
        score,
        TO_TIMESTAMP(created_at) as created_at
    from source
)
select * from transformed