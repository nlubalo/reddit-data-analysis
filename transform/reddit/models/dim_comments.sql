with source as (
    select * from {{source('reddit', 'reddit_comments')}}
),
transformed as (
    select
        comment_id,
        submission_id as post_id,
        comment_author_id as user_id,
        parent_id,
        subreddit_id,
        commnet_body as body,
        score,
        TO_TIMESTAMP(created_at) as created_at
    from source
)
select * from transformed