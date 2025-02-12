with source as (
    select * from {{source('reddit', 'reddit_comments_replies')}}
),
transformed as (
    select
        reply_author_id as reply_id,
        submission_id as post_id,
        reply_author_id as user_id,
        reply_parent_id as parent_comment_id,
        subreddit_id,
        TO_TIMESTAMP(created_at) as created_at,
        score
    from source
)
select * from transformed