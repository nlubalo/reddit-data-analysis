---
title: r/AmItheAsshole Subreddit Interactions
---

## Overview of users interactions last week

<BigValue 
  data={posts_count} 
  value=weekly_posts
  title="Last week's total posts"
/>

<BigValue 
  data={comments_count} 
  value=weekly_comments
  title="Last week's total comments"
/>

<BigValue 
  data={replies_count} 
  value=weekly_replies
  title="Last week's total comment replies"
/>


``` posts_count
SELECT 
    SUM(weekly_interaction_count) as weekly_posts FROM weekly_interactions
    WHERE interaction_type = 'post'

```

``` comments_count
SELECT 
    SUM(weekly_interaction_count) as weekly_comments FROM weekly_interactions
    WHERE interaction_type = 'comment'

```


``` replies_count
SELECT 
    SUM(weekly_interaction_count) as weekly_replies FROM weekly_interactions
    WHERE interaction_type = 'reply'

```


```test_connection

select 1 from motherduck.nyc_service_request_volume limit 1 

```


{#if dev && !test_connection.ready}

## Connect to MotherDuck 🐣

1. [Get your service token](https://motherduck.com/docs/key-tasks/authenticating-to-motherduck/#authentication-using-a-service-token)
1. [Connect Evidence to MotherDuck](/settings)

{:else}

## NYC Service Requests

<Slider 
    defaultValue=7
    title='Rolling Average'
    min=2
    max=90
    name=rolling_avg
/> 

```sql daily_service_requests

select 
    *,
    avg(requests) over(order by "created_date" asc
                       RANGE BETWEEN INTERVAL ${inputs.rolling_avg} DAYS PRECEDING
                       AND INTERVAL 0 DAYS FOLLOWING) as rolling_average
from motherduck.nyc_service_request_volume 

```


<Chart 
    data={daily_service_requests} 
    x=created_date
    title={`Daily Request Volume and ${inputs.rolling_avg} Day Rolling Average`}
    renderer='svg'
>
    <Line 
        y=rolling_average
        lineColor=black
    /> 
    <Scatter 
        y=requests 
        pointSize=3
        opacity=0.3
        fillColor=gray
    /> 
</Chart>


<Histogram 
    data={daily_service_requests} 
    x=requests
    fillColor=black
/> 


## What's Next?
- Edit the markdown files in the `pages` folder
- Add new queries to `sources/motherduck` 
- Deploy your project with [Evidence Cloud](https://evidence.dev/cloud)


## Resources 
- Message us on [Slack](https://slack.evidence.dev/)
- Read the [Docs](https://docs.evidence.dev/)
- Open an issue on [Github](https://github.com/evidence-dev/evidence)

{/if}



