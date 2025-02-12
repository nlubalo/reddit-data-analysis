import requests
import datetime
import json
from base_reader import BaseReader
from pmaw import PushshiftAPI
import datetime as dt 

class RedditBulkReader(BaseReader):
    def authenticate(self):
         api = PushshiftAPI()
         return api

    def run_query(self, subreddit: str, start_time, end_time):
        posts = self.post(subreddit, start_time, end_time)
        return posts
    #'author_id', 'author_name','created_at','num_comments','post','post_title','score','upvote_ratio','post_id'
    def post(self, subreddit, start_time, end_time):
        filters = ['id', 'author', 'created_utc',
                   'domain', 'url',
                   'title', 'num_comments']

        posts = self.authenticate().search_submissions(
            subreddit=subreddit,   #Subreddit we want to audit
            limit=10,              #Number of submissions to return
            #after=start_time,      #Start date
            #before=end_time,       #End date
            )
        for i in posts:
            print(i.created_utc.date())
        return posts

    def save_parquet(self, data, path):
        pass


bulk_reader = RedditBulkReader()
start_date = int(dt.datetime(2024, 1, 1).timestamp())
end_date = int(dt.datetime(2024, 1, 2).timestamp())
posts = bulk_reader.run_query('AmItheAsshole', start_time=start_date, end_time=end_date)
print(posts)