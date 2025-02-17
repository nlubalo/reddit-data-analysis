import requests
import os
from dotenv import load_dotenv
import praw
from praw.models import MoreComments
import datetime
import json
from base_reader import BaseReader
from pyspark.sql import SparkSession

load_dotenv()
# os.environ['SPARK_HOME'] = '/opt/homebrew/opt/apache-spark/libexec'

spark = SparkSession.builder \
    .appName("RedditPartitionedWrite") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()
timestamp_now = datetime.datetime.now().strftime("%Y-%m-%d")


class RedditReader(BaseReader):
    def __init__(self, base_url: str,
                 username: str, password: str,
                 app_id: str, app_secret: str,
                 app_name: str):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_name = app_name
        self.headers = {'user-agent': f'{self.app_name} by {self.username}'}

    def authenticate(self):
        reddit = praw.Reddit(
            client_id=self.app_id,
            client_secret=self.app_secret,
            password=self.password,
            user_agent=f'{self.app_name} by {self.username}',
            username=self.username,
            ratelimit_seconds=60
        )
        return reddit

    def extract_subreddit(self, post):
        data = {"post_title": post.title, 'post': post.selftext, 'url': post.url, 'created_at': post.created_utc,
                'score': post.score, 'num_comments': post.num_comments, 'upvote_ratio': post.upvote_ratio,
                'post_id': post.id, 'author_name': post.author.name, "author_id": post.author.id,
                'subreddit_id': post.subreddit_id, 'display_name': post.subreddit.display_name, 'display_name_prefixed': post.subreddit.display_name_prefixed,
                'subreddit_type': post.subreddit.subreddit_type, 'title': post.subreddit.title, 'url': post.subreddit.url,
                'user_is_banned': post.subreddit.user_is_banned, 'user_is_contributor': post.subreddit.user_is_contributor,
                'user_is_moderator': post.subreddit.user_is_moderator, 'user_is_subscriber': post.subreddit.user_is_subscriber,
                'wiki_enabled': post.subreddit.wiki_enabled, 'over18': post.subreddit.over18, 'public_description': post.subreddit.public_description,
                'num_subscribers': post.subreddit.subscribers, 'subreddit_type': post.subreddit.subreddit_type,
                'dowload_date': timestamp_now}
        return data

    def extract_comments(self, comments):
        comments_data = {'commnet_body': comments.body, 'comment_author_id': str(comments.author), 'created_at': comments.created_utc,
                         'score': comments.score, 'comment_id': comments.id, 'parent_id': comments.parent_id,
                         'submission_title': comments.submission.title,
                         'submission_selftext': comments.submission.selftext, 'submission_id': comments.submission.id,
                         'subreddit_id': comments.subreddit.id,
                         'user_is_banned': comments.subreddit.user_is_banned,
                         'user_is_contributor': comments.subreddit.user_is_contributor, 'user_is_moderator': comments.subreddit.user_is_moderator,
                         'user_is_subscriber': comments.subreddit.user_is_subscriber, 'wiki_enabled': comments.subreddit.wiki_enabled, 'over18': comments.subreddit.over18,
                         'num_subscribers': comments.subreddit.subscribers, 'subreddit_type': comments.subreddit.subreddit_type, 'dowload_date': timestamp_now}
        return comments_data

    def extract_replies(self, replies, comment_id):
        replies_data = {'reply_body': replies.body, 'reply_author_id': str(replies.author), 'created_at': replies.created_utc,
                        'score': replies.score, 'reply_id': replies.id,
                        'reply_parent_id': replies.parent_id, 'submission_title': replies.submission.title,
                        'comment_id': comment_id, 'submission_id': replies.submission.id,
                        'submission_title': replies.submission.title,
                        'subreddit_id': replies.subreddit.id, 'dowload_date': timestamp_now}
        return replies_data

    def search_subreddit(self, subreddit: str, limit: int):
        post_output = []
        post_comments = []
        replies_output = []
        auth = self.authenticate()
        subreddit = auth.subreddit("AmItheAsshole")
        for post in subreddit.hot(limit=limit):
            data = self.extract_subreddit(post)
            post_output.append(data)
            for comments in auth.submission(url=post.url).comments:
                if isinstance(comments, MoreComments):
                    continue
                comments_data = self.extract_comments(comments)
                post_comments.append(comments_data)
                for reply in comments.replies:
                    if isinstance(reply, MoreComments):
                        continue
                    reply_data = self.extract_replies(reply, comments.id)
                    replies_output.append(reply_data)
        return post_comments,  post_output, replies_output

    def run_query(self, subreddit: dict):
        for subreddit, limit in subreddit.items():
            comments, posts, replies_output = self.search_subreddit(
                subreddit, limit)
            timestamp = datetime.datetime.now().strftime(
                "%Y-%m-%d")  # Use hyphens instead of colons
            df_comments = spark.createDataFrame(comments)
            df_posts = spark.createDataFrame(posts)
            df_replies = spark.createDataFrame(replies_output)
            self.save_parquet(
                df_comments, f"data/comments/date={timestamp_now}")
            self.save_parquet(df_posts, f"data/posts/date={timestamp_now}")
            self.save_parquet(
                df_replies, f"data/comment_replies/date={timestamp_now}")

    def save_parquet(self, data, path):
        data.write \
            .mode("overwrite") \
            .parquet(path)

    def save_json(self, data, filename):
        print("Saving data to json file")
        with open(filename, 'a') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


base_url = 'https://www.reddit.com/'
username = os.getenv("REDDIT-USERNAME")
password = os.getenv("REDDIT-PASSWORD")
app_id = os.getenv("APP-ID")
app_secret = os.getenv("APP-SECRET")
app_name = 'Analytics'
reader = RedditReader(base_url=base_url,
                      username=username,
                      password=password,
                      app_id=app_id,
                      app_secret=app_secret,
                      app_name=app_name)

query_items = {'AmItheAsshole': 10}
reader.run_query(query_items)
