from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
os.environ['SPARK_HOME'] = '/opt/homebrew/opt/apache-spark/libexec'

spark = SparkSession.builder \
    .appName("reddit database") \
    .getOrCreate()

mother_duck_token = os.getenv('MOTHERDUCK_TOKEN')
path = f"md:reddit?motherduck_token={mother_duck_token}"
def save_to_duckdb(df, table_name: str, db_path=path):
    df = df.toPandas()
    conn = duckdb.connect(db_path)
    conn.register('df_view', df)  # Register DataFrame as a view
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df_view")
    conn.close()

def load_data(path: str, table_name: str):
    df = spark.read.parquet(path)
    save_to_duckdb(df, table_name)

load_data('data/comments/date=2025-02-04/*', 'reddit_comments')
load_data('data/posts/date=2025-02-04/*', 'reddit_posts')
load_data('data/comment_replies/date=2025-02-04/*', 'reddit_comments_replies')
conn = duckdb.connect('reddit.db')
print(conn.execute("SELECT COUNT(*) FROM reddit_posts").fetchall())
conn.close()


