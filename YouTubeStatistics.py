import os

from mysql import connector
from dotenv import load_dotenv

# Access environment variables
load_dotenv()
HOST = os.getenv("HOST")
USERNAME = "ramanujam"
PASSWORD = os.getenv("PASSWORD")
DB = os.getenv("DB")

# Connect to DB
# Get Video Ids from table
try:
    with connector.connect(
        host = HOST,
        user = USERNAME,
        password = PASSWORD,
        database = DB
    ) as database: 
       
        select_specific_cols = "SELECT video_id, title FROM yt_channel_videos"
        with database.cursor() as cursor:
            cursor.execute(select_specific_cols)
            result = cursor.fetchall()
            for row in result:
                print(row)
except connector.Error as e: 
    print(e)



