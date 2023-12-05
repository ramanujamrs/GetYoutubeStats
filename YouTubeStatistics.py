# Author: Ramanujam Srinivasan
# Get Youtube video statistics - viewcount for each video and save it in DB

import os
import requests

from mysql import connector
from dotenv import load_dotenv

from datetime import datetime
import time
from zoneinfo import ZoneInfo


# Access environment variables
load_dotenv()
HOST = os.getenv("HOST")
USERNAME = "ramanujam"
PASSWORD = os.getenv("PASSWORD")
DB = os.getenv("DB")
KEY = os.getenv("KEY")

# INSERT Statement
insert_stmt = "INSERT INTO yt_video_viewcount (video_id, view_count,datetime)\
    VALUES (%s, %s, %s)"

# Connect to DB
# Get Video Ids from table
try:
    with connector.connect(
        host = HOST,
        user = USERNAME,
        password = PASSWORD,
        database = DB
    ) as database: 
       
        print("Program Started")
        select_stmt = "SELECT video_id, title FROM yt_channel_videos"
        with database.cursor() as cursor:
            cursor.execute(select_stmt)
            result = cursor.fetchall()
            for row in result:
                videoId = row[0]
                videostats = "https://youtube.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&id="+ videoId +"&key="+KEY
                statsrequest = requests.get(videostats)
                statsvideos = statsrequest.json()

                for st in statsvideos["items"]: 
                    now = datetime.now()
                    us_central_dt = datetime.fromtimestamp(time.time(), tz=ZoneInfo("America/Chicago"))
                    # YYYY-MM-DD H:M:S
                    dt_string = us_central_dt.strftime("%Y-%m-%d %H:%M:%S")
                    video_records = [
                    (
                        videoId,
                        st["statistics"]["viewCount"],
                        dt_string
                    )]
                with database.cursor() as cursor:
                    cursor.executemany(insert_stmt, video_records)
                    database.commit()
        database.close()
        print("Program Completed", now)
                    
except connector.Error as e: 
    print(e)



