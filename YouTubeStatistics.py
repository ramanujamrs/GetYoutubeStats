# Author: Ramanujam Srinivasan
# Get Youtube video statistics - viewcount for each video and save it in DB

import os
import requests

from mysql import connector
from dotenv import load_dotenv

from datetime import datetime

# Access environment variables
load_dotenv()
HOST = os.getenv("HOST")
USERNAME = "ramanujam"
PASSWORD = os.getenv("PASSWORD")
DB = os.getenv("DB")
KEY = os.getenv("KEY")

# INSERT Statement
single_record = "INSERT INTO yt_video_viewcount (video_id, view_count,datetime)\
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
       
        select_specific_cols = "SELECT video_id, title FROM yt_channel_videos"
        with database.cursor() as cursor:
            cursor.execute(select_specific_cols)
            result = cursor.fetchall()
            for row in result:
                print(row[0])
                videoId = row[0]
                videostats = "https://youtube.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&id="+ videoId +"&key="+KEY
                statsrequest = requests.get(videostats)
                statsvideos = statsrequest.json()

                for st in statsvideos["items"]: 
                    #print(st["statistics"]["viewCount"])
                    now = datetime.now()
                    print("now =", now)
                    # dd/mm/YY H:M:S
                    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
                    video_records = [
                    (
                        videoId,
                        st["statistics"]["viewCount"],
                        dt_string
                    )]
                with database.cursor() as cursor:
                    cursor.executemany(single_record, video_records)
                    database.commit()
        database.close()
except connector.Error as e: 
    print(e)



