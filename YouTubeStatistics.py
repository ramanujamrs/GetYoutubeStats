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

try:
    with connector.connect(
        host = HOST,
        user = USERNAME,
        password = PASSWORD,
        database = DB
    ) as database: 
        print(f"Database object: {database}")
except connector.Error as e: 
    print(e)


