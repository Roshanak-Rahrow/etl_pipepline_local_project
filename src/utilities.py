import os 
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    try:
        conn = mysql.connector.connect(
            host=os.environ.get("localhost"),
            database=os.environ.get("MYSQL_DATABASE"),
            user=os.environ.get("MYSQL_USER"),
            password=os.environ.get("MYSQL_PASSWORD"),
            port=int(os.environ.get("MYSQL_PORT", 3306))
        )
        print("connected")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None