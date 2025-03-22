import mysql.connector
from mysql.connector import Error
import os
import dotenv

dotenv.load_dotenv()

DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_USER = os.getenv('DB_USER')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')

def create_connection():
    """Create connection with database"""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            return connection
    except Error as e:
        return None

def delete_job_metadata():
    """Delete all job metadata from JJ3_postmeta table"""
    connection = create_connection()
    
    if not connection:
        return False
        
    try:
        with connection.cursor() as cursor:
            # Get all job listing post IDs
            select_query = "SELECT ID FROM JJ3_posts WHERE post_type = 'job_listing'"
            cursor.execute(select_query)
            results = cursor.fetchall()
            
            deleted_count = 0
            # Delete metadata for each job post
            for row in results:
                post_id = row[0]
                delete_query = "DELETE FROM JJ3_postmeta WHERE post_id = %s"
                delete_post_query = "DELETE FROM JJ3_posts WHERE `ID` = %s"
                cursor.execute(delete_query, (post_id,))
                cursor.execute(delete_post_query, (post_id,))
                deleted_count += cursor.rowcount
            
            connection.commit()
            return True
            
    except Error as e:
        connection.rollback()
        return False
        
    finally:
        if connection.is_connected():
            connection.close()

if __name__ == '__main__':
    delete_job_metadata()
