import mysql.connector
from mysql.connector import Error
import os
import dotenv
from rich import print
import random
import csv
from datetime import datetime
import logging

# Custom module
from scraper import main as get_data # the scraper will scrape data from website
from delete_meta import delete_job_metadata # delete all the postmeta data

dotenv.load_dotenv()
# This is database credentials from .env file
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_USER = os.getenv('DB_USER')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')

# Logging path to save lof information
LOG_FILE_PATH = "/home2/obpeavmy/drpython/logs/app_logs.log"

# Configure the logger to save log information
def setup_logger():
    """Set up logging configuration"""
    logger = logging.getLogger("JobScraper")
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers
    if not logger.handlers:
        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File Handler
        file_handler = logging.FileHandler(LOG_FILE_PATH, mode='a')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger

logger = setup_logger()

# Create a connection with database
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
            logger.info("Successfully connected to MySQL database")
            return connection
    except Error as e:
        logger.error(f"Error while connecting to MySQL: {e}")
        return None

# Format the date as DD-MM-YYYY
def format_date(date_str):
    """Convert date from YYYY-MM-DD to DD-MM-YYYY format"""
    try:
        # Parse the date string
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        # Format to desired output
        return date_obj.strftime('%d-%m-%Y')
    except (ValueError, TypeError):
        return date_str  # Return original if conversion fails

# Insert job into JJ3_posts database
def insert_job(connection, job_data):
    """Insert job data into WordPress posts table with proper formatting"""
    try:
        # Create formatted post content with Markdown
        post_content = f"""{job_data['description'] if job_data['description'] else ''}

## Job Details
**Position:** {job_data['occupation']}
**Location:** {job_data['address_locality_name']}
**Address:** {job_data['address_street']} {job_data['address_street_number']}

## Requirements
**Education:** {job_data['education_level_name']}
**Experience:** {job_data['professional_experience_name']}
**Work Type:** {job_data['work_type_name']}
**Contract:** {job_data['contract_type_name']}

## Additional Information
- **Available Positions:** {job_data['open_positions']}
- **Work Regime:** {job_data['work_regime_name']}
- **Expiry Date:** {format_date(job_data['job_expiry_date'])}
- **EU Citizens Eligible:** {'Yes' if job_data['offer_available_eu_citizens'] else 'No'}
        """.strip()

        post_data = {
            'ID': job_data['id'],
            'post_author': 1,
            'post_date': job_data['created_at'],
            'post_date_gmt': job_data['created_at'],
            'post_content': post_content,
            'post_title': f"{job_data['job_domain_name']}",
            'post_modified': job_data['created_at'],
            'post_modified_gmt': job_data['created_at'],
            'post_type': 'job_listing',
            'post_excerpt': "",
            'to_ping': '',
            'pinged': '',
            'post_content_filtered': '',
            'post_name': str(job_data['id'])
        }

        insert_query = """
        INSERT INTO JJ3_posts (
            ID, post_author, post_date, post_date_gmt, 
            post_content, post_title, post_modified, 
            post_modified_gmt, post_type, post_excerpt, to_ping, pinged,
            post_content_filtered, post_name
        ) VALUES (
            %(ID)s, %(post_author)s, %(post_date)s, %(post_date_gmt)s,
            %(post_content)s, %(post_title)s, %(post_modified)s,
            %(post_modified_gmt)s, %(post_type)s, %(post_excerpt)s, %(to_ping)s,
            %(pinged)s, %(post_content_filtered)s, %(post_name)s
        ) ON DUPLICATE KEY UPDATE
            post_content = VALUES(post_content),
            post_title = VALUES(post_title),
            post_modified = VALUES(post_modified),
            post_modified_gmt = VALUES(post_modified_gmt)
        """
        
        with connection.cursor() as cursor:
            cursor.execute(insert_query, post_data)
            connection.commit()
            logger.info(f"Successfully inserted/updated WordPress post ID: {post_data['ID']}")
            return True
    except Error as e:
        logger.error(f"Error inserting WordPress post: {e}")
        connection.rollback()
        return False


# Inser post metada into JJ3_postmeta
def insert_postmeta(connection, post_id, job_data):
    """Insert postmeta data into the JJ3_postmeta table"""
    try:
        # Prepare the postmeta data
        postmeta_data = [
            {'post_id': post_id, 'meta_key': '_job_address', 'meta_value': f"{job_data['address_street']} {job_data['address_street_number']}"},
            {'post_id': post_id, 'meta_key': '_job_qualification', 'meta_value': job_data['education_level_name']},
            {'post_id': post_id, 'meta_key': '_job_experience', 'meta_value': job_data['professional_experience_name']},
            {'post_id': post_id, 'meta_key': '_job_salary_type', 'meta_value': 'Monthly'},
            {'post_id': post_id, 'meta_key': '_job_max_salary', 'meta_value': job_data['maximum_salary']},
            {'post_id': post_id, 'meta_key': '_job_salary', 'meta_value': job_data['minimum_salary']},
            {'post_id': post_id, 'meta_key': '_job_career_level', 'meta_value': job_data['occupation']},
            {'post_id': post_id, 'meta_key': '_job_urgent', 'meta_value': "On",},
            {'post_id': post_id, 'meta_key': '_job_gender', 'meta_value': "Both",},
            {'post_id': post_id, 'meta_key': '_thumbnail_id', 'meta_value': '9769'},
            {'post_id': post_id, 'meta_key': '_job_expiry_date', 'meta_value': job_data['job_expiry_date']},
            {'post_id': post_id, 'meta_key': '_viewed_count', 'meta_value': random.randint(100, 1500)},
        ]

        # Insert each postmeta
        insert_query = """
        INSERT INTO JJ3_postmeta (post_id, meta_key, meta_value)
        VALUES (%(post_id)s, %(meta_key)s, %(meta_value)s)
        """
        
        with connection.cursor() as cursor:
            cursor.executemany(insert_query, postmeta_data)
            connection.commit()
            logger.info(f"Successfully inserted postmeta for post ID: {post_id}")
            return True
    except Error as e:
        logger.error(f"Error inserting postmeta for post ID {post_id}: {e}")
        connection.rollback()
        return False

# To read our csv file which is already in our database
def read_jobs_csv(csv_path):
    """Read existing jobs from CSV file with robust encoding handling"""
    try:
        # First try to detect the encoding
        with open(csv_path, 'rb') as file:
            raw_data = file.read()
            
        encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1', 'utf-16']
        
        for encoding in encodings:
            try:
                # Try to decode the raw data with current encoding
                decoded_data = raw_data.decode(encoding)
                
                # If successful, process the data
                from io import StringIO
                csv_file = StringIO(decoded_data)
                reader = csv.DictReader(csv_file)
                
                new_jobs = []
                for row in reader:
                    # Clean the data
                    cleaned_row = {
                        k: ('' if not v else str(v).strip())
                        for k, v in row.items()
                    }
                    new_jobs.append(cleaned_row)
                
                logger.info(f"Successfully loaded {len(new_jobs)} jobs using {encoding}")
                return new_jobs
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error with {encoding}: {str(e)}")
                continue
                
        logger.error("Failed to read file with any supported encoding")
        return []
        
    except FileNotFoundError:
        logger.warning("No existing jobs file found - starting fresh")
        return []
    except Exception as e:
        logger.error(f"Unexpected error reading jobs file: {str(e)}")
        return []

def main():
    """Main function to start and run the process"""
    logger.info("Program started")
    
    # Scrape new jobs
    get_data()
    
    # Delete all jobs
    delete_job_metadata()
    logger.info("Old job deleted!")
    
    # Create connection with database
    connection = create_connection()
    
    csv_path = 'new_jobs.csv'
    jobs = read_jobs_csv(csv_path)
    
    if connection and jobs:
        try:
            for index, job in enumerate(jobs, start=1):
                insert_job(connection, job)
                insert_postmeta(connection, job['id'], job)
        finally:
            if connection.is_connected():
                connection.close()
                logger.info("MySQL connection closed")
                
    else:
        if connection and connection.is_connected():
            connection.close()
        logger.warning("MySQL connection closed due to failure")
    
    logger.info("Program finished")

if __name__ == "__main__":
    main()
