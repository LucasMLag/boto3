import boto3
import zipfile
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from retry import retry
from botocore.config import Config
import psycopg2
import easyocr

# Initialize EasyOCR reader
reader = easyocr.Reader(['pt', 'en'])

def perform_ocr(file_path):
    """Extract text from an image file and return the result."""
    result = reader.readtext(file_path, detail=0, paragraph=True)
    return "\n".join(result)

def create_table():
    conn = psycopg2.connect(
        dbname=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        host=os.getenv("DATABASE_HOST"),
        port=os.getenv("DATABASE_PORT")
    )
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS zips (
                id SERIAL PRIMARY KEY,
                file_path VARCHAR(255) UNIQUE NOT NULL,
                status VARCHAR(30),
                error_data TEXT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id SERIAL PRIMARY KEY,
                zips_id INTEGER REFERENCES zips(id) ON DELETE CASCADE,
                file_path VARCHAR(255) UNIQUE NOT NULL,
                ocr_text TEXT,
                error_data TEXT,
                status VARCHAR(30)
            );
        """)
        conn.commit()
    conn.close()

# Database connection setup
def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        host=os.getenv("DATABASE_HOST"),
        port=os.getenv("DATABASE_PORT"),
        connect_timeout=500
    )
    return conn

# Check if file has been processed
def is_zip_processed(file_path):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT status FROM zips WHERE file_path = %s", (file_path,))
        result = cursor.fetchone()
    conn.close()
    return result and result[0] == "completed"

# Update file status in the database
def update_zip_status(file_path, status):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO zips (file_path, status)
            VALUES (%s, %s)
            ON CONFLICT (file_path)
            DO UPDATE SET status = EXCLUDED.status;
        """, (file_path, status))
        conn.commit()
    conn.close()

# Update file status with error in the database
def update_file_error_data(file_path, error_data):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO zips (file_path, error_data)
            VALUES (%s, %s)
            ON CONFLICT (file_path)
            DO UPDATE SET error_data = EXCLUDED.error_data;
        """, (file_path, error_data))
        conn.commit()
    conn.close()

# Increase max connections in config
config = Config(
    retries={
        'max_attempts': 10,  # Increase retries if needed
        'mode': 'standard'
    },
    max_pool_connections=20  # Increase from the default 10
)

# Initialize S3 client with custom config
s3_client = boto3.client('s3', config=config)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create console handler for outputting to the VS Code console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a file handler if you also want to log to a file
file_handler = logging.FileHandler("process_zip.log")
file_handler.setLevel(logging.DEBUG)

# Define a simple log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

def insert_file_record(zip_id, file_path, ocr_text=None, status="completed", error_data=None):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO files (zips_id, file_path, ocr_text, status, error_data)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (file_path)
            DO UPDATE SET
                ocr_text = EXCLUDED.ocr_text,
                status = EXCLUDED.status,
                error_data = EXCLUDED.error_data;
        """, (zip_id, file_path, ocr_text, status, error_data))
        conn.commit()
    conn.close()

# Check if file has been processed
def is_file_processed(file_path):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT status FROM files WHERE file_path = %s", (file_path,))
        result = cursor.fetchone()
    conn.close()
    return result and result[0] == "completed"

def insert_zip_record(file_path):
    """Insert zip record and return the zip ID."""
    conn = get_db_connection()
    zip_id = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO zips (file_path, status) VALUES (%s, 'in_progress')
                ON CONFLICT (file_path) DO NOTHING
                RETURNING id;
            """, (file_path,))
            zip_id = cursor.fetchone()
            if zip_id is None:  # Fetch existing ID if not returned by INSERT
                cursor.execute("SELECT id FROM zips WHERE file_path = %s;", (file_path,))
                zip_id = cursor.fetchone()
        conn.commit()
    except Exception as e:
        logger.error(f"Error inserting zip record for {file_path}: {e}")
    finally:
        conn.close()
    return zip_id[0] if zip_id else None

# Retry settings
@retry(tries=3, delay=2, backoff=2, exceptions=(boto3.exceptions.S3UploadFailedError, Exception))
def download_with_retry(bucket_name, s3_path, download_path):
    """Attempts to download a file from S3 with retry on failure."""
    s3_client.download_file(bucket_name, s3_path, download_path)
    logger.info(f"Successfully downloaded {s3_path} to {download_path}")

def process_zip(bucket_name, s3_path, output_dir):
    """Download and extract a zip file from S3 with error handling."""
    filename = s3_path.split('/')[-1]
    download_path = os.path.join(output_dir, filename)
    
    try:
        # Attempt download with retry
        update_zip_status(s3_path, "downloading")
        download_with_retry(bucket_name, s3_path, download_path)

        # Unzip the downloaded file
        update_zip_status(s3_path, "extracting")
        with zipfile.ZipFile(download_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
            logger.info(f"Extracted {filename} to {output_dir}")
            zip_id = insert_zip_record(s3_path)

            # Process each extracted file
            for extracted_file in zip_ref.namelist():
                file_path = os.path.join(output_dir, extracted_file)

                # Skip directories in the extracted files
                if os.path.isdir(file_path):
                    logger.info(f"Skipping directory {file_path}")
                    continue
                
                # Check if the file is already processed
                if is_file_processed(file_path):
                    logger.info(f"File {file_path} already processed, skipping.")
                    continue
                
                # # Perform OCR and save result
                try:
                    ocr_text = perform_ocr(file_path)
                    insert_file_record(zip_id, file_path, ocr_text, "completed")
                    logger.info(f"OCR completed for {file_path}")
                except Exception as e:
                    error_message = f"OCR processing error for {file_path}: {e}"
                    logger.error(error_message)
                    insert_file_record(zip_id, file_path, None, "error", error_message)
        
        update_zip_status(s3_path, "completed")
    
    except zipfile.BadZipFile:
        logger.error(f"Error: {filename} is a bad zip file.")
        update_zip_status(s3_path, "error_bad_zip")
        update_file_error_data(s3_path, "bad_zip")
    except Exception as e:
        logger.error(f"Error processing {filename}: {str(e)}")
        update_zip_status(s3_path, "error")
        update_file_error_data(s3_path, str(e))
    finally:
        if os.path.exists(download_path):
            os.remove(download_path)
            logger.info(f"Cleaned up {download_path}")

def list_client_folders(s3_client, bucket_name):
    """List top-level client folders."""
    response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
    client_folders = [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]
    return client_folders

def list_client_directories(bucket_name):
    """Detect all client folders to monitor."""
    client_folders = list_client_folders(s3_client, bucket_name)
    monitored_paths = []
    for client in client_folders:
        for folder_type in ['2', '3', '4']:
            for subdir in ['manual', 'ocr']:
                path = f"{client}{folder_type}/pending/{subdir}/"
                monitored_paths.append(path)
    return monitored_paths

def list_zips_in_s3_folder(bucket, folder):
    """List all zip files in a specified S3 folder."""
    zip_files = []
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.zip'):
                zip_files.append(obj['Key'])
    return zip_files

def process_client_folder(bucket_name, client_folder, output_dir):
    """Process all zip files in a client's directory."""
    zip_files = list_zips_in_s3_folder(bucket_name, client_folder)
    for zip_file in zip_files:
        process_zip(bucket_name, zip_file, output_dir)

def main():
    bucket_name = "a3dbucket002"
    output_dir = "/app/output"
    monitored_paths = list_client_directories(bucket_name)
    create_table()
    
    with ThreadPoolExecutor(max_workers=1) as executor:
        for client_folder in monitored_paths:
            client_output_dir = os.path.join(output_dir, client_folder)
            os.makedirs(client_output_dir, exist_ok=True)
            executor.submit(process_client_folder, bucket_name, client_folder, client_output_dir)

if __name__ == "__main__":
    main()
