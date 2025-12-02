import os
import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
import logging
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
print(
    os.getenv('AWS_ACCESS_KEY_ID'),
)
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3ToPostgresETL:
    def __init__(self):
        # AWS S3 Configuration
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            # aws_session_token = os.getenv('AWS_SESSION_TOKEN'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.s3_prefix = os.getenv('S3_PREFIX', '')  # Optional folder path in S3
        
        # Azure PostgreSQL Configuration
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
        }
        
        # Add SSL mode only if not localhost (for Azure production)
        if self.db_config['host'] not in ['localhost', '127.0.0.1']:
            self.db_config['sslmode'] = 'require'
        
    def get_db_connection(self):
        """Create PostgreSQL connection"""
        return psycopg2.connect(**self.db_config)
    
    def list_new_files(self, last_sync_time: Optional[datetime] = None) -> List[dict]:
        """
        List CSV files in S3 bucket modified after last_sync_time
        If last_sync_time is None, fetch files from last 24 hours
        """
        if last_sync_time is None:
            last_sync_time = datetime.now() - timedelta(hours=24)
        
        logger.info(f"Fetching files modified after {last_sync_time}")
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.s3_prefix
            )
            
            if 'Contents' not in response:
                logger.warning("No files found in S3 bucket")
                return []
            
            new_files = []
            for obj in response['Contents']:
                # Filter CSV files modified after last_sync_time
                if obj['Key'].endswith('.csv') and obj['LastModified'].replace(tzinfo=None) > last_sync_time:
                    new_files.append({
                        'key': obj['Key'],
                        'last_modified': obj['LastModified'],
                        'size': obj['Size']
                    })
            
            logger.info(f"Found {len(new_files)} new CSV files")
            return new_files
            
        except Exception as e:
            logger.error(f"Error listing S3 files: {e}")
            raise
    
    def download_and_parse_csv(self, s3_key: str) -> pd.DataFrame:
        """Download CSV from S3 and parse into DataFrame"""
        try:
            logger.info(f"Downloading {s3_key}")
            
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            df = pd.read_csv(obj['Body'], sep=';')
            
            logger.info(f"Parsed {len(df)} rows from {s3_key}")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading/parsing {s3_key}: {e}")
            raise
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and validate data"""
        # Create datetime column from year/month/day/hour
        if {'year', 'month', 'day', 'hour'}.issubset(df.columns):
            df['transaction_datetime'] = pd.to_datetime(
                df[['year', 'month', 'day', 'hour']]
            )
        else:
            raise ValueError("CSV missing required date columns: year, month, day, hour")
        
        # Extract date (without time)
        df['transaction_date'] = df['transaction_datetime'].dt.date
        
        # Handle missing values
        df['operation'] = df['operation'].fillna('Unknown')
        df['channel'] = df['channel'].fillna('Unknown')
        df['entity'] = df['entity'].fillna('Unknown')
        
        # Ensure numeric columns
        df['terminal_id'] = pd.to_numeric(df['terminal_id'], errors='coerce')
        df['cant_trx'] = pd.to_numeric(df['cant_trx'], errors='coerce').fillna(0)
        df['transaction_amount'] = pd.to_numeric(df['transaction_amount'], errors='coerce').fillna(0)
        
        return df
    
    def create_table_if_not_exists(self, conn):
        """Create transactions table with proper indexes"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            terminal_id INTEGER,
            operation VARCHAR(100),
            channel VARCHAR(100),
            entity VARCHAR(100),
            year INTEGER,
            month INTEGER,
            day INTEGER,
            hour INTEGER,
            cant_trx NUMERIC,
            transaction_amount NUMERIC,
            transaction_datetime TIMESTAMP,
            transaction_date DATE,
            source_file VARCHAR(255),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_transaction UNIQUE (terminal_id, transaction_datetime, operation, channel)
        );
        
        -- Create indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_terminal_id ON transactions(terminal_id);
        CREATE INDEX IF NOT EXISTS idx_transaction_date ON transactions(transaction_date);
        CREATE INDEX IF NOT EXISTS idx_transaction_datetime ON transactions(transaction_datetime);
        CREATE INDEX IF NOT EXISTS idx_operation ON transactions(operation);
        CREATE INDEX IF NOT EXISTS idx_channel ON transactions(channel);
        CREATE INDEX IF NOT EXISTS idx_entity ON transactions(entity);
        """
        
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()
        
        logger.info("Table and indexes created/verified")
    
    def load_data_to_postgres(self, df: pd.DataFrame, source_file: str, conn):
        """Bulk insert data into PostgreSQL with upsert logic"""
        df['source_file'] = source_file
        
        # Prepare data for insertion
        columns = [
            'terminal_id', 'operation', 'channel', 'entity',
            'year', 'month', 'day', 'hour',
            'cant_trx', 'transaction_amount',
            'transaction_datetime', 'transaction_date', 'source_file'
        ]
        
        data_tuples = [tuple(row) for row in df[columns].values]
        
        # Upsert SQL (insert or update on conflict)
        insert_sql = f"""
        INSERT INTO transactions ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        ON CONFLICT (terminal_id, transaction_datetime, operation, channel)
        DO UPDATE SET
            cant_trx = EXCLUDED.cant_trx,
            transaction_amount = EXCLUDED.transaction_amount,
            entity = EXCLUDED.entity,
            source_file = EXCLUDED.source_file,
            loaded_at = CURRENT_TIMESTAMP
        """
        
        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, data_tuples, page_size=1000)
            conn.commit()
        
        logger.info(f"Loaded {len(df)} rows from {source_file}")
    
    def get_last_sync_time(self, conn) -> Optional[datetime]:
        """Get the last successful sync time from metadata table"""
        # Create metadata table if not exists
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS etl_metadata (
                    id SERIAL PRIMARY KEY,
                    last_sync_time TIMESTAMP,
                    status VARCHAR(50),
                    files_processed INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            
            # Get last successful sync
            cur.execute("""
                SELECT last_sync_time FROM etl_metadata
                WHERE status = 'success'
                ORDER BY created_at DESC
                LIMIT 1
            """)
            result = cur.fetchone()
            
            return result[0] if result else None
    
    def update_sync_metadata(self, conn, files_processed: int, status: str = 'success'):
        """Record sync metadata"""
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO etl_metadata (last_sync_time, status, files_processed)
                VALUES (CURRENT_TIMESTAMP, %s, %s)
            """, (status, files_processed))
            conn.commit()
    
    def run(self):
        """Main ETL process"""
        logger.info("Starting ETL job")
        
        try:
            # Connect to PostgreSQL
            conn = self.get_db_connection()
            logger.info("Connected to PostgreSQL")
            
            # Create table and indexes
            self.create_table_if_not_exists(conn)
            
            # Get last sync time
            last_sync = self.get_last_sync_time(conn)
            logger.info(f"Last sync time: {last_sync}")
            
            # List new files from S3
            new_files = self.list_new_files(last_sync)
            
            if not new_files:
                logger.info("No new files to process")
                return
            
            # Process each file
            files_processed = 0
            for file_info in new_files:
                try:
                    # Download and parse
                    df = self.download_and_parse_csv(file_info['key'])
                    
                    # Transform
                    df = self.transform_data(df)
                    
                    # Load to PostgreSQL
                    self.load_data_to_postgres(df, file_info['key'], conn)
                    
                    files_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error processing {file_info['key']}: {e}")
                    continue
            
            # Update metadata
            self.update_sync_metadata(conn, files_processed)
            
            logger.info(f"ETL job completed. Processed {files_processed} files")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"ETL job failed: {e}")
            raise


if __name__ == "__main__":
    etl = S3ToPostgresETL()
    etl.run()