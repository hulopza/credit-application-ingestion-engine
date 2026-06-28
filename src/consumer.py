import os
import sys
import json
import time
import uuid
import signal
import logging
import traceback
from datetime import datetime
from pathlib import Path
import boto3
import psycopg2
from pydantic import ValidationError
from schemas import CreditApplicationEvent

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Environment configuration
STREAM_NAME = os.getenv("STREAM_NAME", "credit_applications_stream")
KINESIS_ENDPOINT = os.getenv("KINESIS_ENDPOINT", "http://localhost:4566")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "challenge_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Global telemetry variables
session_id = str(uuid.uuid4())
start_time = datetime.now()
processed_count = 0
rejected_count = 0
audit_id = None
db_conn = None
shutdown_requested = False

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def init_audit_log():
    global audit_id, db_conn
    try:
        if not db_conn or db_conn.closed:
            db_conn = get_db_connection()
        with db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_audits 
                (pipeline_name, execution_id, start_time, records_processed, records_rejected, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING audit_id;
                """,
                ('streaming_consumer', session_id, start_time, 0, 0, 'RUNNING')
            )
            audit_id = cur.fetchone()[0]
            db_conn.commit()
            logger.info(f"Initialized audit log row (audit_id: {audit_id}) for session: {session_id}")
    except Exception as e:
        logger.error(f"Failed to initialize audit log: {e}")

def update_audit_log(status='RUNNING', error_details=None):
    global audit_id, db_conn, processed_count, rejected_count
    if audit_id is None:
        return
    try:
        if not db_conn or db_conn.closed:
            db_conn = get_db_connection()
        end_time = datetime.now() if status != 'RUNNING' else None
        with db_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_audits
                SET records_processed = %s,
                    records_rejected = %s,
                    status = %s,
                    end_time = COALESCE(%s, end_time),
                    error_details = COALESCE(%s, error_details)
                WHERE audit_id = %s;
                """,
                (processed_count, rejected_count, status, end_time, error_details, audit_id)
            )
            db_conn.commit()
            logger.info(f"Updated audit log status to {status} (Processed: {processed_count}, Rejected: {rejected_count})")
    except Exception as e:
        logger.error(f"Failed to update audit log: {e}")

def write_to_data_lake(raw_data: str):
    """
    Appends raw incoming JSON event to flat local files partitioned by hour:
    data/lake/year=YYYY/month=MM/day=DD/hour=HH/applications.jsonl
    """
    now = datetime.utcnow()
    dir_path = Path("data/lake") / f"year={now.year:04d}" / f"month={now.month:02d}" / f"day={now.day:02d}" / f"hour={now.hour:02d}"
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / "applications.jsonl"
    
    with open(file_path, "a") as f:
        f.write(raw_data + "\n")

def write_to_dlq(raw_data: str, error_msg: str):
    """
    Safely isolates non-compliant/corrupted data to flat files and DB DLQ table.
    """
    global db_conn, rejected_count
    rejected_count += 1
    
    # 1. Write to local file DLQ
    dlq_dir = Path("data/dlq/stream")
    dlq_dir.mkdir(parents=True, exist_ok=True)
    file_path = dlq_dir / "corrupt_events.jsonl"
    
    dlq_payload = {
        "raw_payload": raw_data,
        "error": error_msg,
        "failed_at": datetime.utcnow().isoformat()
    }
    with open(file_path, "a") as f:
        f.write(json.dumps(dlq_payload) + "\n")
        
    # 2. Write to Postgres DLQ Table
    try:
        if not db_conn or db_conn.closed:
            db_conn = get_db_connection()
        with db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stream_dlq (raw_payload, error_details)
                VALUES (%s, %s);
                """,
                (raw_data, error_msg)
            )
            db_conn.commit()
    except Exception as e:
        logger.error(f"Failed to insert into stream_dlq database table: {e}")

def save_to_operational_store(event: CreditApplicationEvent):
    """
    Persists the validated application to operational cache with idempotent UPSERT.
    """
    global db_conn, processed_count
    try:
        if not db_conn or db_conn.closed:
            db_conn = get_db_connection()
        with db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO operational_credit_applications
                (application_id, customer_id, requested_amount, declared_income, customer_age, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (application_id) DO NOTHING;
                """,
                (
                    event.application_id,
                    event.customer_id,
                    event.requested_amount,
                    event.declared_income,
                    event.customer_age,
                    event.timestamp
                )
            )
            db_conn.commit()
            if cur.rowcount > 0:
                processed_count += 1
    except Exception as e:
        logger.error(f"Database insertion failed for {event.application_id}: {e}")
        # If DB write fails (e.g. constraints not caught by Pydantic), isolate payload
        write_to_dlq(event.model_dump_json(), f"DB Write Failure: {str(e)}")

def process_record(record):
    raw_data = ""
    try:
        raw_data = record['Data'].decode('utf-8')
        # Step 1: Persistence of all raw events in local data lake
        write_to_data_lake(raw_data)
        
        # Step 2: Schema validation
        payload_dict = json.loads(raw_data)
        validated_event = CreditApplicationEvent(**payload_dict)
        
        # Step 3: Write to operational cache store (Idempotent)
        save_to_operational_store(validated_event)
        
    except ValidationError as ve:
        error_details = str(ve)
        logger.warning(f"Pydantic validation failed for record. Errors: {error_details}. Raw: {raw_data}")
        write_to_dlq(raw_data, f"ValidationError: {error_details}")
    except json.JSONDecodeError as jde:
        logger.warning(f"Failed to decode JSON: {raw_data}. Error: {str(jde)}")
        write_to_dlq(raw_data, f"JSONDecodeError: {str(jde)}")
    except Exception as e:
        logger.error(f"Unexpected record processing error: {e}")
        write_to_dlq(raw_data, f"Unexpected error: {str(e)}")

def handle_shutdown(signum, frame):
    global shutdown_requested
    logger.info("Termination signal received. Shutting down gracefully...")
    shutdown_requested = True

def run_consumer():
    global shutdown_requested, db_conn
    
    # Attach signal listeners for graceful termination
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    logger.info("Initializing pipeline database and telemetry logs...")
    init_audit_log()
    
    # Initialize AWS/LocalStack client configuration for Kinesis
    kinesis = boto3.client(
        'kinesis',
        endpoint_url=KINESIS_ENDPOINT,
        region_name='us-east-1',
        aws_access_key_id='mock',
        aws_secret_access_key='mock'
    )
    
    # Polling to await LocalStack stream creation by stream_generator
    logger.info(f"Awaiting stream '{STREAM_NAME}' activation...")
    while not shutdown_requested:
        try:
            kinesis.describe_stream(StreamName=STREAM_NAME)
            logger.info(f"Stream '{STREAM_NAME}' is active and ready.")
            break
        except kinesis.exceptions.ResourceNotFoundException:
            logger.info(f"Stream '{STREAM_NAME}' not found. Retrying in 5s (waiting for generator to create it)...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Connection check to LocalStack Kinesis failed: {e}. Retrying in 5s...")
            time.sleep(5)
            
    if shutdown_requested:
        update_audit_log('FAILED', 'Shutdown triggered prior to stream startup.')
        return

    # Obtain shard iterators from the stream
    try:
        stream_info = kinesis.describe_stream(StreamName=STREAM_NAME)
        shards = stream_info['StreamDescription']['Shards']
        shard_iterators = {}
        for s in shards:
            shard_id = s['ShardId']
            # Start polling from the beginning of stream history
            iterator_info = kinesis.get_shard_iterator(
                StreamName=STREAM_NAME,
                ShardId=shard_id,
                ShardIteratorType='TRIM_HORIZON'
            )
            shard_iterators[shard_id] = iterator_info['ShardIterator']
    except Exception as e:
        error_msg = f"Failed to initialize stream shard iterators: {str(e)}"
        logger.error(error_msg)
        update_audit_log('FAILED', f"{error_msg}\n{traceback.format_exc()}")
        return

    logger.info(f"Active consumer session established. Listening to {len(shard_iterators)} stream shard(s).")
    
    last_audit_update = time.time()
    
    while not shutdown_requested:
        records_fetched = 0
        for shard_id, iterator in list(shard_iterators.items()):
            if not iterator:
                continue
            
            try:
                response = kinesis.get_records(ShardIterator=iterator, Limit=100)
                records = response.get('Records', [])
                records_fetched += len(records)
                
                for r in records:
                    process_record(r)
                
                # Fetch next iterator pointer
                shard_iterators[shard_id] = response.get('NextShardIterator')
                
            except kinesis.exceptions.ExpiredIteratorException:
                logger.info(f"Shard iterator expired for shard {shard_id}. Regenerating...")
                try:
                    iterator_info = kinesis.get_shard_iterator(
                        StreamName=STREAM_NAME,
                        ShardId=shard_id,
                        ShardIteratorType='LATEST'
                    )
                    shard_iterators[shard_id] = iterator_info['ShardIterator']
                except Exception as ex:
                    logger.error(f"Failed to regenerate expired shard iterator: {ex}")
            except Exception as e:
                logger.error(f"Failed to fetch data from shard {shard_id}: {e}")
                time.sleep(2)
                
        # Update audit statistics dynamically every 10 seconds
        if time.time() - last_audit_update > 10:
            update_audit_log('RUNNING')
            last_audit_update = time.time()
            
        # Avoid heavy polling when stream is idle
        if records_fetched == 0:
            time.sleep(1)

    logger.info(f"Exiting consumer gracefully. Processed: {processed_count}, Rejected: {rejected_count}")
    update_audit_log('SUCCESS')
    if db_conn and not db_conn.closed:
        db_conn.close()

if __name__ == '__main__':
    try:
        run_consumer()
    except Exception as err:
        err_msg = f"Fatal system crash: {str(err)}"
        logger.fatal(err_msg)
        try:
            update_audit_log('FAILED', f"{err_msg}\n{traceback.format_exc()}")
        except Exception:
            pass
        if db_conn and not db_conn.closed:
            db_conn.close()
        sys.exit(1)
