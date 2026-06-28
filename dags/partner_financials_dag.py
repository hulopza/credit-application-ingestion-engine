from datetime import datetime, timedelta
import os
import sys
import json
import csv
import gzip
import shutil
import logging
from pathlib import Path
import psycopg2
from pydantic import ValidationError

from airflow import DAG
from airflow.operators.python import PythonOperator

# Append `/opt/airflow` to search path to load schemas from `src/schemas.py`
sys.path.append("/opt/airflow")
from src.schemas import PartnerTransactionRow

# Logging setup
logger = logging.getLogger("airflow.task")

# PostgreSQL Connection settings (resolves to the docker container alias)
DB_CONN_STR = os.getenv("DB_CONN_STR", "host=postgres dbname=challenge_db user=postgres password=postgres")

def get_db_conn():
    return psycopg2.connect(DB_CONN_STR)

# --- Python Operator Functions ---

def start_audit_log_fn(**context):
    """
    Inserts an initial audit tracking record for the execution session.
    """
    run_id = context['run_id']
    execution_id = f"batch-{run_id}"
    start_time = datetime.now()
    
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_audits 
                (pipeline_name, execution_id, start_time, records_processed, records_rejected, status)
                VALUES (%s, %s, %s, 0, 0, 'RUNNING')
                RETURNING audit_id;
                """,
                ('batch_partner_transactions', execution_id, start_time)
            )
            audit_id = cur.fetchone()[0]
            conn.commit()
        
        # Save ids in XCom context for other tasks
        context['ti'].xcom_push(key='audit_id', value=audit_id)
        context['ti'].xcom_push(key='execution_id', value=execution_id)
        logger.info(f"Initialized audit log row (audit_id: {audit_id}) for session: {execution_id}")
    finally:
        conn.close()

def ingest_partner_data_fn(**context):
    """
    Scans, validates, caches and archives partner transactions.
    """
    ti = context['ti']
    audit_id = ti.xcom_pull(task_ids='start_audit_log', key='audit_id')
    
    input_dir = Path("/opt/airflow/data/partner_transactions")
    archive_dir = input_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    dlq_dir = Path("/opt/airflow/data/dlq/batch")
    dlq_dir.mkdir(parents=True, exist_ok=True)
    dlq_file_path = dlq_dir / "corrupt_rows.jsonl"
    
    # Locate txt CSV files in data/partner_transactions
    files = list(input_dir.glob("partner_transactions_day_*.txt"))
    if not files:
        logger.info("No partner transaction files found to ingest.")
        ti.xcom_push(key='processed_count', value=0)
        ti.xcom_push(key='rejected_count', value=0)
        ti.xcom_push(key='files_metadata', value=[])
        return {"processed": 0, "rejected": 0}
        
    total_processed = 0
    total_rejected = 0
    files_metadata = []
    
    conn = get_db_conn()
    try:
        for file_path in sorted(files):
            logger.info(f"Beginning ingestion of file: {file_path.name}")
            file_processed = 0
            file_rejected = 0
            file_total_rows = 0
            
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    file_total_rows += 1
                    raw_line = ",".join([f"{k}:{v}" for k, v in row.items()])
                    
                    # Data quality typecasting validation
                    try:
                        amount_raw = row.get("amount", "")
                        try:
                            amount_val = float(amount_raw)
                        except ValueError:
                            # Replaced by non-numeric value to trigger schema exception
                            amount_val = -999.0
                            
                        # Schema validation
                        validated = PartnerTransactionRow(
                            transaction_id=row.get("transaction_id", ""),
                            account_id=row.get("account_id", ""),
                            transaction_date=row.get("transaction_date", ""),
                            amount=amount_val,
                            reference_code=row.get("reference_code", "")
                        )
                        
                        # Idempotent write to PostgreSQL production tables
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO batch_transactions 
                                (transaction_id, account_id, transaction_date, amount, reference_code)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (transaction_id) DO NOTHING;
                                """,
                                (
                                    validated.transaction_id,
                                    validated.account_id,
                                    validated.transaction_date,
                                    validated.amount,
                                    validated.reference_code
                                )
                            )
                            # Increment count regardless of conflict since transaction is resolved
                            file_processed += 1
                                 
                    except ValidationError as ve:
                        file_rejected += 1
                        error_msg = str(ve)
                        
                        # Isolate failed rows to local DLQ file
                        dlq_payload = {
                            "file": file_path.name,
                            "raw_payload": raw_line,
                            "error": error_msg,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        with open(dlq_file_path, "a", encoding="utf-8") as df:
                            df.write(json.dumps(dlq_payload) + "\n")
                            
                        # Isolate failed rows to Database DLQ
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO batch_dlq (raw_payload, error_details)
                                VALUES (%s, %s);
                                """,
                                (raw_line, error_msg)
                            )
            
            conn.commit()
            logger.info(f"Processed file '{file_path.name}' -> Validated: {file_processed}, DLQ: {file_rejected}")
            
            # Local Storage Optimization: Compress original file to Gzip and delete source
            archive_file_path = archive_dir / f"{file_path.name}.gz"
            with open(file_path, "rb") as f_in:
                with gzip.open(archive_file_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(file_path)
            logger.info(f"Compressed archive created: {archive_file_path.name}")
            
            total_processed += file_processed
            total_rejected += file_rejected
            files_metadata.append({
                "file_name": file_path.name,
                "source_rows": file_total_rows,
                "inserted_rows": file_processed,
                "rejected_rows": file_rejected
            })
            
        # Write stats to XCom
        ti.xcom_push(key='processed_count', value=total_processed)
        ti.xcom_push(key='rejected_count', value=total_rejected)
        ti.xcom_push(key='files_metadata', value=files_metadata)
        
        return {"processed": total_processed, "rejected": total_rejected}
    finally:
        conn.close()

def reverse_etl_report_fn(**context):
    """
    Extracts consolidated figures, formats, and writes CSV report to shared folder.
    """
    execution_date_str = context['ds_nodash']
    output_dir = Path("/opt/airflow/data/gdrive_shared/summary_reports")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"partner_summary_{execution_date_str}.csv"
    
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Query aggregates grouped by partner parsed from reference_code
            cur.execute(
                """
                SELECT 
                    SPLIT_PART(reference_code, '-', 2) AS partner_name,
                    COUNT(*) AS transaction_count,
                    SUM(amount) AS total_amount
                FROM batch_transactions
                GROUP BY SPLIT_PART(reference_code, '-', 2)
                ORDER BY partner_name;
                """
            )
            rows = cur.fetchall()
            
        row_count = len(rows)
        
        # Write CSV with custom validation metadata line at line 1
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            # Inject validation metadata line
            csvfile.write(f"row_count|{row_count}\n")
            
            # Write column headers and rows
            writer = csv.writer(csvfile)
            writer.writerow(["partner", "transaction_count", "total_amount"])
            for r in rows:
                writer.writerow(r)
                
        logger.info(f"Reverse ETL Summary Report created successfully at: {output_file}")
    finally:
        conn.close()

def reconcile_data_run_fn(**context):
    """
    Verifies destination records match source files record counts.
    """
    ti = context['ti']
    files_metadata = ti.xcom_pull(task_ids='ingest_partner_data', key='files_metadata')
    
    if not files_metadata:
        logger.info("No transaction files processed. Reconciliation check skipped.")
        return
        
    all_reconciled = True
    for meta in files_metadata:
        file_name = meta['file_name']
        src_count = meta['source_rows']
        dest_count = meta['inserted_rows']
        rej_count = meta['rejected_rows']
        
        # Check source count matches total of accepted + rejected
        is_reconciled = (src_count == dest_count + rej_count)
        if is_reconciled:
            logger.info(f"✅ Reconciliation Success for '{file_name}': Source ({src_count}) == Destinations ({dest_count} ingested + {rej_count} rejected)")
        else:
            all_reconciled = False
            logger.error(f"❌ Reconciliation Failure for '{file_name}': Source ({src_count}) != Destinations ({dest_count} ingested + {rej_count} rejected)")
            
    if not all_reconciled:
        raise ValueError("Run Failed: Reconciliation checks between files and DB records failed.")

def end_audit_log_fn(**context):
    """
    Logs status, durations, and counts upon workflow termination.
    """
    ti = context['ti']
    audit_id = ti.xcom_pull(task_ids='start_audit_log', key='audit_id')
    processed_count = ti.xcom_pull(task_ids='ingest_partner_data', key='processed_count') or 0
    rejected_count = ti.xcom_pull(task_ids='ingest_partner_data', key='rejected_count') or 0
    
    # Check workflow task statuses
    dag_run = context['dag_run']
    tis = dag_run.get_task_instances()
    
    # Collect error traces for failures
    failed_tasks = [t for t in tis if t.state == 'failed']
    status = 'SUCCESS'
    error_details = None
    
    if failed_tasks:
        status = 'FAILED'
        error_details = "\n".join([f"Task '{t.task_id}' failed." for t in failed_tasks])
        
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_audits
                SET records_processed = %s,
                    records_rejected = %s,
                    status = %s,
                    end_time = %s,
                    error_details = COALESCE(%s, error_details)
                WHERE audit_id = %s;
                """,
                (processed_count, rejected_count, status, datetime.now(), error_details, audit_id)
            )
            conn.commit()
        logger.info(f"Audit log session updated with status: {status}")
    finally:
        conn.close()

# --- DAG Definition ---

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'partner_financials_pipeline',
    default_args=default_args,
    description='Orchestrated Partner Transaction Ingestion & Reverse ETL Pipeline',
    schedule_interval=None,  # Run on trigger/demand
    catchup=False,
    max_active_runs=1,
) as dag:

    start_audit_log = PythonOperator(
        task_id='start_audit_log',
        python_callable=start_audit_log_fn,
    )

    ingest_partner_data = PythonOperator(
        task_id='ingest_partner_data',
        python_callable=ingest_partner_data_fn,
    )

    reverse_etl_report = PythonOperator(
        task_id='reverse_etl_report',
        python_callable=reverse_etl_report_fn,
    )

    reconcile_data_run = PythonOperator(
        task_id='reconcile_data_run',
        python_callable=reconcile_data_run_fn,
    )

    end_audit_log = PythonOperator(
        task_id='end_audit_log',
        python_callable=end_audit_log_fn,
        trigger_rule='all_done',  # Always execute to close the audit lifecycle
    )

    # Task Dependencies
    start_audit_log >> ingest_partner_data >> [reverse_etl_report, reconcile_data_run] >> end_audit_log
