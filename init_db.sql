-- Phase A: Operational Store
CREATE TABLE IF NOT EXISTS operational_credit_applications (
    application_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    requested_amount NUMERIC(12, 2) NOT NULL,
    declared_income NUMERIC(12, 2) NOT NULL,
    customer_age INT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Phase B: Batch Ingest Layer (Production Transactions)
CREATE TABLE IF NOT EXISTS batch_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL,
    transaction_date DATE NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    reference_code VARCHAR(100) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Governance & Observability: Auditing
CREATE TABLE IF NOT EXISTS pipeline_audits (
    audit_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(50) NOT NULL,            -- 'streaming_consumer' or 'batch_partner_transactions'
    execution_id VARCHAR(100) NOT NULL,            -- Session UUID or Airflow run_id
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    records_processed INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    status VARCHAR(20) NOT NULL,                   -- 'RUNNING', 'SUCCESS', 'FAILED'
    error_details TEXT
);

-- Governance & Observability: Dead Letter Queues (DLQ)
CREATE TABLE IF NOT EXISTS stream_dlq (
    dlq_id SERIAL PRIMARY KEY,
    raw_payload TEXT NOT NULL,
    error_details TEXT NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS batch_dlq (
    dlq_id SERIAL PRIMARY KEY,
    raw_payload TEXT NOT NULL,
    error_details TEXT NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indices for performance optimization
CREATE INDEX IF NOT EXISTS idx_ops_cust_id ON operational_credit_applications(customer_id);
CREATE INDEX IF NOT EXISTS idx_batch_ref_code ON batch_transactions(reference_code);
CREATE INDEX IF NOT EXISTS idx_audits_pipeline ON pipeline_audits(pipeline_name);
