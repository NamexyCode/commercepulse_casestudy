CommercePulse Data Pipeline
Overview
CommercePulse aggregates data for multi-vendor e-commerce platforms . It solves the challenge of merging historical JSON batch exports with high-volume live event streams characterized by out-of-order arrivals and evolving schemas.

Technical Architecture
1. Ingestion Layer (Raw)
Technology: MongoDB Atlas

Role: Acts as the immutable system of record.

Logic: Receives raw JSON events via API. No schema enforcement is applied here to ensure 100% data retention for auditing and reprocessing.

2. Transformation Layer (Normalized/Silver)
Technology: Python, pandas

Operations: * Incremental Extraction: Processes only new records since the last watermark timestamp.

Schema Normalization: Maps inconsistent vendor fields (e.g., user_id vs customerID) to a standard internal schema.

Deduplication: Implements deterministic hashing (SHA-256) of event attributes to generate unique IDs, ensuring idempotency.

Validation: Flags orphaned records and late-arriving refunds using windowing logic.

3. Analytics Layer (Warehouse/Gold)
Technology: Google BigQuery

Role: Serves as the optimized OLAP warehouse.

Strategy: * Fact Tables: Append-only logs for payments and refunds.

Dimension Tables: Upsert (Merge) strategy for mutable states like order status or user profiles.

Key Engineering Decisions
Idempotency: Every pipeline stage is re-runnable. If a job fails mid-way, re-executing it will not result in duplicate data or corrupted states.

Data Quality Framework: An automated pre-load check validates row counts and schema integrity. If anomalies exceed a 5% threshold, the load to BigQuery is halted and an alert is triggered.

Efficiency: By utilizing incremental loads, the pipeline minimizes I/O costs and BigQuery slot usage.

Tech Stack
Languages: Python (pandas),sql

Databases: MongoDB Atlas, Google BigQuery

Version Control: Git

Environment: Virtualenv 
Setup & Usage
Clone the repo: git clone https://github.com/NamexyCode/commercepulse_casestudy

Install dependencies: pip install -r requirements.txt

Configure Credentials: Add your service_account.json for BigQuery and MongoDB URI to .env.

Run Pipeline: python pipeline.py --mode incremental
