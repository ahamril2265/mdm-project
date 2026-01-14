# MDM (Master Data Management) Pipeline

This repository contains a complete, end-to-end Master Data Management (MDM) pipeline built with Python, PostgreSQL, and Minio. The project demonstrates a phased approach to ingesting customer data from disparate sources, cleaning it, resolving identities, and creating a single, consolidated "golden record" for each customer.

The pipeline processes mock customer data from three systems:
*   **Sales**: Customer orders with email and name.
*   **Support**: Support tickets with email.
*   **Marketing**: Leads with name and phone number.

The entire process is orchestrated by a single shell script and is designed to be fully reproducible using Docker.

## Architecture

The project uses a simple, robust architecture for data processing and storage:

*   **Orchestration**: A master shell script (`run_mdm_pipeline.sh`) executes the pipeline phases in sequence.
*   **Data Producers**: Python scripts that generate realistic, event-driven data and push it to a data lake.
*   **Data Lake (Object Storage)**: **Minio** is used to store raw, partitioned JSONL data from the producers.
*   **Data Warehouse**: **PostgreSQL** serves as the data warehouse, organized into a multi-layered schema (`raw`, `staging`, `identity`, `gold`) that reflects the data's journey.
*   **Infrastructure**: **Docker Compose** is used to manage and run the Postgres and Minio services.

## Pipeline Execution Flow

The pipeline is broken down into distinct, sequential phases managed by `run_mdm_pipeline.sh`.

#### Phase 0: Infrastructure & Schema
Sets up the required infrastructure using `docker-compose up`. This starts the PostgreSQL and Minio containers. It then executes a series of SQL scripts located in `db/init/` to create schemas, tables, views, and indexes.

#### Phase 1: Data Production
Python scripts (`producers/*.py`) simulate data generation from Sales, Support, and Marketing systems. They create JSONL files containing customer events and upload them to a Minio bucket, partitioned by date and hour.

#### Phase 2: Ingestion
The `ingestion/minio_to_raw.py` script scans the Minio bucket for new data using a watermark system. It ingests new JSONL files into the corresponding tables in the `raw` PostgreSQL schema.

#### Phase 3: Staging & Normalization
The `staging/run_staging.py` script reads from the `raw` tables and performs basic cleaning and normalization (e.g., trimming whitespace, converting to lowercase). The cleaned data is loaded into the `staging` schema.

#### Phase 4: Identity Inputs
A unified view of all customer touchpoints is created by `staging/run_identity_inputs.py`. This script combines data from the various staging tables into a single `staging.identity_inputs` table, which serves as the foundation for the matching process.

#### Phase 4.5: Blocking
To optimize the matching process, this SQL-based step (`db/init/040_blocking_tables.sql`) generates a set of candidate pairs. It uses a deterministic blocking strategy (exact match on email or phone) to drastically reduce the number of pairs that need to be compared in the next phase.

#### Phase 5: Matching Engine
The core identity matching logic resides in `matching/run_matching_engine.py`. This script iterates through the blocked candidate pairs, calculates a similarity score based on email, phone, and name, and assigns a decision: `AUTO_MERGE`, `FLAG_REVIEW`, or `REJECT`.

#### Phase 6: Identity Resolution
Using the `AUTO_MERGE` and `FLAG_REVIEW` decisions from the matching engine, `identity/run_identity_resolution.py` builds an identity graph. It assigns a unique `global_customer_id` to all source records that are determined to belong to the same entity, storing these mappings in `identity.customer_identity_map`.

#### Phase 7: Golden Record Construction
The `gold/run_golden_customers.py` script constructs the master `gold.dim_customers` table. For each `global_customer_id`, it applies survivorship rules (e.g., 'priority', 'most_frequent') to select the best attribute values from the cluster of source records, creating a single "golden" view of each customer.

#### Phase 8: Golden Record History (SCD2)
To track changes over time, `gold/run_golden_history.py` implements a Slowly Changing Dimension Type 2 (SCD2) methodology. It compares the current golden records with the previous state, expiring old records and inserting new versions into `gold.dim_customers_history`.

#### Phase 9: Governance & Change Tracking
*   **9A - Change Data Capture (CDC)**: The `gold/run_golden_cdc.py` script compares the new and old versions from the SCD2 table to generate explicit change events (`INSERT`, `UPDATE`) in the `gold.customer_change_events` table.
*   **9B - Steward Overrides**: The pipeline applies manual corrections via `gold/run_steward_overrides.py`, which updates `gold.dim_customers` based on active rules in the `gold.customer_steward_overrides` table.

#### Phase 10: Data Quality & Reporting
The final phase focuses on monitoring and governance.
*   `gold/run_conflict_detection.py`: Detects attribute conflicts within a single customer entity (e.g., one customer with multiple emails).
*   `gold/run_quality_metrics.py`: Calculates and stores metrics about the matching process.
*   A `gold.steward_review_queue` view is available to flag records that require manual review due to conflicts, a high number of linked records, or active overrides.

## Data Model

The PostgreSQL database is structured into four primary schemas, representing a medallion-style architecture:

*   **`raw`**: Contains tables that are a direct copy of the data ingested from Minio, with minimal processing.
*   **`staging`**: Holds cleaned, normalized, and standardized data, ready for downstream processing. This is where transformations like data typing and basic cleaning occur.
*   **`identity`**: A schema dedicated to the identity resolution process. Its key table, `customer_identity_map`, links source system records to the `global_customer_id`.
*   **`gold`**: The final, curated schema. It contains the master customer dimension (`dim_customers`), its SCD2 history (`dim_customers_history`), and various governance tables for metrics, conflicts, and change events.

## Getting Started

Follow these instructions to run the entire MDM pipeline on your local machine.

### Prerequisites

*   Docker and Docker Compose
*   Python 3.8+
*   An environment that supports shell scripts (`.sh`)

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/ahamril2265/mdm-project.git
    cd mdm-project
    ```

2.  **Create an environment file:**
    Create a file named `.env` in the root of the project and add the following configuration. These values will be used by Docker Compose and the Python scripts.
    ```env
    # PostgreSQL Config
    POSTGRES_USER=mdm_user
    POSTGRES_PASSWORD=mdm_password
    POSTGRES_DB=mdm_db
    POSTGRES_PORT=5433

    # Minio Config
    MINIO_ROOT_USER=minioadmin
    MINIO_ROOT_PASSWORD=minioadmin
    ```

3.  **Install Python dependencies:**
    It is recommended to use a virtual environment.
    ```bash
    python -m venv venv
    source venv/bin/activate
    pip install sqlalchemy "psycopg2-binary" python-dotenv minio tqdm
    ```

### Running the Pipeline

The entire pipeline can be executed with a single command. The script handles starting the services, running all data processing phases, and providing progress updates.

1.  **Make the script executable:**
    ```bash
    chmod +x run_mdm_pipeline.sh
    ```

2.  **Run the script:**
    ```bash
    ./run_mdm_pipeline.sh
    ```

Upon completion, you can connect to the PostgreSQL database on port `5433` to explore the `raw`, `staging`, `identity`, and `gold` schemas. You can also access the Minio console at `http://localhost:9001` to view the raw ingested files.

