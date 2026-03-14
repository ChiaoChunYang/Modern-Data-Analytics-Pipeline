# Modern Data Analytics Pipeline

## Architecture Overview

This project implements a highly professional, enterprise-grade data analytics pipeline designed for scalability, reliability, and data quality. The architecture leverages the industry-standard **Modern Data Stack (MDS)**:

- **Snowflake**: The primary Cloud Data Warehouse, serving as the centralized source of truth. It utilizes role-based access control (RBAC) and isolated compute resources (warehouses) for optimal performance and security.
- **dbt (data build tool)**: Handles the "Transform" layer of the ELT process. It uses modular SQL to transform raw data into high-quality analytics-ready datasets, employing best practices such as version control, documentation, and automated testing.
- **Apache Airflow**: The orchestration engine responsible for scheduling and managing end-to-end data workflows, ensuring dependencies are respected and providing robust monitoring and alerting.

### Data Flow

1. **Extract & Load (EL)**: Raw data is ingested into Snowflake's `RAW` database from various sources (not covered in this repo's initialization but part of the broader strategy).
2. **Transform (T)**:
   - **Staging Layer**: Clean and rename raw data.
   - **Intermediate Layer**: Complex joins and business logic.
   - **Marts Layer**: Final, business-ready models optimized for consumption (e.g., `fct_campaign_performance`).
3. **Validation**: Automated tests run at every stage (dbt tests and custom quality checks) to ensure data integrity.
4. **Orchestration**: Airflow triggers dbt jobs and potentially other tasks (e.g., data quality checks, external API calls).

## Project Structure

- `airflow/`: Contains DAGs for workflow orchestration.
- `dbt/`: The core transformation logic, including models, macros, and configuration.
- `snowflake/`: SQL scripts for database initialization and administrative tasks.
- `tests/`: Custom Python-based data quality tests using `pytest`.
- `requirements.txt`: Python dependencies for the project.

## Getting Started

1. Set up your Snowflake environment using the scripts in `snowflake/setup/`.
2. Install Python dependencies: `pip install -r requirements.txt`.
3. Configure your `profiles.yml` for dbt to connect to Snowflake.
4. Deploy the dbt models: `dbt run`.
5. Schedule the pipeline in Airflow.
