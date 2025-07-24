# End-to-End-Sales-Analytics-Pipeline-Using-AWS-and-Snowflake-from-Woocommerce

This project is an end-to-end Data Engineering pipeline designed to ingest, process, and model sales data from WooCommerce, leveraging cloud technologies and modern ELT tools.

## Key Features:
Efficient raw data ingestion from AWS S3 into Snowflake using secure IAM roles and COPY commands

Snowflake staging and data warehouse setup with automated data loading scripts

Robust and modular data transformation pipeline built using dbt (Data Build Tool)

Comprehensive data quality testing with automated dbt tests to ensure reliability

Version-controlled codebase for collaboration, reproducibility, and maintainability

Documentation generated via dbt to provide transparency and model lineage

## Architecture

Data Ingestion:
Upload raw sales CSV files to AWS S3 buckets using IAM roles for secure access.

Snowflake Staging and Loading:
Define external stages and file formats in Snowflake, and run COPY commands to load raw data into staging tables.

Data Transformation (dbt):
Build tested and documented dbt models for staging, facts, and KPI metrics. Automate data cleaning, aggregation, and business logic.

Testing and Documentation:
Use dbt tests to enforce data quality and generate browsable documentation for the entire pipeline.

## Getting Started
Prerequisites
AWS account with access to S3 and IAM role configuration

Snowflake account with warehouse and database permissions

Python and dbt installed (pip install dbt-core dbt-snowflake)

Git for version control

## Setup Instructions

Upload raw data to S3
Use the provided scripts or manually upload CSV files to the configured S3 buckets.

Set up Snowflake
Run SQL scripts in data_ingestion/snowflake_staging to create stages, file formats, and load raw data.

Configure and run dbt

Set your profiles.yml with Snowflake credentials.

Install dependencies:
dbt deps
Run the transformation pipeline:

dbt run
dbt test
dbt docs generate
dbt docs serve

## Usage
Leverage dbt to maintain, test, and deploy transformations.

Access generated documentation to understand data flow and model relationships.

Connect BI tools to Snowflake for dashboarding and analysis.

## Contributing
Contributions and suggestions are welcome. Please raise issues or pull requests, keeping tests and documentation up to date.

License
This project is licensed under the MIT License. See LICENSE for details.

## Contact
Developed by Wasim Rana — reach out via LinkedIn or email for collaboration or questions.
