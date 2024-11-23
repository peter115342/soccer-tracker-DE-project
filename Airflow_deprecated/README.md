# Airflow  (Deprecated)

I have decided to discontinue the use of Airflow in this data pipeline due to its complexity and cost implications in GCP. Instead, I have migrated our workflows to use Google Cloud Functions and Cloud Scheduler for a simpler and more cost-effective solution.

Despite this change, I am  keeping the existing Airflow DAGs and the `Dockerfile` in the repository.

## Contents

- `dags/` - Contains the Airflow DAGs:
  - `fetch_league_data_dag.py`
  - `fetch_football_data_dag.py`
- `Dockerfile` - Used for setting up the Airflow environment.

## Note

These files are no longer actively used in the project but may serve as a useful reference for how the data pipelines were previously structured with Airflow.
