import os
import time
from datetime import datetime
from urllib.parse import urlparse

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryUpdateTableOperator,
    BigQueryUpdateTableSchemaOperator,
    BigQueryUpsertTableOperator,
)

START_DATE = datetime(2022, 2, 10)

PROJECT_ID = "thcdnadevdata"
BQ_LOCATION = "us-central1"

DATASET_NAME = "thc_dna_temp_staging"
LOCATION_DATASET_NAME = "thc_dna_temp_staging"
DATA_SAMPLE_GCS_URL = "gs://bkt-thc-dna-dev-daac-001/"

DATA_SAMPLE_GCS_URL_PARTS = urlparse(DATA_SAMPLE_GCS_URL)
DATA_SAMPLE_GCS_BUCKET_NAME = DATA_SAMPLE_GCS_URL_PARTS.netloc
DATA_SAMPLE_GCS_OBJECT_NAME = DATA_SAMPLE_GCS_URL_PARTS.path[1:]


with models.DAG(
    "connection_test",
    schedule_interval='@once',  # Override to match your needs
    start_date=START_DATE,
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_bigquery_create_table]
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    # [END howto_operator_bigquery_create_table]

    # [START howto_operator_bigquery_update_table_schema]
    update_table_schema = BigQueryUpdateTableSchemaOperator(
        task_id="update_table_schema",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields_updates=[
            {"name": "emp_name", "description": "Name of employee"},
            {"name": "salary", "description": "Monthly salary in USD"},
        ],
    )
    # [END howto_operator_bigquery_update_table_schema]

    # [START howto_operator_bigquery_delete_table]
    delete_table = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.test_table",
    )
    # [END howto_operator_bigquery_delete_table]

    # [START howto_operator_bigquery_create_view]
    create_view = BigQueryCreateEmptyTableOperator(
        task_id="create_view",
        dataset_id=DATASET_NAME,
        table_id="test_view",
        view={
            "query": f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.test_table`",
            "useLegacySql": False,
        },
    )
    # [END howto_operator_bigquery_create_view]

    # [START howto_operator_bigquery_delete_view]
    delete_view = BigQueryDeleteTableOperator(
        task_id="delete_view", deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.test_view"
    )
    # [END howto_operator_bigquery_delete_view]

    # [START howto_operator_bigquery_create_materialized_view]
    create_materialized_view = BigQueryCreateEmptyTableOperator(
        task_id="create_materialized_view",
        dataset_id=DATASET_NAME,
        table_id="test_materialized_view",
        materialized_view={
            "query": f"SELECT SUM(salary)  AS sum_salary FROM `{PROJECT_ID}.{DATASET_NAME}.test_table`",
            "enableRefresh": True,
            "refreshIntervalMs": 2000000,
        },
    )
    # [END howto_operator_bigquery_create_materialized_view]

    # [START howto_operator_bigquery_delete_materialized_view]
    delete_materialized_view = BigQueryDeleteTableOperator(
        task_id="delete_materialized_view",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.test_materialized_view",
    )
    # [END howto_operator_bigquery_delete_materialized_view]

    # [START howto_operator_bigquery_create_external_table]
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": "external_table",
            },
            "schema": {
                "fields": [
                    {"name": "name", "type": "STRING"},
                    {"name": "post_abbr", "type": "STRING"},
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "compression": "NONE",
                "csvOptions": {"skipLeadingRows": 1},
                "sourceUris": [DATA_SAMPLE_GCS_URL],
            },
        },
    )
    # [END howto_operator_bigquery_create_external_table]

    # [START howto_operator_bigquery_upsert_table]
    upsert_table = BigQueryUpsertTableOperator(
        task_id="upsert_table",
        dataset_id=DATASET_NAME,
        table_resource={
            "tableReference": {"tableId": "test_table_id"},
            "expirationTime": (int(time.time()) + 300) * 1000,
        },
    )
    # [END howto_operator_bigquery_upsert_table]

    # [START howto_operator_bigquery_create_dataset]
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create-dataset", dataset_id=DATASET_NAME)
    # [END howto_operator_bigquery_create_dataset]

    # [START howto_operator_bigquery_get_dataset_tables]
    get_dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id="get_dataset_tables", dataset_id=DATASET_NAME
    )
    # [END howto_operator_bigquery_get_dataset_tables]

    # [START howto_operator_bigquery_get_dataset]
    get_dataset = BigQueryGetDatasetOperator(task_id="get-dataset", dataset_id=DATASET_NAME)
    # [END howto_operator_bigquery_get_dataset]

    get_dataset_result = BashOperator(
        task_id="get_dataset_result",
        bash_command="echo \"{{ task_instance.xcom_pull('get-dataset')['id'] }}\"",
    )

    # [START howto_operator_bigquery_update_table]
    update_table = BigQueryUpdateTableOperator(
        task_id="update_table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        fields=["friendlyName", "description"],
        table_resource={
            "friendlyName": "Updated Table",
            "description": "Updated Table",
        },
    )
    # [END howto_operator_bigquery_update_table]

    # [START howto_operator_bigquery_update_dataset]
    update_dataset = BigQueryUpdateDatasetOperator(
        task_id="update_dataset",
        dataset_id=DATASET_NAME,
        dataset_resource={"description": "Updated dataset"},
    )
    # [END howto_operator_bigquery_update_dataset]

    # [START howto_operator_bigquery_delete_dataset]
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )
    # [END howto_operator_bigquery_delete_dataset]

    create_dataset >> update_dataset >> get_dataset >> get_dataset_result >> delete_dataset



    