from typing import Dict, List, Tuple
import time

from google.cloud import bigquery
from google.oauth2 import service_account

from .queries import make_bigquery_merge_query


def init_client(credentials_dict: Dict) -> bigquery.Client:
    credentials_dict["private_key"] = credentials_dict["private_key"].replace(
        "\\n", "\n"
    )

    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict
    )

    return bigquery.Client(
        credentials=credentials, project=credentials_dict["project_id"]
    )


def insert(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    rows: List[Dict],
) -> bool:
    try:
        errors = client.insert_rows_json(
            table=f"{dataset_id}.{table_id}", json_rows=rows
        )

        return len(errors) == 0

    except:
        return False


def insert_unique(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    rows: List[Dict],
    unique_field: str,
) -> Tuple[int, bool]:
    try:
        if not rows:
            return 0

        project_id = client.project
        temp_table_id = f"temp_{table_id}_{int(time.time())}"
        temp_table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"
        target_table_ref = f"{project_id}.{dataset_id}.{table_id}"

        target_table = client.get_table(target_table_ref)
        schema = target_table.schema

        temp_table = bigquery.Table(temp_table_ref, schema=schema)
        client.create_table(temp_table, exists_ok=True)

        errors = client.insert_rows_json(temp_table_ref, rows)
        if errors:
            client.delete_table(temp_table_ref)

            return 0, False

        query = make_bigquery_merge_query(
            target_table_ref=target_table_ref,
            temp_table_ref=temp_table_ref,
            unique_field=unique_field,
            rows=rows,
        )

        query_job = client.query(query)
        result = query_job.result()
        num_inserted = result.num_dml_affected_rows

        client.delete_table(temp_table_ref)

        return num_inserted, True

    except Exception as e:
        print(f"Error in insert_unique_rows: {str(e)}")

        try:
            client.delete_table(temp_table_ref)
        except:
            pass

        return 0, False
