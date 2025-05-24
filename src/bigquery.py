from typing import Dict, List, Tuple
import time

from google.cloud import bigquery
from google.oauth2 import service_account

from .queries import make_merge_query


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
    field_ids: List[str],
) -> Tuple[int, bool]:
    try:
        if not rows:
            return 0, False

        project_id = client.project
        target_table_ref = f"{project_id}.{dataset_id}.{table_id}"

        temp_table_ref, temp_table_id = _create_temp_table(
            client=client,
            dataset_id=dataset_id,
            table_id=table_id,
            target_table_ref=target_table_ref,
        )

        if not insert(
            client=client,
            dataset_id=dataset_id,
            table_id=temp_table_id,
            rows=rows,
        ):
            _cleanup_temp_table(client, temp_table_ref)
            return 0, False

        fields = list(rows[0].keys())

        num_inserted = _merge_tables(
            client=client,
            target_table_ref=target_table_ref,
            temp_table_ref=temp_table_ref,
            field_ids=field_ids,
            fields=fields,
        )

        _cleanup_temp_table(client, temp_table_ref)

        return num_inserted, True

    except:
        _cleanup_temp_table(client, temp_table_ref)

        return 0, False


def _create_temp_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    target_table_ref: str,
) -> Tuple[str, str]:
    project_id = client.project
    temp_table_id = f"temp_{table_id}_{int(time.time())}"
    temp_table_ref = f"{project_id}.{dataset_id}.{temp_table_id}"

    target_table = client.get_table(target_table_ref)
    schema = target_table.schema

    temp_table = bigquery.Table(temp_table_ref, schema=schema)
    client.create_table(temp_table, exists_ok=True)

    return temp_table_ref, temp_table_id


def _merge_tables(
    client: bigquery.Client,
    target_table_ref: str,
    temp_table_ref: str,
    field_ids: List[str],
    fields: List[str],
) -> int:
    query = make_merge_query(
        target_table_ref=target_table_ref,
        temp_table_ref=temp_table_ref,
        field_ids=field_ids,
        fields=fields,
    )

    query_job = client.query(query)
    result = query_job.result()

    return result.num_dml_affected_rows


def _cleanup_temp_table(client: bigquery.Client, temp_table_ref: str) -> None:
    try:
        client.delete_table(temp_table_ref)
    except:
        pass
