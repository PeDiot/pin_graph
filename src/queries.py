from typing import Dict, Optional, List

from .enums.bigquery import *
from .enums.supabase import *


def make_bigquery_merge_query(
    target_table_ref: str,
    temp_table_ref: str,
    unique_field: str,
    rows: List[Dict],
) -> str:
    return f"""
    MERGE `{target_table_ref}` T
    USING `{temp_table_ref}` S
    ON T.{unique_field} = S.{unique_field}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(rows[0].keys())})
        VALUES ({', '.join([f'S.{field}' for field in rows[0].keys()])})
    """


def make_bigquery_board_pin_query(n: int, index: int = 0) -> str:
    offset = int(index * n)

    return f"""
    SELECT pinterest.user_id, board_pin.* EXCEPT (board_pin.pinterest_id)
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID_BOARD_PIN}` board_pin
    INNER JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID_PINTEREST}` pinterest USING (pinterest_id)
    LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID_PIN_VECTOR}` pin_vector
        ON board_pin.id = pin_vector.pin_id
    WHERE pin_vector.pin_id IS NULL
    LIMIT {n}
    OFFSET {offset};
    """


def make_bigquery_last_date_query(table_id: str) -> str:
    return f"""
    SELECT MAX(created_at) AS created_at
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{table_id}`
    """