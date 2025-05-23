from typing import List

from .enums.bigquery import *
from .enums.supabase import *


def make_merge_query(
    target_table_ref: str,
    temp_table_ref: str,
    field_ids: List[str],
    fields: List[str],
) -> str:
    on_clause = " AND ".join([f"T.{field_id} = S.{field_id}" for field_id in field_ids])

    return f"""
    MERGE `{target_table_ref}` T
    USING `{temp_table_ref}` S
    ON {on_clause}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(fields)})
        VALUES ({', '.join([f'S.{field}' for field in fields])})
    """


def make_board_pin_query(n: int, index: int = 0) -> str:
    offset = int(index * n)

    return f"""
    SELECT pinterest.user_id, board_pin.* EXCEPT (pinterest_id)
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_BOARD_PIN}` board_pin
    INNER JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_PINTEREST}` pinterest USING (pinterest_id)
    LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_PIN_VECTOR}` pin_vector
        ON CONCAT(pinterest.user_id, board_pin.id) = pin_vector.id
    WHERE pin_vector.pin_id IS NULL
    LIMIT {n}
    OFFSET {offset};
    """


def make_last_date_query(table_id: str) -> str:
    return f"""
    SELECT MAX(created_at) AS created_at
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{table_id}`
    """


def make_top_user_query(is_new: bool) -> str:
    query = f"""
    WITH 
        click_outs AS (
        SELECT user_id, COUNT(*) n
        FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID_PROD}.{GCP_TABLE_ID_CLICK_OUT}`
        GROUP BY user_id
        )
        , saves AS (
        SELECT user_id, COUNT(*) n
        FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID_PROD}.{GCP_TABLE_ID_SAVED}`
        GROUP BY user_id
        )
        , users AS (
        SELECT user_id FROM click_outs WHERE n > 20
        UNION ALL
        SELECT user_id FROM saves WHERE n > 10
        )
    SELECT DISTINCT(user_id) 
    FROM users
    """

    if is_new:
        query += f"""
        LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_PIN_VECTOR}` pin_vector USING (user_id)
        WHERE pin_vector.user_id IS NULL;
        """
    else:
        query += f"ORDER BY RAND();"

    return query


def make_pin_vector_query(
    n: int,
    is_new: bool,
    user_id: str,
    shuffle: bool = False,
) -> str:
    base_query = f"""   
    SELECT pv.*
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_PIN_VECTOR}` pv
    """

    where_prefix = "WHERE"

    if is_new:
        base_query += f"""
        LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_PIN_RECOMMEND}` pin 
            USING (point_id)
        WHERE pin.id IS NULL
        """
        where_prefix = "AND"

    base_query += f" {where_prefix} pv.user_id = '{user_id}'"

    if shuffle:
        base_query += "\nORDER BY RAND()"

    if n:
        base_query += f"\nLIMIT {n}"

    return base_query


def make_recommend_board_id_query(user_id: str) -> str:
    return f"""
    SELECT *
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_BOARD_RECOMMEND}`
    WHERE user_id = '{user_id}'
    """


def make_recommend_image_urls_query(board_id: str) -> str:
    return f"""
    SELECT DISTINCT(image_url) AS image_url
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_PIN_RECOMMEND}`
    WHERE board_id = '{board_id}';
    """


def make_insert_board_query() -> str:
    return f"""
    SELECT board.* 
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_BOARD_RECOMMEND}` board
    LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_PIN_INSERTED}` inserted
        USING(id)
    WHERE inserted.id IS NULL;
    """


def make_insert_pin_query() -> str:
    return f"""
    SELECT pin.* 
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_PIN_RECOMMEND}` pin
    LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID_SUPABASE}.{GCP_TABLE_ID_PIN_INSERTED}` inserted
        USING(id)
    WHERE inserted.id IS NULL;
    """
