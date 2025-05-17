from typing import Optional

from .enums.bigquery import *
from .enums.supabase import *


def make_bigquery_board_query(n: int, index: int = 0) -> str:
    offset = int(index * n)

    return f"""
    SELECT board.*
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID_BOARD}` board
    LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID_BOARD_PROCESSED}` processed USING (id)
    WHERE processed.id IS NULL
    LIMIT {n}
    OFFSET {offset};
    """


def make_bigquery_pin_query(n: int, index: int = 0) -> str:
    offset = int(index * n)

    return f"""
    SELECT pin.*
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID_PIN}` pin
    LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID_PIN_PROCESSED}` processed USING (id)
    WHERE processed.id IS NULL
    LIMIT {n}
    OFFSET {offset};
    """


def make_supabase_pin_query(
    from_pinterest: bool, n: Optional[int] = None, index: Optional[int] = None
) -> str:
    if from_pinterest:
        query = f"""
        SELECT 
        pinterest.user_id,
        pin.board_id,
        board.name as board_name,
        pin.id,
        pin.created_at,
        pin.image_url,
        pin.title,   
        true as from_pinterest
        FROM {SUPABASE_SCHEMA_ID_BIGQUERY}.pin
        INNER JOIN {SUPABASE_SCHEMA_ID_BIGQUERY}.board ON pin.board_id = board.id
        INNER JOIN {SUPABASE_SCHEMA_ID_PUBLIC}.pinterest ON pinterest.pinterest_id = board.pinterest_id
        LEFT JOIN {SUPABASE_SCHEMA_ID_RAW}.pin_vector ON pin.id = raw.pin_vector.pin_id
        WHERE pin_vector.pin_id IS NULL
        """

    else:
        query = f"""
        WITH 
            board_pins AS (
            SELECT 
            board.user_id,
            pin.board_id,
            board.name as board_name,
            CAST(pin.id AS TEXT),
            pin.created_at,
            pin.image_url,
            pin.title,
            false as from_pinterest
            FROM {SUPABASE_SCHEMA_ID_PUBLIC}.board
            INNER JOIN {SUPABASE_SCHEMA_ID_PUBLIC}.pin ON board.id = pin.board_id
            )
            , remaining_pins AS (
            SELECT board_pins.*
            FROM board_pins
            LEFT JOIN {SUPABASE_SCHEMA_ID_RAW}.pin_vector ON board_pins.id = pin_vector.pin_id
            WHERE pin_vector.pin_id IS NULL
            )
        SELECT * FROM remaining_pins
        """

    if n:
        query += f"LIMIT {n}"

        if index:
            offset = int(index * n)
            query += f"OFFSET {offset}"

    return query


def make_supabase_pin_vector_query(
    n: int,
    is_new: bool,
    user_id: Optional[str] = None,
    index: int = 0,
    shuffle: bool = False,
) -> str:
    base_query = f"""   
    SELECT pv.*
    FROM {SUPABASE_SCHEMA_ID_RAW}.pin_vector pv
    """
    where_prefix = "WHERE"

    if is_new:
        base_query += f"""
        LEFT JOIN {SUPABASE_SCHEMA_ID_RAW}.pin USING (point_id)
        WHERE pin.id IS NULL
        """
        where_prefix = "AND"

    if user_id:
        base_query += f" {where_prefix} pv.user_id = '{user_id}'"

    if shuffle:
        base_query += "\nORDER BY RANDOM()"

    if n:
        base_query += f"\nLIMIT {n}"

        if index:
            offset = int(index * n)
            base_query += f"\nOFFSET {offset}"

    return base_query
