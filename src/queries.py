from typing import Dict, Optional

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
    from_pinterest: bool,
    n: Optional[int] = None,
    index: Optional[int] = None,
    is_premium: bool = False,
    is_top: bool = False,
) -> str:
    if is_premium:
        users_cte = _get_premium_users_cte()
        join_clause = "INNER JOIN users USING (user_id)"
    elif is_top:
        users_cte = _get_top_users_cte()
        join_clause = "INNER JOIN users USING (user_id)"
    else:
        users_cte = ""
        join_clause = ""

    if from_pinterest:
        return f"""
        WITH
        {users_cte}
        , {_get_pinterest_pins_cte()}
        SELECT pins.*
        FROM pins
        {join_clause}
        """

    else:
        query = f"""
        WITH 
        {users_cte}
        , {_get_pins_cte()}
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


def get_supabase_user_rpc_fn(
    is_premium: bool,
    is_top: bool,
    is_new: bool,
) -> str:
    if is_premium:
        fn = SUPABASE_RPC_ID_GET_DISTINCT_PREMIUM_USERS

    elif is_top:
        if is_new:
            fn = SUPABASE_RPC_ID_GET_DISTINCT_TOP_USERS_STRICT
        else:
            fn = SUPABASE_RPC_ID_GET_DISTINCT_TOP_USERS

    else:
        if is_new:
            fn = SUPABASE_RPC_ID_GET_DISTINCT_USERS_STRICT
        else:
            fn = SUPABASE_RPC_ID_GET_DISTINCT_USERS

    return fn


def get_supabase_user_rpc_params(
    is_premium: bool, is_new: bool, n: int, index: Optional[int] = None
) -> Dict:
    params = {
        "p_limit": n,
    }

    if (not is_new or is_premium) and index is not None:
        params["p_offset"] = int(index * n)

    return params


def _get_top_users_cte(
    click_out_threshold: int = 20,
    saved_item_threshold: int = 10,
) -> str:
    return f"""
    click_out_count AS (
        SELECT user_id, COUNT(*) n
        FROM {SUPABASE_SCHEMA_ID_PUBLIC}.click_out
        GROUP BY user_id
    )
    , saved_item_count AS (
        SELECT user_id, COUNT(*) n
        FROM {SUPABASE_SCHEMA_ID_PUBLIC}.saved_item
        GROUP BY user_id
    )
    , top_users AS (
        SELECT DISTINCT(user_id) FROM click_out_count WHERE n > {click_out_threshold}
        UNION ALL SELECT DISTINCT(user_id) FROM saved_item_count WHERE n > {saved_item_threshold}
    )
    , users AS (
        SELECT DISTINCT(user_id) FROM top_users
    )
    """


def _get_premium_users_cte() -> str:
    return f"""
    users AS (
        SELECT DISTINCT(user_id) 
        FROM {SUPABASE_SCHEMA_ID_PUBLIC}.subscription
    )
    """


def _get_pinterest_pins_cte() -> str:
    return f"""
    pins AS (
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
    )
    """


def _get_pins_cte() -> str:
    return f"""
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
        WHERE board.name != '{DEFAULT_RECOMMEND_BOARD_NAME}'
    )
    , remaining_pins AS (
        SELECT board_pins.*
        FROM board_pins
        LEFT JOIN {SUPABASE_SCHEMA_ID_RAW}.pin_vector ON board_pins.id = pin_vector.pin_id
        WHERE pin_vector.pin_id IS NULL
    )
    """
