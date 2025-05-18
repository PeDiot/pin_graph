from typing import List, Dict
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions


from .models import Pin, Board
from .enums.supabase import *
from .utils import execute_with_retry


def init_client(url: str, key: str, schema: str = "public") -> Client:
    kwargs = {
        "supabase_url": url,
        "supabase_key": key,
    }

    if schema:
        options = ClientOptions().replace(schema=schema)
        kwargs["options"] = options

    return create_client(**kwargs)


@execute_with_retry()
def execute_rpc(
    client: Client,
    fn: str,
    params: Dict,
) -> List[Dict]:
    return client.rpc(fn, params).execute()


@execute_with_retry()
def insert(
    client: Client,
    table_id: str,
    rows: List[Dict],
) -> bool:
    try:
        response = (
            client.table(table_id)
            .upsert(
                json=rows,
                ignore_duplicates=True,
            )
            .execute()
        )

        return len(response.data) > 0

    except Exception as e:
        print(e)
        return False


@execute_with_retry()
def get_recommend_board_id(client: Client, user_id: str) -> str:
    try:
        response = (
            client.table(SUPABASE_TABLE_ID_BOARD)
            .select("id")
            .eq("user_id", user_id)
            .execute()
        )

        if response.data:
            board_id = response.data[0]["id"]

            return board_id

        board = Board(user_id=user_id)

        success = insert(
            client=client, table_id=SUPABASE_TABLE_ID_BOARD, rows=[board.to_dict()]
        )

        if success:
            return board.id

        return None

    except Exception as e:
        print(e)
        return None


@execute_with_retry()
def get_recommend_image_urls(
    client: Client, board_id: str, n: int = SUPABASE_BATCH_SIZE
) -> List[Pin]:
    try:
        index = 0
        image_urls = []

        while True:
            offset = int(index * n)

            response = (
                client.table(SUPABASE_TABLE_ID_PIN)
                .select("image_url")
                .eq("board_id", board_id)
                .limit(n)
                .offset(offset)
                .execute()
            )

            image_urls.extend([data["image_url"] for data in response.data])

            if len(response.data) < n:
                return image_urls

            index += 1

    except Exception as e:
        print(e)
        return []


@execute_with_retry()
def get_index(
    client: Client,
    is_premium: bool,
    is_top: bool,
) -> int:
    try:
        response = (
            client.table(SUPABASE_TABLE_ID_INDEX)
            .select("value")
            .eq("is_premium", is_premium)
            .eq("is_top", is_top)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        if response.data:
            index = response.data[0]["value"]

            return index

        return 0

    except Exception as e:
        print(e)
        return 0


@execute_with_retry()
def update_index(
    client: Client,
    is_premium: bool,
    is_top: bool,
    value: int,
) -> int:
    try:
        new_value = value + 1

        response = (
            client.table(SUPABASE_TABLE_ID_INDEX)
            .update({"value": new_value})
            .eq("is_premium", is_premium)
            .eq("is_top", is_top)
            .execute()
        )

        if not response.data:
            row = {
                "is_premium": is_premium,
                "is_top": is_top,
                "value": new_value,
            }

            return insert(
                client=client,
                table_id=SUPABASE_TABLE_ID_INDEX,
                rows=[row],
            )

        return new_value

    except Exception as e:
        print(e)
        return 0
