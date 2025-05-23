from typing import List, Dict
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

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
def get_rows(client: Client, table_id: str, n: int, index: int) -> List[Dict]:
    offset = int(index * n)

    response = client.table(table_id).select("*").limit(n).offset(offset).execute()

    return response.data


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
