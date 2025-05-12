from typing import List, Dict
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions


def init_client(url: str, key: str, schema: str = "public") -> Client:
    kwargs = {
        "supabase_url": url,
        "supabase_key": key,
    }

    if schema:
        options = ClientOptions().replace(schema=schema)
        kwargs["options"] = options

    return create_client(**kwargs)


def insert(
    client: Client,
    table_id: str,
    rows: List[Dict],
) -> bool:
    try:
        response = client.table(table_id).upsert(rows).execute()
        
        return len(response.data) == len(rows)

    except Exception as e:
        print(e)
        return False
