import sys

sys.path.append("../")

import src

from typing import Tuple, List, Dict
from tqdm import tqdm
from pinecone import Pinecone


NUM_REFERENCE_VECTORS = 1
NUM_NEIGHBORS = 3
NUM_PREFETCH = 10
MIN_SIMILARITY_SCORE = 0.5
MAX_SIMILARITY_SCORE = 0.9
UPLOAD_EVERY = 5

USER_ID = "5c1f9f1d-d1d3-4b28-8a62-1cfbc0d1839f"


def initialize_clients() -> Tuple:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    spb_client = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
        schema=src.enums.SUPABASE_SCHEMA_ID_RAW,
    )

    pc_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pc_index = pc_client.Index(src.enums.PINECONE_INDEX_NAME)

    return spb_client, pc_index


def fetch_reference_vectors(user_id: str) -> List[Dict]:
    query = src.queries.make_supabase_pin_vector_query(
        user_id=user_id, n=NUM_REFERENCE_VECTORS, shuffle=False
    )

    kwargs = {
        "fn": src.enums.supabase.SUPABASE_RPC_ID_GET_PIN_VECTORS,
        "params": {"query": query},
    }

    response = spb_client.rpc(**kwargs).execute()

    return response.data


def should_upload(ix: int, vectors_count: int) -> bool:
    return (ix % UPLOAD_EVERY == 0 and ix > 0) or ix == vectors_count - 1


def main():
    global spb_client, pc_index
    spb_client, pc_index = initialize_clients()

    pc_kwargs = {
        "index": pc_index,
        "n": NUM_NEIGHBORS,
        "prefetch": NUM_PREFETCH,
        "min_score": MIN_SIMILARITY_SCORE,
        "max_score": MAX_SIMILARITY_SCORE,
    }

    vectors = fetch_reference_vectors(user_id=USER_ID)
    if not vectors:
        return

    board_id = src.supabase.get_recommend_board_id(client=spb_client, user_id=USER_ID)

    if not board_id:
        return

    pc_kwargs["user_id"] = USER_ID
    pc_kwargs["board_id"] = board_id

    loop = tqdm(iterable=enumerate(vectors), total=len(vectors))
    pins, n, uploaded, success_rate = [], 0, 0, -1

    for ix, row in loop:
        vector = src.models.PinVector.from_dict(row)

        current_pins = src.pinecone.get_neighbors(point_id=vector.point_id, **pc_kwargs)

        pins.extend(current_pins)
        n += len(current_pins)

        if should_upload(ix, len(vectors)):
            if src.supabase.insert(
                client=spb_client,
                table_id=src.enums.supabase.SUPABASE_TABLE_ID_PIN,
                rows=pins,
            ):
                uploaded += len(pins)

            pins = []
            success_rate = uploaded / n

        loop.set_description(
            f"Batch: {ix} | "
            f"Processed: {n} | "
            f"Uploaded: {uploaded} | "
            f"Success: {success_rate:.2f}"
        )


if __name__ == "__main__":
    main()
