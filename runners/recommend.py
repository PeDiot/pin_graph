import sys

sys.path.append("../")

import src

from typing import Tuple, List, Dict, Optional
from tqdm import tqdm
from pinecone import Pinecone


ONLY_NEW_USERS = True
NUM_USERS = 1000
NUM_REFERENCE_VECTORS_MAX = 100
NUM_REFERENCE_VECTORS_MIN = 3
NUM_NEIGHBORS = 3
NUM_PREFETCH = 10
MIN_SIMILARITY_SCORE = 0.6
MAX_SIMILARITY_SCORE = 0.95
UPLOAD_EVERY = 50


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


def fetch_user_ids(
    only_new: bool,
    n: int, 
    index: Optional[int] = None
) -> List[str]:
    params = {"p_limit": n}

    if only_new:
        fn = src.enums.supabase.SUPABASE_RPC_ID_GET_DISTINCT_USERS_STRICT
    else:
        if index is None:
            return []
        
        fn = src.enums.supabase.SUPABASE_RPC_ID_GET_DISTINCT_USERS
        params["p_offset"] = int(index * n)

    response = spb_client.rpc(fn, params).execute()

    if response.data:
        return [data["user_id"] for data in response.data]

    return []


def fetch_reference_vectors(user_id: str) -> List[Dict]:
    query = src.queries.make_supabase_pin_vector_query(
        user_id=user_id, 
        n=NUM_REFERENCE_VECTORS_MAX, 
    )

    kwargs = {
        "fn": src.enums.supabase.SUPABASE_RPC_ID_GET_PIN_VECTORS,
        "params": {"query": query},
    }

    response = spb_client.rpc(**kwargs).execute()
    data = response.data

    if len(data) < NUM_REFERENCE_VECTORS_MIN:
        return []

    return data


def should_upload(ix: int, vectors_count: int) -> bool:
    return (ix % UPLOAD_EVERY == 0 and ix > 0) or ix == vectors_count - 1


def main():
    global spb_client, pc_index
    spb_client, pc_index = initialize_clients()

    pc_kwargs = {
        "index": pc_index,
        "n": NUM_NEIGHBORS + NUM_PREFETCH,
    }

    pin_kwargs = {
        "n": NUM_NEIGHBORS,
        "min_score": MIN_SIMILARITY_SCORE,
        "max_score": MAX_SIMILARITY_SCORE,
    }

    index = 0

    while True:
        user_ids = fetch_user_ids(
            only_new=ONLY_NEW_USERS,
            n=NUM_USERS, 
            index=index
        )

        index += 1

        if not user_ids:
            return

        for user_ix, user_id in enumerate(user_ids):
            vectors = fetch_reference_vectors(user_id)
            if not vectors:
                continue

            board_id = src.supabase.get_recommend_board_id(
                client=spb_client, user_id=user_id
            )

            if not board_id:
                continue

            image_urls = src.supabase.get_recommend_image_urls(
                client=spb_client,
                board_id=board_id,
            )

            loop = tqdm(
                iterable=enumerate(vectors),
                total=len(vectors),
                desc=f"User: {user_ix}",
            )

            pins, n, uploaded, success_rate = [], 0, 0, -1

            for ix, row in loop:
                vector = src.models.PinVector.from_dict(row)

                neighbors = src.pinecone.get_neighbors(
                    point_id=vector.point_id,
                    user_id=user_id,
                    image_urls=image_urls,
                    **pc_kwargs,
                )

                current_pins, image_urls = src.pinecone.postprocess_matches(
                    matches=neighbors,
                    board_id=board_id,
                    image_urls=image_urls,
                    **pin_kwargs,
                )

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
                    success_rate = uploaded / n if n > 0 else -1

                loop.set_description(
                    f"Batch: {index} | "
                    f"User: {user_ix} | "
                    f"Processed: {n} | "
                    f"Uploaded: {uploaded} | "
                    f"Success: {success_rate:.2f}"
                )

if __name__ == "__main__":
    main()
