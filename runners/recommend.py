import sys

sys.path.append("../")

import src

from typing import Tuple, List, Dict, Optional
import random, argparse
from tqdm import tqdm
from pinecone import Pinecone


NEW_USERS_ALPHA = 0.8
NUM_USERS = 1000
NUM_REFERENCE_VECTORS_MAX = 100
NUM_REFERENCE_VECTORS_MIN = 3
NUM_NEIGHBORS = 3
NUM_PREFETCH = 10
MIN_SIMILARITY_SCORE = 0.75
MAX_SIMILARITY_SCORE = 0.95
UPLOAD_EVERY = 50


def parse_args() -> Tuple[bool, bool]:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--is_premium",
        "-p",
        type=lambda x: x.lower() == "true",
        default=False,
    )
    parser.add_argument(
        "--is_top",
        "-t",
        type=lambda x: x.lower() == "true",
        default=False,
    )

    args = parser.parse_args()
    is_premium, is_top = args.is_premium, args.is_top

    if is_premium and is_top:
        raise ValueError("is_premium and is_top cannot both be True")

    return is_premium, is_top


def initialize_clients() -> Tuple:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    spb_client_raw = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
        schema=src.enums.SUPABASE_SCHEMA_ID_RAW,
    )

    spb_client_public = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
    )

    pc_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pc_index = pc_client.Index(src.enums.PINECONE_INDEX_NAME)

    return spb_client_raw, spb_client_public, pc_index


def is_new_users(is_premium: bool, is_top: bool) -> bool:
    if is_premium or is_top:
        return False

    else:
        return random.random() < NEW_USERS_ALPHA


def fetch_user_ids(
    is_premium: bool, is_top: bool, is_new: bool, n: int, index: Optional[int] = None
) -> List[str]:
    fn = src.queries.get_supabase_user_rpc_fn(is_premium, is_top, is_new)
    params = src.queries.get_supabase_user_rpc_params(is_premium, is_new, n, index)

    client = spb_client_public if is_premium or is_top else spb_client_raw
    response = src.supabase.execute_rpc(client, fn, params)

    if response.data:
        return [data["user_id"] for data in response.data]

    return []


def fetch_reference_vectors(user_id: str) -> List[Dict]:
    query_args = {
        "user_id": user_id,
        "n": NUM_REFERENCE_VECTORS_MAX,
    }

    query = src.queries.make_supabase_pin_vector_query(is_new=True, **query_args)

    kwargs = {
        "fn": src.enums.supabase.SUPABASE_RPC_ID_GET_PIN_VECTORS,
        "params": {"query": query},
    }

    response = src.supabase.execute_rpc(spb_client_raw, **kwargs)

    if len(response.data) < NUM_REFERENCE_VECTORS_MIN:
        query = src.queries.make_supabase_pin_vector_query(
            is_new=False, shuffle=True, **query_args
        )

        kwargs["params"]["query"] = query

        response = src.supabase.execute_rpc(spb_client_raw, **kwargs)

    if len(response.data) < NUM_REFERENCE_VECTORS_MIN:
        return []

    return response.data


def should_upload(ix: int, vectors_count: int) -> bool:
    return (ix % UPLOAD_EVERY == 0 and ix > 0) or ix == vectors_count - 1


def should_stop(
    is_new: bool, user_ids: List[str], count: int
) -> Tuple[bool, bool, bool]:
    stop, reset = False, False

    if not user_ids:
        if is_new:
            is_new, stop, reset = False, False, False

        elif count == 0:
            reset, stop = True, False

        else:
            stop, reset = True, False

    return stop, reset, is_new


def update_index(
    index: int,
    is_premium: bool,
    is_top: bool,
    is_new: bool,
    reset: bool,
) -> int:
    if is_new:
        return index + 1

    if is_premium or is_top:
        index = -1 if reset else index

    index = src.supabase.update_index(
        client=spb_client_raw,
        is_premium=is_premium,
        is_top=is_top,
        value=index,
    )

    return index


def main(is_premium: bool, is_top: bool):
    global spb_client_raw, spb_client_public, pc_index
    spb_client_raw, spb_client_public, pc_index = initialize_clients()

    pc_kwargs = {
        "index": pc_index,
        "n": NUM_NEIGHBORS + NUM_PREFETCH,
    }

    pin_kwargs = {
        "n": NUM_NEIGHBORS,
        "min_score": MIN_SIMILARITY_SCORE,
        "max_score": MAX_SIMILARITY_SCORE,
    }

    is_new = is_new_users(is_premium, is_top)
    count, index = 0, 0

    if not is_new and not is_premium and not is_top:
        index = src.supabase.get_index(
            client=spb_client_raw,
            is_premium=is_premium,
            is_top=is_top,
        )

    while True:
        user_ids = fetch_user_ids(is_premium, is_top, is_new, NUM_USERS, index)
        stop, reset, is_new = should_stop(is_new, user_ids, count)

        if stop:
            return

        index = update_index(index, is_premium, is_top, is_new, reset)
        count += 1
        n, uploaded, success_rate = 0, 0, -1

        loop = tqdm(
            iterable=enumerate(user_ids),
            total=len(user_ids),
            desc=f"Batch: {index}",
        )

        for user_ix, user_id in loop:
            vectors = fetch_reference_vectors(user_id)
            if not vectors:
                continue

            board_id = src.supabase.get_recommend_board_id(
                client=spb_client_raw, user_id=user_id
            )

            if not board_id:
                continue

            image_urls = src.supabase.get_recommend_image_urls(
                client=spb_client_raw,
                board_id=board_id,
            )

            pins = []

            for ix, row in enumerate(vectors):
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
                        client=spb_client_raw,
                        table_id=src.enums.supabase.SUPABASE_TABLE_ID_PIN,
                        rows=pins,
                    ):
                        uploaded += len(pins)

                    pins = []
                    success_rate = uploaded / n if n > 0 else -1

                loop.set_description(
                    f"Batch: {count-1} | "
                    f"User: {user_ix} | "
                    f"Processed: {n} | "
                    f"Uploaded: {uploaded} | "
                    f"Success: {success_rate:.2f}"
                )


if __name__ == "__main__":
    is_premium, is_top = parse_args()

    main(is_premium, is_top)
