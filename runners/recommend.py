import sys

sys.path.append("../")

import src

from typing import Tuple, List, Iterable
import random
from tqdm import tqdm
from pinecone import Pinecone


NEW_USERS_ALPHA = 0.8
NUM_REFERENCE_VECTORS_MAX = 100
NUM_NEIGHBORS = 3
NUM_PREFETCH = 10
MIN_SIMILARITY_SCORE = 0.5
MAX_SIMILARITY_SCORE = 0.95


def initialize_clients() -> Tuple:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    bq_client = src.bigquery.init_client(secrets["GCP_CREDENTIALS"])

    pc_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pc_index = pc_client.Index(src.enums.PINECONE_INDEX_NAME)

    return bq_client, pc_index


def fetch_user_ids(is_new: bool) -> Iterable:
    query = src.queries.make_top_user_query(is_new)

    return bq_client.query(query).result()


def fetch_reference_vectors(user_id: str) -> Iterable:
    query_args = {
        "user_id": user_id,
        "n": NUM_REFERENCE_VECTORS_MAX,
        "shuffle": True,
    }

    query = src.queries.make_pin_vector_query(is_new=True, **query_args)

    response = bq_client.query(query).result()

    if response.total_rows == 0:
        query = src.queries.make_pin_vector_query(is_new=False, **query_args)

        response = bq_client.query(query).result()

    return response


def get_recommend_board_id(user_id: str) -> str:
    query = src.queries.make_recommend_board_id_query(user_id)
    response = bq_client.query(query).result()

    if response.total_rows == 0:
        board = src.models.Board(user_id=user_id)

        if src.bigquery.insert(
            client=bq_client,
            dataset_id=src.enums.bigquery.GCP_DATASET_ID_SUPABASE,
            table_id=src.enums.bigquery.GCP_TABLE_ID_BOARD_RECOMMEND,
            rows=[board.to_dict()],
        ):
            return board.id

    data = next(response)
    board = src.models.Board(**data)

    return board.id


def get_recommend_image_urls(board_id: str) -> List[str]:
    query = src.queries.make_recommend_image_urls_query(board_id)
    response = bq_client.query(query).result()

    return [row.image_url for row in response]


def process_user(
    user_id: str,
    pc_kwargs: dict,
    postprocess_kwargs: dict,
) -> Tuple[int, int]:
    loader_vectors = fetch_reference_vectors(user_id)
    if loader_vectors.total_rows == 0:
        return 0, 0, -1

    board_id = get_recommend_board_id(user_id)
    image_urls = get_recommend_image_urls(board_id)
    pins = []

    for row in loader_vectors:
        vector = src.models.PinVector.from_dict(dict(row))

        neighbors = src.pinecone.get_neighbors(
            point_id=vector.point_id,
            user_id=user_id,
            image_urls=image_urls,
            **pc_kwargs,
        )

        pins_, image_urls_ = src.pinecone.postprocess_matches(
            matches=neighbors,
            board_id=board_id,
            image_urls=image_urls,
            **postprocess_kwargs,
        )

        image_urls = image_urls_
        pins.extend(pins_)

    n_inserted, _ = src.bigquery.insert_unique(
        client=bq_client,
        dataset_id=src.enums.bigquery.GCP_DATASET_ID_SUPABASE,
        table_id=src.enums.bigquery.GCP_TABLE_ID_PIN_RECOMMEND,
        rows=pins,
        field_ids=["board_id", "image_url"],
    )
    
    return len(pins), n_inserted


def main():
    global bq_client, pc_index
    bq_client, pc_index = initialize_clients()

    pc_kwargs = {
        "index": pc_index,
        "n": NUM_NEIGHBORS + NUM_PREFETCH,
    }

    postprocess_kwargs = {
        "n": NUM_NEIGHBORS,
        "min_score": MIN_SIMILARITY_SCORE,
        "max_score": MAX_SIMILARITY_SCORE,
    }

    is_new = random.random() < NEW_USERS_ALPHA
    batch_ix = 0

    while True:
        loader_user_ids = fetch_user_ids(is_new)
        n, n_inserted, success_rate, user_ix = 0, 0, -1, 0

        loop = tqdm(
            iterable=loader_user_ids,
            total=loader_user_ids.total_rows,
            desc=f"Batch: {batch_ix}",
        )

        for row in loop:
            n_, n_inserted_ = process_user(
                user_id=row["user_id"],
                pc_kwargs=pc_kwargs,
                postprocess_kwargs=postprocess_kwargs,
            )
            
            n += n_
            n_inserted += n_inserted_
            success_rate = n_inserted / n if n > 0 else -1

            loop.set_description(
                f"Batch: {batch_ix} | "
                f"User: {user_ix} | "
                f"Processed: {n} | "
                f"Inserted: {n_inserted} | "
                f"Success: {success_rate:.2f}"
            )

            user_ix += 1
            

if __name__ == "__main__":
    main()
