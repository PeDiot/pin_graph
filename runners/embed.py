import sys

sys.path.append("../")


from typing import List, Iterable, Tuple

from tqdm import tqdm
from PIL import Image
from pinecone import Pinecone

import src


BATCH_SIZE = 64


def initialize_clients() -> Tuple:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    bq_client = src.bigquery.init_client(secrets["GCP_CREDENTIALS"])

    pc_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pc_index = pc_client.Index(src.enums.PINECONE_INDEX_NAME)

    return bq_client, pc_index


def fetch_pins() -> Iterable:
    query = src.queries.make_board_pin_query()

    return bq_client.query(query).result()


def process_batch(
    pins: List[src.models.Pin],
    images: List[Image.Image],
) -> Tuple[bool, bool, int]:
    try:
        embeddings = encoder.encode(images)

    except Exception as e:
        pins_valid, embeddings = [], []

        for pin, image in zip(pins, images):
            try:
                embedding = encoder.encode([image])
                pins_valid.append(pin)
                embeddings.append(embedding)

            except Exception as e:
                continue

        pins = pins_valid

    pin_vectors, vectors = [], []

    for pin, embedding in zip(pins, embeddings):
        metadata = pin.to_dict()
        vector = src.models.Vector(values=embedding, metadata=metadata)

        pin_vector = src.models.PinVector(
            user_id=pin.user_id,
            pin_id=pin.id,
            point_id=vector.id,
        )

        pin_vectors.append(pin_vector.to_dict())
        vectors.append(vector.to_dict())

    pc_success = src.pinecone.insert(
        index=pc_index,
        vectors=vectors,
    )

    bq_success, num_inserted = False, 0

    if pc_success:
        num_inserted, bq_success = src.bigquery.insert_unique(
            client=bq_client,
            dataset_id=src.enums.bigquery.GCP_DATASET_ID_SUPABASE,
            table_id=src.enums.bigquery.GCP_TABLE_ID_PIN_VECTOR,
            rows=pin_vectors,
            field_ids=["id"],
        )

    return pc_success, bq_success, num_inserted


def main() -> None:
    global bq_client, pc_index, encoder

    bq_client, pc_index = initialize_clients()
    encoder = src.encoder.FashionCLIPEncoder()

    n, n_success = 0, 0
    n_pc_success, n_bq_success, success_rate = 0, 0, 0

    loader = fetch_pins()

    if loader.total_rows == 0:
        return

    batch_ix, output_pins, images = 0, [], []
    loop = tqdm(iterable=loader, total=loader.total_rows)

    for row in loop:
        n += 1
        pin = src.models.Pin(**dict(row))
        image = src.utils.download_image_as_pil(pin.image_url)

        if image:
            images.append(image)
            output_pins.append(pin)

        if len(images) == len(output_pins) == BATCH_SIZE or n == loader.total_rows:
            pc_success, bq_success, n_pins = process_batch(output_pins, images)
            n_pc_success += int(pc_success)
            n_bq_success += int(bq_success)

            if pc_success and bq_success:
                n_success += n_pins

            batch_ix += 1
            output_pins, images = [], []
            success_rate = n_success / n

        loop.set_description(
            f"Batch: {batch_ix} | "
            f"Processed: {n} | "
            f"Success rate: {success_rate:.2f} | "
            f"Pinecone: {n_pc_success} | "
            f"BigQuery: {n_bq_success}"
        )


if __name__ == "__main__":
    main()
