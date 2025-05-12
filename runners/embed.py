import sys
sys.path.append("../")


from typing import List, Dict

import argparse
from tqdm import tqdm
from PIL import Image
from pinecone import Pinecone

import src


BATCH_SIZE = 64


def parse_args() -> bool:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--from-pinterest",
        "-fp",  
        type = lambda x: x.lower() == "true",
        required=True
    )

    args = parser.parse_args()

    return args.from_pinterest


def initialize_clients(from_pinterest: bool):
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    spb_kwargs = {
        "url": secrets["SUPABASE_URL"],
        "key": secrets["SUPABASE_SERVICE_ROLE_KEY"],
    }

    if from_pinterest:
        spb_kwargs["schema"] = src.enums.SUPABASE_SCHEMA_ID_BIGQUERY

    spb_client = src.supabase.init_client(**spb_kwargs)

    spb_kwargs["schema"] = src.enums.SUPABASE_SCHEMA_ID_RAW
    spb_client_raw = src.supabase.init_client(**spb_kwargs)    

    pc_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pc_index = pc_client.Index(src.enums.PINECONE_INDEX_NAME)

    return spb_client, spb_client_raw, pc_index


def fetch_pins(from_pinterest: bool) -> List[Dict]: 
    query = src.queries.make_supabase_pin_query(
        from_pinterest=from_pinterest, 
        n=src.enums.supabase.SUPABASE_BATCH_SIZE
    )

    kwargs = {
        "fn": src.enums.supabase.SUPABASE_RPC_ID_GET_PINS, 
        "params": {"query": query}
    }

    response = spb_client.rpc(**kwargs).execute()

    return response.data


def process_batch(pins: List[src.models.Pin], images: List[Image.Image]):
    embeddings = encoder.encode(images)
    pin_vectors, vectors = [], []

    for pin, embedding in zip(pins, embeddings):
        vector = src.models.Vector(
            values=embedding, 
            metadata=pin.to_dict()
        )

        pin_vector = src.models.PinVector(
            user_id=pin.user_id,
            pin_id=pin.id,
            point_id=vector.id
        )

        pin_vectors.append(pin_vector.to_dict())
        vectors.append(vector.to_dict())

    pc_success = src.pinecone.insert(pc_index, vectors)
    spb_success = False

    if pc_success:
        spb_success = src.supabase.insert(
            client=spb_client_raw, 
            table_id=src.enums.supabase.SUPABASE_TABLE_ID_PIN_VECTOR,
            rows=pin_vectors
        )

    return pc_success, spb_success


def main(from_pinterest: bool) -> None:
    global spb_client, spb_client_raw, pc_index, encoder

    spb_client, spb_client_raw, pc_index = initialize_clients(from_pinterest)
    encoder = src.encoder.FashionCLIPEncoder()

    index, n, n_success = 0, 0, 0
    n_pc_success, n_spb_success, success_rate = 0, 0, 0

    while True:
        input_pins = fetch_pins(from_pinterest)

        if len(input_pins) == 0:
            return
        
        batch_ix, output_pins, images = 0, [], []
        loop = tqdm(iterable=input_pins, total=len(input_pins))
        
        for entry in loop:
            n += 1
            pin = src.models.Pin(**entry)
            image = src.utils.download_image_as_pil(pin.image_url)

            if image:
                images.append(image)
                output_pins.append(pin)

            if len(images) == len(output_pins) == BATCH_SIZE or n == len(input_pins):
                pc_success, spb_success = process_batch(output_pins, images)
                n_pc_success += int(pc_success)
                n_spb_success += int(spb_success)
                
                if pc_success and spb_success:
                    n_success += len(images)

                batch_ix += 1
                output_pins, images = [], []
                success_rate = n_success / n
            
            loop.set_description(
                f"Index: {index} | "
                f"Batch: {batch_ix} | "
                f"Processed: {n} | "
                f"Success rate: {success_rate:.2f} | "
                f"Pinecone: {n_pc_success} | "
                f"Supabase: {n_spb_success}"
            )

        index += 1


if __name__ == "__main__":
    from_pinterest = parse_args()
    main(from_pinterest=from_pinterest)