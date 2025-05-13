from typing import List, Dict
import pinecone

from .models import Pin


def insert(index: pinecone.Index, vectors: List[Dict]) -> bool:
    if len(vectors) == 0:
        return False

    try:
        response = index.upsert(vectors=vectors)
        n_upserted = response.get("upserted_count", 0)

        return n_upserted == len(vectors)

    except Exception as e:
        print(e)
        return False


def get_neighbors(
    index: pinecone.Index,
    point_id: str,
    user_id: str,
    n: int,
) -> List[pinecone.ScoredVector]:
    filter_conditions = {"user_id": {"$ne": user_id}}

    results = index.query(
        id=point_id,
        top_k=n,
        filter=filter_conditions,
        include_values=False,
        include_metadata=True,
    )

    return results.matches


def postprocess_matches(
    matches: List[pinecone.ScoredVector],
    board_id: str,
    n: int,
    min_score: float,
    max_score: float,
) -> List[Pin]:
    index, pins = [], []

    for match in matches:
        if match.score < min_score:
            continue

        if match.score > max_score:
            continue

        metadata = match.metadata
        pin = Pin(**metadata)

        if pin.image_url in index:
            continue

        pin.set_point_id(match.id)
        pin.set_board_id(board_id)

        pins.append(pin.to_supabase())
        index.append(pin.image_url)

        if len(pins) == n:
            return pins

    return pins
