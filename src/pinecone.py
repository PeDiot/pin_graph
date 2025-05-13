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
    board_id: str,
    n: int,
    prefetch: int = 0,
    max_score: float = 1.0,
    min_score: float = 0.0,
) -> List[Dict]:
    filter_conditions = {"user_id": {"$ne": user_id}}

    results = index.query(
        id=point_id,
        top_k=n + prefetch,
        filter=filter_conditions,
        include_values=False,
        include_metadata=True,
    )

    neighbors = []

    for match in results.matches:
        if match.score < min_score or match.score > max_score:
            continue

        pin = Pin(**match.metadata)
        pin.set_point_id(match.id)
        pin.set_board_id(board_id)

        neighbors.append(pin.to_supabase())

        if len(neighbors) == n:
            return neighbors

    return neighbors
