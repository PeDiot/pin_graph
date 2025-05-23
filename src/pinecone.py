from typing import List, Dict, Tuple, Optional
import pinecone

from .models import Pin


def insert(
    index: pinecone.Index, vectors: List[Dict], namespace: Optional[str] = None
) -> bool:
    if len(vectors) == 0:
        return False

    try:
        response = index.upsert(vectors=vectors, namespace=namespace)
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
    image_urls: List[str],
) -> List[pinecone.ScoredVector]:
    filter_conditions = _create_filter_conditions(
        user_id=user_id, image_urls=image_urls
    )

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
    image_urls: List[str],
) -> Tuple[List[Dict], List[str]]:
    pins, score_list = [], []

    for match in matches:
        score = round(match.score, 3)

        if score < min_score:
            continue

        if score > max_score:
            continue

        if score in score_list:
            continue

        metadata = match.metadata
        pin = Pin(**metadata)

        if pin.image_url in image_urls:
            continue

        pin.set_point_id(match.id)
        pin.set_board_id(board_id)
        pin.set_created_at()

        pins.append(pin.to_bigquery())
        score_list.append(score)
        image_urls.append(pin.image_url)

        if len(pins) == n:
            return pins, image_urls

    return pins, image_urls


def _create_filter_conditions(user_id: str, image_urls: List[str]) -> Dict:
    filter_conditions = {"from_pinterest": {"$eq": True}, "user_id": {"$ne": user_id}}

    if image_urls:
        filter_conditions["image_url"] = {"$nin": image_urls}

    return filter_conditions
