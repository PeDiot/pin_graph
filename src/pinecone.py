from typing import List, Dict
import pinecone


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