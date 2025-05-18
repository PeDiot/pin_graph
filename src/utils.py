from typing import Any

import json, requests, os, time
from functools import wraps
from PIL import Image


def load_secrets(env_var_name: str) -> Any:
    return json.loads(os.getenv(env_var_name))


def save_json(data: Any, file_path: str) -> bool:
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        return True
    except:
        return False


def download_image_as_pil(url: str, timeout: int = 10) -> Image.Image:
    try:
        REQUESTS_HEADERS = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.124 Safari/537.36"
            )
        }

        response = requests.get(
            url, stream=True, headers=REQUESTS_HEADERS, timeout=timeout
        )

        if response.status_code == 200:
            return Image.open(response.raw)

    except Exception as e:
        return


def execute_with_retry(max_retries: int = 3, delay: float = 1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))
                    continue
            raise last_exception

        return wrapper

    return decorator
