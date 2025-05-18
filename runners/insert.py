import sys

sys.path.append("../")

import src
from tqdm import tqdm


PIN_BATCH_SIZE = 1000


def main() -> None:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    client = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
    )

    fn = src.enums.supabase.SUPABASE_RPC_ID_COPY_RECOMMEND_BOARDS
    response = src.supabase.execute_rpc(client, fn)
    print(f"{fn}: {len(response.data)} rows.")

    fn = src.enums.supabase.SUPABASE_RPC_ID_COPY_RECOMMEND_PINS
    params = {"p_limit": PIN_BATCH_SIZE}
    n_rows = 0

    with tqdm(desc=fn, unit="batch") as pbar:
        while True:
            response = src.supabase.execute_rpc(client, fn, params)

            n_rows += len(response.data)
            pbar.update(1)
            pbar.set_description(f"{fn}: {n_rows} rows")

            if len(response.data) < PIN_BATCH_SIZE:
                break


if __name__ == "__main__":
    main()
