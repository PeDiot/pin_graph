import sys

sys.path.append("../")

import src


def main() -> None:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    client = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
    )

    for fn in [
        src.enums.supabase.SUPABASE_RPC_ID_COPY_RECOMMEND_BOARDS,
        src.enums.supabase.SUPABASE_RPC_ID_COPY_RECOMMEND_PINS,
    ]:
        response = client.rpc(fn).execute()
        n_rows = len(response.data)
        print(f"{fn}: {n_rows} rows.")


if __name__ == "__main__":
    main()
