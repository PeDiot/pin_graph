import sys 

sys.path.append("../")

import src


def main() -> None:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    client = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
    )

    for table_id in ["board", "pin"]:
        success = src.supabase.copy_from_raw_to_public(
            client=client,
            raw_table_id=table_id,
            public_table_id=table_id,
        )
        
        print(f"{table_id}: {success}.")


if __name__ == "__main__":
    main()