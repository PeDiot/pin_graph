import sys

sys.path.append("../")


import src


def insert_boards():
    query = src.queries.make_insert_board_query()
    response = bq_client.query(query).result()

    rows, inserted = [], []
    for row in response:
        entry = src.models.Board(**row)
        entry.reset_created_at()

        rows.append(entry.to_dict())

        inserted.append(
            {
                "id": entry.id,
                "created_at": entry.created_at,
            }
        )

    if src.supabase.insert(
        client=spb_client,
        table_id=src.enums.SUPABASE_TABLE_ID_BOARD,
        rows=rows,
    ):
        return src.bigquery.insert(
            client=bq_client,
            dataset_id=src.enums.GCP_DATASET_ID_SUPABASE,
            table_id=src.enums.GCP_TABLE_ID_BOARD_INSERTED,
            rows=inserted,
        )

    return False


def main() -> None:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    global bq_client, spb_client
    bq_client = src.bigquery.init_client(secrets["GCP_CREDENTIALS"])

    spb_client = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
    )


if __name__ == "__main__":
    main()
