import sys

sys.path.append("../")


import src


def get_last_created_at() -> str:
    query = src.queries.make_last_date_query(
        table_id=src.enums.GCP_TABLE_ID_PINTEREST,
    )

    result = bq_client.query(query).result()

    for row in result:
        return row["created_at"]


def main() -> None:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    global bq_client
    bq_client = src.bigquery.init_client(secrets["GCP_CREDENTIALS"])

    spb_client = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
    )

    last_created_at = get_last_created_at()
    index, n_success, n_rows, n_inserted = 0, 0, 0, 0

    while True:
        rows = src.supabase.get_rows(
            client=spb_client,
            table_id=src.enums.SUPABASE_TABLE_ID_PINTEREST,
            n=src.enums.SUPABASE_BATCH_SIZE,
            index=index,
            created_at=last_created_at,
        )

        if len(rows) == 0:
            return

        n, success = src.bigquery.insert_unique(
            client=bq_client,
            dataset_id=src.enums.GCP_DATASET_ID_SUPABASE,
            table_id=src.enums.GCP_TABLE_ID_PINTEREST,
            rows=rows,
            field_ids=["user_id"],
        )

        index += 1
        n_success += int(success)
        n_rows += len(rows)
        success_rate = n_success / index

        print(
            f"Batch: {index} | "
            f"Processed: {n_rows} | "
            f"Inserted: {n_inserted} | "
            f"Success rate: {success_rate:.2f}"
        )


if __name__ == "__main__":
    main()
