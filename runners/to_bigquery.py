import sys

sys.path.append("../")


import src


def main() -> None:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    bq_client = src.bigquery.init_client(secrets["GCP_CREDENTIALS"])

    spb_client = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
    )

    index, n_success, n_rows, n_inserted = 0, 0, 0, 0

    while True:
        rows = src.supabase.get_rows(
            client=spb_client,
            table_id=src.enums.GCP_TABLE_ID_PINTEREST,
            n=src.enums.SUPABASE_BATCH_SIZE,
            index=index,
        )

        if len(rows) == 0:
            return

        n, success = src.bigquery.insert_unique(
            client=bq_client,
            dataset_id=src.enums.GCP_DATASET_ID,
            table_id=src.enums.GCP_TABLE_ID_PINTEREST,
            rows=rows,
            unique_field="user_id",
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
