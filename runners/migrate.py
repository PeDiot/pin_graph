import sys

sys.path.append("../")


from typing import Literal
import src


def main(table_id: Literal["board", "pin"]) -> None:
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    bq_client = src.bigquery.init_client(secrets["GCP_CREDENTIALS"])

    spb_client = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
        schema=src.enums.SUPABASE_SCHEMA_ID_BIGQUERY,
    )

    if table_id == "board":
        query_func = src.queries.make_bigquery_board_query
        supabase_table_id = src.enums.SUPABASE_TABLE_ID_BOARD
        bigquery_table_id = src.enums.GCP_TABLE_ID_BOARD_PROCESSED
    else:
        query_func = src.queries.make_bigquery_pin_query
        supabase_table_id = src.enums.SUPABASE_TABLE_ID_PIN
        bigquery_table_id = src.enums.GCP_TABLE_ID_PIN_PROCESSED

    index, n_success, n_rows = 0, 0, 0

    while True:
        query = query_func(n=src.enums.SUPABASE_BATCH_SIZE)
        result = bq_client.query(query).result()
        rows, processed_rows = [], []

        for row in result:
            row = dict(row)
            row["created_at"] = row["created_at"].isoformat()
            processed_row = {"id": row["id"], "created_at": row["created_at"]}

            rows.append(row)
            processed_rows.append(processed_row)

        if len(rows) == 0:
            return

        success = src.supabase.insert(
            client=spb_client, table_id=supabase_table_id, rows=rows
        )

        success = src.bigquery.insert(
            client=bq_client,
            dataset_id=src.enums.GCP_DATASET_ID,
            table_id=bigquery_table_id,
            rows=processed_rows,
        )

        if success:
            n_success += 1
            n_rows += len(rows)

        index += 1
        success_rate = n_success / index

        print(
            f"Batch: {index} | Success: {n_success} | Success rate: {success_rate} | Rows: {n_rows}"
        )


if __name__ == "__main__":
    for table_id in ["board", "pin"]:
        main(table_id)
