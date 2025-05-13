import sys

sys.path.append("../")

import src


def main():
    secrets = src.utils.load_secrets(env_var_name="SECRETS_JSON")

    spb_client = src.supabase.init_client(
        url=secrets["SUPABASE_URL"],
        key=secrets["SUPABASE_SERVICE_ROLE_KEY"],
        schema=src.enums.SUPABASE_SCHEMA_ID_RAW,
    )

    user_id = "5c1f9f1d-d1d3-4b28-8a62-1cfbc0d1839f"

    query = src.queries.make_supabase_pin_vector_query(
        user_id=user_id, n=10, shuffle=False
    )

    kwargs = {
        "fn": src.enums.supabase.SUPABASE_RPC_ID_GET_PIN_VECTORS,
        "params": {"query": query},
    }

    response = spb_client.rpc(**kwargs).execute()

    if not response.data:
        return

    board_id = src.supabase.get_recommend_board_id(client=spb_client, user_id=user_id)

    if not board_id:
        return

    print(f"Board ID: {board_id}")

    existing_pins = src.supabase.get_recommend_pins(
        client=spb_client, board_id=board_id, n=10
    )

    print(len(existing_pins))

    data = []

    for row in response.data:
        vector = src.models.PinVector.from_dict(row)
        data.append(vector)

    print(len(data))
    print(data[0])


if __name__ == "__main__":
    main()
