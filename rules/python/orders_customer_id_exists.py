def query(env):
    orders = env("${ATHENA_ORDERS_TABLE}")
    customers = env("${ATHENA_CUSTOMERS_TABLE}")
    return f"""
        SELECT
            o.order_id,
            o.customer_id
        FROM {orders} o
        LEFT JOIN {customers} c
            ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """


def expectations():
    return {
        "type": "expect_table_row_count_to_equal",
        "description": "Every order customer_id must exist in customers.",
        "kwargs": {
            "value": 0,
        },
    }
