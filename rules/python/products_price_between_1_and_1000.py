def expectations():
    return {
        "type": "expect_column_values_to_be_between",
        "description": "Product price must be between 1 and 1000 inclusive.",
        "kwargs": {
            "column": "price",
            "min_value": 1,
            "max_value": 1000,
        },
    }
