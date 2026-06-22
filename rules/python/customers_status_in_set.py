def expectations():
    return {
        "type": "expect_column_values_to_be_in_set",
        "description": "Customer status must be active or inactive.",
        "kwargs": {
            "column": "status",
            "value_set": ["active", "inactive"],
        },
    }
