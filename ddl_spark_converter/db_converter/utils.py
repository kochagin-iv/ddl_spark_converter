def generate_full_type_name(column):
    column_full_name = f"{column['type']}"

    column_size = column.get("size", "")

    if column_size and str(column_size)[0] != "(":
        column_size = f"({column_size})"

        column_full_name += column_size

    return column_full_name.upper()
