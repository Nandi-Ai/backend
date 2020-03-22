def __create_where_section_positive(field, operator, value):
    dev_express_value = (
        value
        if isinstance(value, int) or isinstance(value, float) or value is None
        else value.lower()
    )

    if operator == "contains":
        return f"lower(\"{field}\") LIKE '%{dev_express_value}%'"

    if operator == "notcontains":
        return f"lower(\"{field}\") NOT LIKE '%{dev_express_value}%'"

    if operator == "startswith":
        return f"lower(\"{field}\") LIKE '{dev_express_value}%'"

    if operator == "endswith":
        return f"lower(\"{field}\") LIKE '%{dev_express_value}'"

    if operator == "notstartswith":
        return f"lower(\"{field}\") NOT LIKE '{dev_express_value}%'"

    if operator == "notendswith":
        return f"lower(\"{field}\") NOT LIKE '%{dev_express_value}'"

    if operator == "=":
        if dev_express_value is None:
            return f'"{field}" is null'

        if isinstance(dev_express_value, str):
            return f"lower(\"{field}\") = '{dev_express_value}'"
        return f'"{field}" = {dev_express_value}'

    if operator == "<>":
        if dev_express_value is None:
            return f'"{field}" is not null'

        if isinstance(dev_express_value, str):
            return f"lower(\"{field}\") <> '{dev_express_value}'"
        return f'"{field}" <> {dev_express_value}'

    if operator in [">", "<", ">=", "<="]:
        return f'"{field}" {operator} {dev_express_value}'

    raise TypeError("unknown operator: " + operator)


def __create_where_section_negative(field, operator, value):
    dev_express_value = (
        value
        if isinstance(value, int) or isinstance(value, float) or value is None
        else value.lower()
    )

    if operator == "=":
        if dev_express_value is None:
            return f'"{field}" is not null'

        if isinstance(dev_express_value, str):
            return f"lower(\"{field}\") <> '{dev_express_value}'"
        return f'"{field}" <> {dev_express_value}'

    raise TypeError("unknown operator: " + operator)


def __generate_where_sql_query_from_single_filter(data_filter, negative=None):
    field = data_filter[0]
    operator = data_filter[1]
    value = data_filter[2]
    if negative:
        return __create_where_section_negative(field, operator, value)
    else:
        return __create_where_section_positive(field, operator, value)


def dev_express_to_sql(table, data_filter, columns, schema=None, limit=None):

    select = '"' + ('","'.join(columns)) + '"' if columns else "*"

    if schema:
        query = f'SELECT {select} FROM "{schema}"."{table}"'
    else:
        query = f'SELECT {select} FROM "{table}"'

    if data_filter:
        query += f" {generate_where_sql_query(data_filter)}"

    query_no_limit = query

    if limit:
        query += f" LIMIT {str(limit)}"

    return query, query_no_limit


def __generate_where_sql_query_from_multiple_filters(data_filter, negative=None):
    query = ""
    for data in data_filter:
        if isinstance(data, list):
            if isinstance(data[0], list):
                query += f"({__generate_where_sql_query_from_multiple_filters(data, negative)})"
            else:
                if data[0] == "!":
                    data.pop(0)
                    query += __generate_where_sql_query_from_multiple_filters(
                        data, not negative
                    )
                else:
                    query += __generate_where_sql_query_from_single_filter(
                        data, negative
                    )
        elif isinstance(data, str):
            if data == "or" and negative:
                query += f" and "
            elif data == "and" and negative:
                query += f" or "
            elif data == "!":
                negative = True
                continue
            else:
                query += f" {data} "
    return query


def generate_where_sql_query(data_filter):
    query = "WHERE "

    if not isinstance(data_filter, list):
        raise Exception("invalid data filters")

    if data_filter[0] in ["and", "or"]:
        data_filter.pop(0)

    if isinstance(data_filter[0], str) and isinstance(data_filter[1], str):
        query += __generate_where_sql_query_from_single_filter(data_filter)
    elif isinstance(data_filter[0], list) and isinstance(data_filter[1], str):
        query += __generate_where_sql_query_from_multiple_filters(data_filter)
    elif isinstance(data_filter[0], str) and isinstance(data_filter[1], list):
        query += __generate_where_sql_query_from_multiple_filters(data_filter)

    return query
