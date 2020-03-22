import django

django.setup()

from django.test import TestCase
from mainapp.utils import devexpress_filtering


class QueryViewTestCase(TestCase):
    def test_filter_int_equals(self):
        filter = ["age", "=", 28]
        sql_query = 'WHERE "age" = 28'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_int_does_not_equals(self):
        filter = ["age", "<>", 28]
        sql_query = 'WHERE "age" <> 28'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_int_less_then(self):
        filter = ["age", "<", 28]
        sql_query = 'WHERE "age" < 28'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_int_greater_than(self):
        filter = ["age", ">", 28]
        sql_query = 'WHERE "age" > 28'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_int_less_than_or_equal_to(self):
        filter = ["age", "<=", 28]
        sql_query = 'WHERE "age" <= 28'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_int_greater_than_or_equal_to(self):
        filter = ["age", ">=", 28]
        sql_query = 'WHERE "age" >= 28'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_int_is_blank(self):
        filter = ["age", "=", None]
        sql_query = 'WHERE "age" is null'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_int_is_not_blank(self):
        filter = ["age", "<>", None]
        sql_query = 'WHERE "age" is not null'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_int_between(self):
        filter = [["age", ">=", 28], "and", ["age", "<=", 33]]
        sql_query = 'WHERE "age" >= 28 and "age" <= 33'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_int_is_any_of(self):
        filter = [
            ["age", "=", 25],
            "or",
            ["age", "=", 26],
            "or",
            ["age", "=", 27],
            "or",
            ["age", "=", 31],
        ]
        sql_query = 'WHERE "age" = 25 or "age" = 26 or "age" = 27 or "age" = 31'.replace(
            '"', "'"
        )
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_int_is_non_of(self):
        filter = [
            "!",
            [["age", "=", 25], "or", ["age", "=", 26], "or", ["age", "=", 27]],
        ]
        sql_query = 'WHERE ("age" <> 25 and "age" <> 26 and "age" <> 27)'.replace(
            '"', "'"
        )
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_float_equals(self):
        filter = ["age", "=", 28.1]
        sql_query = 'WHERE "age" = 28.1'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_float_does_not_equals(self):
        filter = ["age", "<>", 28.1]
        sql_query = 'WHERE "age" <> 28.1'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_float_less_then(self):
        filter = ["age", "<", 28.1]
        sql_query = 'WHERE "age" < 28.1'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_float_greater_than(self):
        filter = ["age", ">", 28.1]
        sql_query = 'WHERE "age" > 28.1'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_float_less_than_or_equal_to(self):
        filter = ["age", "<=", 28.1]
        sql_query = 'WHERE "age" <= 28.1'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_float_greater_than_or_equal_to(self):
        filter = ["age", ">=", 28.1]
        sql_query = 'WHERE "age" >= 28.1'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_float_is_blank(self):
        filter = ["age", "=", None]
        sql_query = 'WHERE "age" is null'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_float_is_not_blank(self):
        filter = ["age", "<>", None]
        sql_query = 'WHERE "age" is not null'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_float_between(self):
        filter = [["age", ">=", 28.1], "and", ["age", "<=", 33.1]]
        sql_query = 'WHERE "age" >= 28.1 and "age" <= 33.1'
        response = devexpress_filtering.generate_where_sql_query(filter)
        self.assertEqual(sql_query, response)

    def test_filter_float_is_any_of(self):
        filter = [
            ["age", "=", 25.1],
            "or",
            ["age", "=", 26.1],
            "or",
            ["age", "=", 27.1],
            "or",
            ["age", "=", 31.1],
        ]
        sql_query = 'WHERE "age" = 25.1 or "age" = 26.1 or "age" = 27.1 or "age" = 31.1'.replace(
            '"', "'"
        )
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_float_is_non_of(self):
        filter = [
            "!",
            [["age", "=", 25.1], "or", ["age", "=", 26.1], "or", ["age", "=", 27.1]],
        ]
        sql_query = 'WHERE ("age" <> 25.1 and "age" <> 26.1 and "age" <> 27.1)'.replace(
            '"', "'"
        )
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_contains(self):
        filter = ["name", "contains", "messi"]
        sql_query = "WHERE lower('name') LIKE '%messi%'"
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_does_not_contains(self):
        filter = ["name", "notcontains", "messi"]
        sql_query = "WHERE lower('name') NOT LIKE '%messi%'"
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_starts_with(self):
        filter = ["name", "startswith", "me"]
        sql_query = 'WHERE lower("name") LIKE "me%"'.replace('"', "'")
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_ends_with(self):
        filter = ["name", "endswith", "me"]
        sql_query = "WHERE lower('name') LIKE '%me'"
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_equal(self):
        filter = ["name", "=", "L. Messi"]
        sql_query = "WHERE lower('name') = 'l. messi'"
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_does_not_equal(self):
        filter = ["name", "<>", "L. Messi"]
        sql_query = "WHERE lower('name') <> 'l. messi'"
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_is_blank(self):
        filter = [["name", "=", None], "or", ["name", "=", ""]]
        sql_query = "WHERE 'name' is null or lower('name') = ''"
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_is_not_blank(self):
        filter = [["name", "<>", None], "and", ["name", "<>", ""]]
        sql_query = "WHERE 'name' is not null and lower('name') <> ''"
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_is_any_of(self):
        filter = ["name", "=", "Cristiano Ronaldo"]
        sql_query = "WHERE lower('name') = 'cristiano ronaldo'"
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_str_is_none_of(self):
        filter = ["!", ["name", "=", "Cristiano Ronaldo"]]
        sql_query = "WHERE lower('name') <> 'cristiano ronaldo'"
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_recursion_is_non_of_with_a_list(self):
        filter = [
            [
                "!",
                [
                    ["name", "=", "Cristiano Ronaldo"],
                    "or",
                    ["name", "=", "De Gea"],
                    "or",
                    ["name", "=", "E. Hazard"],
                    "or",
                    ["name", "=", "J. Oblak"],
                ],
            ],
            "and",
            [["age", ">=", 28], "and", ["age", "<=", 33]],
        ]
        sql_query = (
            'WHERE (lower("name") <> "cristiano ronaldo" '
            'and lower("name") <> "de gea" '
            "and lower('name') <> 'e. hazard' "
            "and lower('name') <> 'j. oblak') "
            "and ('age' >= 28 and 'age' <= 33)".replace('"', "'")
        )
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_filter_recursion_grouping(self):
        filter = [["nationality", "=", "England"], "and", ["name", "endswith", "me"]]
        sql_query = (
            "WHERE lower('nationality') = 'england' and lower('name') LIKE '%me'"
        )
        response = devexpress_filtering.generate_where_sql_query(filter).replace(
            '"', "'"
        )
        self.assertEqual(sql_query, response)

    def test_dev_express_to_sql(self):
        table = "data1"
        filter = [["nationality", "=", "England"], "and", ["name", "endswith", "me"]]
        columns = ["col1", "col2", "col3"]
        sql_query = (
            'SELECT "col1","col2","col3" '
            "FROM 'data1' "
            "WHERE lower(\"nationality\") = 'england' and lower(\"name\") LIKE '%me'".replace(
                '"', "'"
            )
        )
        response = devexpress_filtering.dev_express_to_sql(table, filter, columns)
        self.assertEqual(sql_query, response[0].replace('"', "'"))
