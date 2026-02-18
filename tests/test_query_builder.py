import unittest

from contact_mcp.query import build_count, build_select


class TestQueryBuilder(unittest.TestCase):
    def test_build_select_default(self):
        query = build_select("contact")
        self.assertIn("FROM lvousr.contact", query.sql)
        self.assertIn("LIMIT %s", query.sql)
        self.assertEqual(query.params[-2:], [100, 0])

    def test_build_select_filters(self):
        query = build_select("contact", filters={"client_id": 10, "account": "A1"})
        self.assertIn("client_id = %s", query.sql)
        self.assertIn("account = %s", query.sql)
        self.assertEqual(query.params[:-2], [10, "A1"])

    def test_invalid_table(self):
        with self.assertRaises(ValueError):
            build_select("nope")

    def test_invalid_column(self):
        with self.assertRaises(ValueError):
            build_select("contact", columns=["not_a_col"])

    def test_build_count_filters(self):
        query = build_count("contact", filters={"client_id": 10})
        self.assertIn("SELECT COUNT(*) AS count", query.sql)
        self.assertIn("client_id = %s", query.sql)
        self.assertEqual(query.params, [10])

    def test_build_select_operator_filters(self):
        query = build_select(
            "contact",
            filters={"account": {"op": "not_like", "value": "ECT%"}},
        )
        self.assertIn("account NOT LIKE %s", query.sql)
        self.assertEqual(query.params[:-2], ["ECT%"])


if __name__ == "__main__":
    unittest.main()
