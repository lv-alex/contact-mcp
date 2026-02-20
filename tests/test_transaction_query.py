import unittest
from datetime import date

from contact_mcp.transaction import (
    build_transaction_union,
    resolve_transaction_tables,
)


class TestTransactionQuery(unittest.TestCase):
    def test_resolve_transaction_tables(self):
        tables = resolve_transaction_tables(
            "dial.example",
            "report.example",
            today=date(2026, 2, 18),
        )
        self.assertEqual(
            tables,
            [
                "LVOUSR.TRANSACTION@dial.example",
                "LVOUSR.TRANSACTION_0126@report.example",
                "LVOUSR.TRANSACTION_1225@report.example",
            ],
        )

    def test_build_transaction_union(self):
        query = build_transaction_union(
            ["LVOUSR.TRANSACTION@dial.example"],
            filters={"account": "A1"},
            columns=["account", "client_id"],
            limit=10,
            order_by="call_start_time DESC",
        )
        self.assertIn("ACCOUNT = :p1", query.sql)
        self.assertIn("ORDER BY CALL_START_TIME DESC", query.sql)
        self.assertEqual(query.params["p1"], "A1")
        self.assertEqual(query.params["limit"], 10)


if __name__ == "__main__":
    unittest.main()
