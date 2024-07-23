import os
import sys

# This is needed to run debug with VSCode
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from unittest.mock import Mock, patch

import pandas as pd
from shared import db_utils


class TestDbUtils(unittest.TestCase):
    def test_get_and_close_db_conn(self):
        db_utils.get_db_conn()
        self.assertIsNotNone(db_utils.db_conn)
        db_utils.close_db_conn()
        self.assertIsNone(db_utils.db_conn)

    @patch("shared.db_utils.pd.read_sql_query")
    @patch("shared.db_utils.get_db_conn")
    def test_f_get_table(self, mock_get_db_conn, mock_read_sql_query):
        # Arrange
        mock_query = "SELECT * FROM table"
        mock_result = "blabla"
        mock_read_sql_query.return_value = mock_result

        # Act
        result = db_utils.f_get_table(mock_query)

        # Assert
        mock_get_db_conn.assert_called_once()
        mock_read_sql_query.assert_called_once_with(
            mock_query, mock_get_db_conn.return_value
        )
        self.assertEqual(result, mock_result)

    @patch("shared.db_utils.get_db_conn")
    @patch("shared.db_utils.pd.DataFrame.to_sql")
    @patch("shared.db_utils.logging")
    def test_f_append_to_table_no_rows(
        self, mock_logging, mock_to_sql, mock_get_db_conn
    ):
        df = pd.DataFrame()
        table_name = "test_table"
        mock_to_sql.return_value = 0

        db_utils.f_append_to_table(df, table_name)

        mock_to_sql.assert_called_once_with(
            table_name,
            con=mock_get_db_conn.return_value,
            if_exists="append",
            index=False,
        )
        mock_logging.warn.assert_called_once_with(f"No data appended to {table_name}")
        mock_logging.info.assert_not_called()

    @patch("shared.db_utils.get_db_conn")
    @patch("shared.db_utils.pd.DataFrame.to_sql")
    @patch("shared.db_utils.logging")
    def test_f_append_to_table_with_rows(
        self, mock_logging, mock_to_sql, mock_get_db_conn
    ):
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        table_name = "test_table"
        mock_to_sql.return_value = 2

        db_utils.f_append_to_table(df, table_name)

        mock_to_sql.assert_called_once_with(
            table_name,
            con=mock_get_db_conn.return_value,
            if_exists="append",
            index=False,
        )
        mock_logging.warn.assert_not_called()
        mock_logging.info.assert_called_once_with(
            f"FusionSolar 2 data rows appended to {table_name}"
        )


if __name__ == "__main__":
    unittest.main()
