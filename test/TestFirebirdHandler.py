import unittest
from unittest.mock import MagicMock, patch

import fdb
import pandas as pd

from firebird.FirebirdHandler import FirebirdHandler
from utils.errors import FirebirdConnectionError, FirebirdQueryError


class TestFirebirdHandler(unittest.TestCase):
    def setUp(self):
        """
        Sets up the test environment.
        """
        self.handler = FirebirdHandler(
            host='127.0.0.1',
            port=3051,
            database='/firebird/data/mydb.fdb',
            user='SYSDBA',
            password='masterkey'
        )

    @patch('fdb.connect')
    def test_connect_success(self, mock_connect):
        """
        Tests successful connection to the database.
        """
        self.handler.connect()
        mock_connect.assert_called_once_with(
            host='127.0.0.1',
            port=3051,
            database='/firebird/data/mydb.fdb',
            user='SYSDBA',
            password='masterkey'
        )

    @patch('fdb.connect', side_effect=fdb.DatabaseError("Connection failed"))
    def test_connect_failure(self, mock_connect):
        """
        Tests connection failure.
        """
        with self.assertRaises(FirebirdConnectionError):
            self.handler.connect()

    @patch('fdb.Connection')
    def test_execute_query_to_csv_success(self, mock_connection):
        """
        Tests successful query execution and CSV generation.
        """
        # Mock cursor behavior
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 'John Doe'), (2, 'Jane Smith')]
        mock_cursor.description = [('id',), ('name',)]
        mock_connection.cursor.return_value = mock_cursor

        self.handler.connection = mock_connection
        query = "SELECT * FROM employees"
        output_file = "test_output.csv"

        self.handler.execute_query_to_csv(query, output_file)

        # Assert cursor methods were called
        mock_cursor.execute.assert_called_once_with(query)
        mock_cursor.fetchall.assert_called_once()

        # Validate CSV content
        df = pd.read_csv(output_file)
        self.assertEqual(len(df), 2)
        self.assertIn('id', df.columns)
        self.assertIn('name', df.columns)

    def test_execute_query_to_csv_no_connection(self):
        """
        Tests executing a query without an established connection.
        """
        with self.assertRaises(FirebirdConnectionError):
            self.handler.execute_query_to_csv("SELECT * FROM employees", "output.csv")

    @patch('fdb.Connection')
    def test_execute_query_to_csv_failure(self, mock_connection):
        """
        Tests query execution failure.
        """
        # Mock cursor behavior
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = fdb.DatabaseError("Query failed")
        mock_connection.cursor.return_value = mock_cursor

        self.handler.connection = mock_connection

        with self.assertRaises(FirebirdQueryError):
            self.handler.execute_query_to_csv("SELECT * FROM employees", "output.csv")

    def test_close_connection(self):
        """
        Tests closing the database connection.
        """
        mock_connection = MagicMock()
        self.handler.connection = mock_connection

        self.handler.close()

        self.assertIsNotNone(self.handler.connection)

if __name__ == "__main__":
    unittest.main()
