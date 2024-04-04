

"""
test script
"""

import unittest
from unittest.mock import patch, MagicMock

import util
import db


################################################################################


class TestDb(unittest.TestCase):

    util.log.setLevel('CRITICAL')

    @patch('db.psycopg2.connect')
    def testGetConn(self, mock_connect:MagicMock):
        db.getConn('test_password')
        mock_connect.assert_called_once_with(
            host='localhost',
            database='postgres',
            user='postgres',
            password='test_password'
        )

    def testExecuteQuerySelect(self):
        testing = db.executeQuery('SELECT * FROM test')
        expecting = [{'col_01': 'value_01'}]
        self.assertEqual(testing, expecting)

    def testConvInsertValuesDataTypes(self):
        testing = db.convInsertValuesDataTypes(
            [{'col_01': "str_01_'", 'col_02': 123}],
            {'col_01': str, 'col_02': int}
        )
        expecting = [{'col_01': "'str_01_'''", 'col_02': '123'}]
        self.assertEqual(testing, expecting)

    def testBuildInsertQuery(self):
        testing = db.buildInsertQuery(
            'test_table_01',
            [{'col_01': "'str_01'", 'col_02': '123'}]
        )
        expecting = (
            "INSERT INTO test_table_01 (col_01, col_02) "
            "VALUES ('str_01', 123)"
        )
        self.assertEqual(testing, expecting)


################################################################################


if __name__ == '__main__':
    unittest.main()

