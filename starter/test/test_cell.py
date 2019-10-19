import unittest
import requests


class CellTestCase(unittest.TestCase):
    hostname = 'localhost'
    port = 10000

    def suite():
        suite = unittest.TestSuite()
        suite.addTest(CellTestCase('test_setup'))
        suite.addTest(CellTestCase('test_insert_cell'))
        suite.addTest(CellTestCase('test_teardown'))
        return suite

    def test_setup(self):
        table1 = {
            "name": "table1",
            "column_families": [
                {
                    "column_family_key": "key1",
                    "columns": ["column_key1", "column_key2"]
                },
                {
                    "column_family_key": "key2",
                    "columns": ["column_key3", "column_key4"]
                }
            ]
        }

        table2 = {
            "name": "table2",
            "column_families": [
                {
                    "column_family_key": "key1",
                    "columns": ["column_key1", "column_key2"]
                },
                {
                    "column_family_key": "key2",
                    "columns": ["column_key3", "column_key4"]
                }
            ]

        }

        url = 'http://{}:{}/api/tables'.format(self.hostname, self.port)

        requests.post(url, json=table1)
        requests.post(url, json=table2)

    def test_teardown(self):
        url_tmpl = 'http://{}:{}/api/tables/{}'
        requests.delete(url_tmpl.format(self.hostname, self.port, 'table1'))
        requests.delete(url_tmpl.format(self.hostname, self.port, 'table2'))

    def test_insert_cell(self):
        url_tmpl = 'http://{}:{}/api/table/{}/cell'

        payload_1 = {
            "column_family": "key1",
            "column": "column_key1",
            "row": "xiacan",
            "data": [
                {
                    "value": "i am a very long string...",
                    "time": 1570212112.504831,
                }
            ]
        }

        payload_2 = {
            "column_family": "hahaha",
            "column": "column_key1",
            "row": "xiacan",
            "data": [
                {
                    "value": "i am a very long string...",
                    "time": 1570212112.504831,
                }
            ]
        }

        payload_3 = {
            "column_family": "key1",
            "column": "hahaha",
            "row": "xiacan",
            "data": [
                {
                    "value": "i am a very long string...",
                    "time": 1570212112.504831,
                }
            ]
        }

        # no such table
        url_1 = url_tmpl.format(self.hostname, self.port, 'hahaha')
        resp_1 = requests.post(url_1, json=payload_1)
        self.assertEqual(resp_1.status_code, 404)
        self.assertFalse(resp_1.content)

        # no such column_family
        url_2 = url_tmpl.format(self.hostname, self.port, 'table1')
        resp_2 = requests.post(url_2, json=payload_2)
        self.assertEqual(resp_2.status_code, 400)
        self.assertFalse(resp_2.content)

        # no such column
        resp_3 = requests.post(url_2, json=payload_3)
        self.assertEqual(resp_3.status_code, 400)
        self.assertFalse(resp_3.content)


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(CellTestCase.suite())
