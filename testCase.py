import unittest
from flask import jsonify
from testCase import app


class MyAppTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()

    def test_insert_data(self):
        response = self.app.post("/api/insert_data")
        self.assertEqual(response.status_code, 200)
        data = response.json
        self.assertEqual(data["message"], "successfully posted")

        # Add more assertions if needed

    def test_insert_single_data(self):
        payload = {
            "PartitionKey": "TSLA",
            "RowKey": "2023-05-20",
            "Open": 123.45,
            "Close": 123.67,
            "High": 125.00,
            "Low": 122.80,
            "Volume": 1000000,
            "Adj_Close": 123.55,
        }
        response = self.app.post("/api/insert_single_data", json=payload)
        self.assertEqual(response.status_code, 200)
        data = response.json
        self.assertEqual(data["message"], "Data Insert SuccessFully.")

        # Add more assertions if needed

    def test_get_data(self):
        response = self.app.get("/api/get_data")
        self.assertEqual(response.status_code, 200)
        data = response.json
        self.assertIsInstance(data, list)

        # Add more assertions if needed

    def test_update_data(self):
        payload = {
            "RowKey": "2023-05-20",
            "Open": 125.50,
            "Close": 124.80,
            "High": 126.20,
            "Low": 124.30,
            "Volume": 2000000,
            "Adj_Close": 125.00,
        }
        response = self.app.put("/api/update_data", json=payload)
        self.assertEqual(response.status_code, 200)
        data = response.json
        self.assertEqual(data, "Data updated successfully")

        # Add more assertions if needed

    def test_insert_message(self):
        payload = {"message": "Test message"}
        response = self.app.post("/api/insert-message", json=payload)
        self.assertEqual(response.status_code, 200)
        data = response.data
        self.assertEqual(
            data.decode("utf-8"), "Message inserted into the queue successfully"
        )

        # Add more assertions if needed


if __name__ == "__main__":
    unittest.main()
