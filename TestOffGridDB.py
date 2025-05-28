import unittest
import sqlite3
import json
import os
import time
from datetime import datetime
import pytest
from httpx import Client
from fastapi.testclient import TestClient
from offgrid_api import app  # Import the FastAPI app
from OffGridDB import OffGridDB

class TestOffGridDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize class-level test results."""
        cls.test_results = []

    def setUp(self):
        """Set up test environment."""
        self.db_path = "test_offgrid.db"
        self.json_path = "test_offgrid.json"
        self.db = OffGridDB(self.db_path)
        
        # Create a minimal valid JSON file for testing
        self.test_json = {
            "levels": [
                {
                    "level": 1,
                    "name": "Test Level",
                    "description": "Test description",
                    "monthly_costs": [{"name": "food", "amount": 100}],
                    "fixed_costs": [
                        {"name": "Test Item", "units": 1, "unit_type": "unit", "unit_cost": 50, "total": 50, "seller_source": "Test Seller"}
                    ],
                    "total_monthly": 100,
                    "total_fixed": 50
                }
            ]
        }
        with open(self.json_path, 'w') as f:
            json.dump(self.test_json, f)

    def tearDown(self):
        """Clean up test environment."""
        try:
            self.db.close()
        except:
            pass
        # Retry deleting the database file to handle Windows file locking
        for _ in range(3):
            if os.path.exists(self.db_path):
                try:
                    os.remove(self.db_path)
                    break
                except PermissionError:
                    time.sleep(0.1)  # Brief delay to allow file release
        if os.path.exists(self.json_path):
            try:
                os.remove(self.json_path)
            except PermissionError:
                pass  # Ignore if JSON file can't be deleted

    def log_result(self, test_name, status, message):
        """Log test result for console and final report."""
        result = {"test": test_name, "status": status, "message": message, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        self.test_results.append(result)
        print(f"Test: {test_name} | Status: {status} | Message: {message}")

    def test_connect(self):
        """Test database connection."""
        try:
            self.db.connect()
            self.assertIsNotNone(self.db.conn, "Connection object should not be None")
            self.assertIsNotNone(self.db.cursor, "Cursor object should not be None")
            self.log_result("test_connect", "PASS", "Database connection established successfully")
        except Exception as e:
            self.log_result("test_connect", "FAIL", f"Connection failed: {str(e)}")
            self.fail(str(e))

    def test_create_tables(self):
        """Test table creation with and without drop_if_exists."""
        try:
            self.db.connect()
            self.db.create_tables(drop_if_exists=True)
            self.db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('levels', 'monthly_costs', 'fixed_costs')")
            tables = [row[0] for row in self.db.cursor.fetchall()]
            self.assertEqual(len(tables), 3, "All three tables should be created")
            self.assertIn("levels", tables)
            self.assertIn("monthly_costs", tables)
            self.assertIn("fixed_costs", tables)
            self.log_result("test_create_tables", "PASS", "Tables created successfully")
        except Exception as e:
            self.log_result("test_create_tables", "FAIL", f"Table creation failed: {str(e)}")
            self.fail(str(e))

    def test_load_json_valid(self):
        """Test loading valid JSON data."""
        try:
            self.db.load_json(self.json_path, drop_if_exists=True)
            result = self.db.query("SELECT * FROM levels")
            self.assertEqual(len(result), 1, "One level should be inserted")
            self.assertEqual(result[0][1], "Test Level", "Level name should match")
            result = self.db.query("SELECT * FROM monthly_costs")
            self.assertEqual(len(result), 1, "One monthly cost should be inserted")
            self.assertEqual(result[0][2], "food", "Monthly cost name should match")
            result = self.db.query("SELECT * FROM fixed_costs")
            self.assertEqual(len(result), 1, "One fixed cost should be inserted")
            self.assertEqual(result[0][2], "Test Item", "Fixed cost name should match")
            self.assertEqual(result[0][7], "Test Seller", "Seller source should match")
            self.log_result("test_load_json_valid", "PASS", "Valid JSON loaded successfully")
        except Exception as e:
            self.log_result("test_load_json_valid", "FAIL", f"Loading valid JSON failed: {str(e)}")
            self.fail(str(e))

    def test_load_json_invalid_file(self):
        """Test loading non-existent JSON file."""
        try:
            self.db.load_json("nonexistent.json", drop_if_exists=True)
            self.log_result("test_load_json_invalid_file", "FAIL", "Expected FileNotFoundError but none raised")
            self.fail("Expected FileNotFoundError")
        except FileNotFoundError:
            self.log_result("test_load_json_invalid_file", "PASS", "Correctly handled non-existent JSON file")
        except Exception as e:
            self.log_result("test_load_json_invalid_file", "FAIL", f"Unexpected error: {str(e)}")
            self.fail(str(e))

    def test_load_json_invalid_data(self):
        """Test loading JSON with invalid schema."""
        invalid_json = {"levels": [{"level": 1, "name": "Test"}]}
        with open(self.json_path, 'w') as f:
            json.dump(invalid_json, f)
        try:
            self.db.load_json(self.json_path, drop_if_exists=True)
            self.log_result("test_load_json_invalid_data", "FAIL", "Expected schema error but none raised")
            self.fail("Expected schema error")
        except Exception as e:
            self.log_result("test_load_json_invalid_data", "PASS", f"Correctly handled invalid JSON data: {str(e)}")
        except:
            self.log_result("test_load_json_invalid_data", "FAIL", f"Unexpected error: {str(e)}")
            self.fail(str(e))

    def test_query(self):
        """Test custom query execution."""
        try:
            self.db.load_json(self.json_path, drop_if_exists=True)
            result = self.db.query("SELECT * FROM fixed_costs WHERE level_id = ?", (1,))
            self.assertEqual(len(result), 1, "One fixed cost should be returned")
            self.assertEqual(result[0][2], "Test Item", "Fixed cost name should match")
            self.log_result("test_query", "PASS", "Query executed successfully")
        except Exception as e:
            self.log_result("test_query", "FAIL", f"Query failed: {str(e)}")
            self.fail(str(e))

    def test_close(self):
        """Test closing database connection."""
        try:
            self.db.connect()
            self.db.close()
            self.assertIsNone(self.db.conn, "Connection should be None after close")
            self.assertIsNone(self.db.cursor, "Cursor should be None after close")
            self.log_result("test_close", "PASS", "Database connection closed successfully")
        except Exception as e:
            self.log_result("test_close", "FAIL", f"Closing connection failed: {str(e)}")
            self.fail(str(e))

class TestOffGridAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize class-level test results, reusing TestOffGridDB's results."""
        if not hasattr(TestOffGridDB, 'test_results'):
            TestOffGridDB.test_results = []
        cls.test_results = TestOffGridDB.test_results  # Share test_results with TestOffGridDB
        cls.client = TestClient(app)  # FastAPI test client

    def setUp(self):
        """Set up test environment for API tests."""
        self.db_path = "test_offgrid.db"
        self.json_path = "test_offgrid.json"
        # Create test JSON file
        with open(self.json_path, 'w') as f:
            json.dump({
                "levels": [
                    {
                        "level": 1,
                        "name": "Test Level",
                        "description": "Test description",
                        "monthly_costs": [{"name": "food", "amount": 100}],
                        "fixed_costs": [
                            {"name": "Test Item", "units": 1, "unit_type": "unit", "unit_cost": 50, "total": 50, "seller_source": "Test Seller"}
                        ],
                        "total_monthly": 100,
                        "total_fixed": 50
                    }
                ]
            }, f)

    def tearDown(self):
        """Clean up test environment."""
        for path in [self.db_path, self.json_path, "test_report.md"]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except PermissionError:
                    time.sleep(0.1)  # Brief delay for file release

    def log_result(self, test_name, status, message):
        """Log test result, reusing TestOffGridDB's method."""
        result = {"test": test_name, "status": status, "message": message, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        self.test_results.append(result)
        print(f"Test: {test_name} | Status: {status} | Message: {message}")

    def test_api_load(self):
        """Test API load endpoint."""
        try:
            response = self.client.post(f"/load?json_path={self.json_path}&db={self.db_path}&drop=true")
            self.assertEqual(response.status_code, 200, f"API load failed: {response.text}")
            self.assertIn("Successfully loaded", response.json()["message"])
            self.log_result("test_api_load", "PASS", "API load endpoint executed successfully")
        except Exception as e:
            self.log_result("test_api_load", "FAIL", f"API load failed: {str(e)}")
            self.fail(str(e))

    def test_api_query_levels(self):
        """Test API query levels endpoint."""
        try:
            # First load data
            self.client.post(f"/load?json_path={self.json_path}&db={self.db_path}&drop=true")
            response = self.client.get(f"/query/levels?db={self.db_path}")
            self.assertEqual(response.status_code, 200, f"API query levels failed: {response.text}")
            self.assertEqual(len(response.json()), 1, "Expected one level in response")
            self.assertEqual(response.json()[0]["name"], "Test Level", "Level name should match")
            self.log_result("test_api_query_levels", "PASS", "API query levels endpoint executed successfully")
        except Exception as e:
            self.log_result("test_api_query_levels", "FAIL", f"API query levels failed: {str(e)}")
            self.fail(str(e))

    def test_api_query_monthly(self):
        """Test API query monthly costs endpoint."""
        try:
            self.client.post(f"/load?json_path={self.json_path}&db={self.db_path}&drop=true")
            response = self.client.get(f"/query/monthly?db={self.db_path}&level=1")
            self.assertEqual(response.status_code, 200, f"API query monthly failed: {response.text}")
            self.assertEqual(len(response.json()), 1, "Expected one monthly cost in response")
            self.assertEqual(response.json()[0]["name"], "food", "Monthly cost name should match")
            self.log_result("test_api_query_monthly", "PASS", "API query monthly endpoint executed successfully")
        except Exception as e:
            self.log_result("test_api_query_monthly", "FAIL", f"API query monthly failed: {str(e)}")
            self.fail(str(e))

    def test_api_query_fixed(self):
        """Test API query fixed costs endpoint."""
        try:
            self.client.post(f"/load?json_path={self.json_path}&db={self.db_path}&drop=true")
            response = self.client.get(f"/query/fixed?db={self.db_path}&level=1")
            self.assertEqual(response.status_code, 200, f"API query fixed failed: {response.text}")
            self.assertEqual(len(response.json()), 1, "Expected one fixed cost in response")
            self.assertEqual(response.json()[0]["name"], "Test Item", "Fixed cost name should match")
            self.log_result("test_api_query_fixed", "PASS", "API query fixed endpoint executed successfully")
        except Exception as e:
            self.log_result("test_api_query_fixed", "FAIL", f"API query fixed failed: {str(e)}")
            self.fail(str(e))

    def test_api_report(self):
        """Test API report endpoint."""
        try:
            self.client.post(f"/load?json_path={self.json_path}&db={self.db_path}&drop=true")
            response = self.client.get(f"/report?db={self.db_path}&output=test_report.md")
            self.assertEqual(response.status_code, 200, f"API report failed: {response.text}")
            self.assertIn("Report generated at test_report.md", response.json()["message"])
            self.assertTrue(os.path.exists("test_report.md"), "Report file should be created")
            with open("test_report.md", "r") as f:
                content = f.read()
                self.assertIn("Test Level", content, "Report should contain level name")
            self.log_result("test_api_report", "PASS", "API report endpoint executed successfully")
        except Exception as e:
            self.log_result("test_api_report", "FAIL", f"API report failed: {str(e)}")
            self.fail(str(e))

    def test_api_invalid_query_type(self):
        """Test API query with invalid query type."""
        try:
            response = self.client.get(f"/query/invalid?db={self.db_path}")
            self.assertEqual(response.status_code, 400, "Expected 400 status for invalid query type")
            self.assertIn("Invalid query_type", response.json()["detail"])
            self.log_result("test_api_invalid_query_type", "PASS", "API correctly handled invalid query type")
        except Exception as e:
            self.log_result("test_api_invalid_query_type", "FAIL", f"API invalid query type test failed: {str(e)}")
            self.fail(str(e))

    @classmethod
    def tearDownClass(cls):
        """Generate and save Markdown report after all tests."""
        report = f"# OffGridDB Test Report\n\nGenerated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        report += f"Total Tests: {len(cls.test_results)}\n"
        report += f"Passed: {sum(1 for r in cls.test_results if r['status'] == 'PASS')}\n"
        report += f"Failed: {sum(1 for r in cls.test_results if r['status'] == 'FAIL')}\n\n"
        report += "## Test Results\n\n"
        for result in cls.test_results:
            report += f"- **Test**: {result['test']}\n"
            report += f"  - **Status**: {result['status']}\n"
            report += f"  - **Message**: {result['message']}\n"
            report += f"  - **Timestamp**: {result['timestamp']}\n\n"

        print("\n=== Final Test Report ===")
        print(report)

        with open("offgrid_test_report.md", "w") as f:
            f.write(report)

if __name__ == "__main__":
    unittest.main()