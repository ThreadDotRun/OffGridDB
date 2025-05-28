import unittest
import sqlite3
import json
import os
import time
from pathlib import Path
from datetime import datetime

# OffGridDB class
class OffGridDB:
    def __init__(self, db_path: str):
        """Initialize SQLite database connection."""
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def connect(self):
        """Connect to SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            raise Exception(f"Database connection failed: {e}")

    def create_tables(self, drop_if_exists: bool = False):
        """Create tables for levels, monthly_costs, and fixed_costs."""
        try:
            if drop_if_exists:
                self.cursor.execute("DROP TABLE IF EXISTS levels")
                self.cursor.execute("DROP TABLE IF EXISTS monthly_costs")
                self.cursor.execute("DROP TABLE IF EXISTS fixed_costs")

            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS levels (
                    level INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    total_monthly REAL NOT NULL,
                    total_fixed REAL NOT NULL
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_costs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    amount REAL NOT NULL,
                    FOREIGN KEY (level_id) REFERENCES levels(level)
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS fixed_costs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    units INTEGER NOT NULL,
                    unit_type TEXT NOT NULL,
                    unit_cost REAL NOT NULL,
                    total REAL NOT NULL,
                    seller_source TEXT NOT NULL,
                    FOREIGN KEY (level_id) REFERENCES levels(level)
                )
            """)
            self.conn.commit()
        except sqlite3.Error as e:
            raise Exception(f"Table creation failed: {e}")

    def load_json(self, json_path: str, drop_if_exists: bool = False):
        """Load JSON data into SQLite database."""
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)

            self.connect()
            self.create_tables(drop_if_exists)

            for level_data in data.get('levels', []):
                self.cursor.execute("""
                    INSERT INTO levels (level, name, description, total_monthly, total_fixed)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    level_data['level'],
                    level_data['name'],
                    level_data['description'],
                    level_data['total_monthly'],
                    level_data['total_fixed']
                ))

                level_id = level_data['level']

                for mc in level_data.get('monthly_costs', []):
                    self.cursor.execute("""
                        INSERT INTO monthly_costs (level_id, name, amount)
                        VALUES (?, ?, ?)
                    """, (level_id, mc['name'], mc['amount']))

                for fc in level_data.get('fixed_costs', []):
                    self.cursor.execute("""
                        INSERT INTO fixed_costs (level_id, name, units, unit_type, unit_cost, total, seller_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        level_id,
                        fc['name'],
                        fc['units'],
                        fc['unit_type'],
                        fc['unit_cost'],
                        fc['total'],
                        fc['seller_source']
                    ))

            self.conn.commit()
        except (sqlite3.Error, json.JSONDecodeError) as e:
            raise Exception(f"Failed to load JSON into database: {e}")
        except KeyError as e:
            raise Exception(f"Invalid JSON schema: missing required field {e}")
        finally:
            self.close()

    def close(self):
        """Close database connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except sqlite3.Error:
            pass
        finally:
            self.cursor = None
            self.conn = None

    def query(self, query: str, params: tuple = ()):
        """Execute a custom query and return results."""
        try:
            self.connect()
            self.cursor.execute(query, params)
            results = self.cursor.fetchall()
            return results
        except sqlite3.Error as e:
            raise Exception(f"Query failed: {e}")
        finally:
            self.close()

# Unit Test Class
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