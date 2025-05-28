import sqlite3
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

class OffGridDB:
    def __init__(self, db_path: str, log_file: Optional[str] = None):
        """Initialize SQLite database connection."""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        if log_file:
            logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def connect(self):
        """Connect to SQLite database."""
        if self.conn is None or self.conn.closed:  # Check if connection is closed
            try:
                self.conn = sqlite3.connect(self.db_path)
                self.conn.row_factory = sqlite3.Row  # Enable row factory for dict-like access
                self.cursor = self.conn.cursor()
                self.logger.info(f"Connected to database: {self.db_path}")
            except sqlite3.Error as e:
                self.logger.error(f"Database connection failed: {e}")
                raise Exception(f"Database connection failed: {e}")

    def create_tables(self, drop_if_exists: bool = False):
        """Create tables for levels, monthly_costs, and fixed_costs."""
        try:
            if drop_if_exists:
                self.cursor.execute("DROP TABLE IF EXISTS fixed_costs")
                self.cursor.execute("DROP TABLE IF EXISTS monthly_costs")
                self.cursor.execute("DROP TABLE IF EXISTS levels")

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
            self.logger.info("Tables created successfully")
        except sqlite3.Error as e:
            self.logger.error(f"Table creation failed: {e}")