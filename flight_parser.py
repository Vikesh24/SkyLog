# ./flight_parser.py
"""
A simple pyton programme to parse the flight data in the csv file to sqlite db
"""

import sqlite3
import csv
import os

DB_FILE = "flightops.db"
CSV_FILE = "flightlog.csv"

def init_db():
    """
    Function to initialize a DB with flights table.

    This function initializes a flightops DB with a flights table
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS flights(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_no TEXT,
            departure TEXT,
            arrival TEXT,
            status TEXT 
        )
    ''')

    conn.commit()
    conn.close()

def process_flights():
    conn = None
    file = None

    try:
        # Open DB connection
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Open the CSV file
        file = open(CSV_FILE, newline="")
        reader = csv.DictReader(file)

        # Insert Data
        for row in reader:
            cursor.execute("""
                INSERT INTO flights (flight_no, departure, arrival, status)
                VALUES(?, ?, ?, ?)
            """, (row['flight_no'], row['departure'], row['arrival'], row['status']))
        conn.commit()
        print("[✓] Flight data successfully inserted into database.")

    except FileNotFoundError:
        print("[✗] CSV file not found.")

    except sqlite3.Error as e:
        print(f"[✗] DB error: {e}")

    except Exception as e:
        print(f"[✗] Unknown error: {e}")

    finally:
        if file:
            file.close()
            print("[i] CSV file closed.")
        if conn:
            conn.close()
            print("[i] DB connection closed.")

if __name__ == "__main__":
    if not os.path.exists(DB_FILE):
        init_db()
    process_flights()