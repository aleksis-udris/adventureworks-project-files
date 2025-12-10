import os
import clickhouse_connect
from datetime import datetime, date
from decimal import Decimal
import psycopg2
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

MAX_CLICKHOUSE_DATE = date(2100, 12, 31)

def pg():
    return psycopg2.connect(
        dbname=os.getenv("ADVENTUREWORKS_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASS"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )

def ch():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASS"),
        port=8443,
        secure=True
    )

def check_insert(data, columns):
    for i, row in enumerate(data):
        if len(row) != len(columns):
            print(f"Row {i} length mismatch: expected {len(columns)}, got {len(row)}")
            print("Row content:", row)
            raise ValueError("Mismatched row found before insert.")

def clean_values_in_rows(rows, columns):
    date_keyword = 'date'
    indices = [i for i, col in enumerate(columns) if date_keyword in col.lower()]

    # Add validation: only use indices that are valid for the actual row data
    if rows:
        max_row_length = max(len(row) for row in rows)
        # Filter out indices that are beyond the actual row length
        indices = [i for i in indices if i < max_row_length]

    cleaned_rows = []
    for row in rows:
        cleaned_row = []
        for value in row:
            if isinstance(value, Decimal):
                cleaned_row.append(float(value))
            elif value is None:
                cleaned_row.append(0)
            else:
                cleaned_row.append(value)

        ready_row = list(cleaned_row)

        for idx in indices:
            # Double-check bounds for this specific row
            if idx >= len(ready_row):
                continue

            val = ready_row[idx]
            if isinstance(val, str):
                ready_row[idx] = datetime.strptime(val, "%Y-%m-%d").date()
            elif isinstance(val, datetime):
                ready_row[idx] = val.date()

            if isinstance(val, datetime):
                val = val.date()

            if isinstance(val, date) and val > MAX_CLICKHOUSE_DATE:
                ready_row[idx] = MAX_CLICKHOUSE_DATE

        cleaned_rows.append(tuple(ready_row))

    return cleaned_rows

def clamp_date(val):
    if isinstance(val, date) and val > MAX_CLICKHOUSE_DATE:
        return MAX_CLICKHOUSE_DATE
    return val

def partition_rows_fixed_batch(data, batch_size):
    partitioned = defaultdict(list)
    batch_id = 0

    for i, row in enumerate(data):
        if i % batch_size == 0:
            batch_id += 1
        partitioned[batch_id].append(row)

    return partitioned