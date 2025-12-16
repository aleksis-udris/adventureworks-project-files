import json
import os
import uuid
from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import clickhouse_connect
import psycopg2
from dotenv import load_dotenv

load_dotenv()


MAX_CLICKHOUSE_DATE = datetime(2100, 12, 31, 0, 0, 0, 0)

DIM_SURROGATE_KEY_COLUMN = {
    "DimCustomer": "CustomerKey",
    "DimProduct": "ProductKey",
    "DimStore": "StoreKey",
    "DimEmployee": "EmployeeKey",
    "DimWarehouse": "WarehouseKey",
    "DimVendor": "VendorKey",
    "DimSalesTerritory": "TerritoryKey",
    "DimPromotion": "PromotionKey",
    "DimProductCategory": "ProductCategoryKey",
    "DimReturnReason": "ReturnReasonKey",
    "DimDate": "DateKey",
}

ERROR_RECORD_COLUMNS: List[str] = [
    "ErrorID",
    "ErrorDate",
    "SourceTable",
    "RecordNaturalKey",
    "ErrorType",
    "ErrorSeverity",
    "ErrorMessage",
    "FailedData",
    "ProcessingBatchID",
    "TaskName",
    "IsRecoverable",
    "RetryCount",
    "LastAttemptDate",
    "IsResolved",
    "ResolutionComment",
]


def pg():
    return psycopg2.connect(
        dbname=os.getenv("ADVENTUREWORKS_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASS"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )


def ch():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASS"),
        port=8443,
        secure=True,
    )


def check_insert(data, columns):
    for i, row in enumerate(data):
        if len(row) != len(columns):
            print(f"Row {i} length mismatch: expected {len(columns)}, got {len(row)}")
            print("Row content:", row)
            raise ValueError("Mismatched row found before insert.")


def clean_values_in_rows(rows, columns):
    date_keyword = "date"
    indices = [i for i, col in enumerate(columns) if date_keyword in col.lower()]
    if rows:
        max_row_length = max(len(row) for row in rows)
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
            if idx >= len(ready_row):
                continue
            val = ready_row[idx]
            if isinstance(val, str):
                try:
                    val = datetime.strptime(val, "%Y-%m-%d")
                except ValueError:
                    pass
                else:
                    ready_row[idx] = val
            val = ready_row[idx]
            if isinstance(val, date) and not isinstance(val, datetime):
                val = datetime.combine(val, time.min)
                ready_row[idx] = val
            val = ready_row[idx]
            if isinstance(val, datetime):
                if val.tzinfo is not None:
                    val = val.replace(tzinfo=None)
                    ready_row[idx] = val
                if val > MAX_CLICKHOUSE_DATE:
                    ready_row[idx] = MAX_CLICKHOUSE_DATE
        cleaned_rows.append(tuple(ready_row))
    return cleaned_rows


def partition_rows_fixed_batch(data, batch_size):
    partitioned = defaultdict(list)
    batch_id = 0
    for i, row in enumerate(data):
        if i % batch_size == 0:
            batch_id += 1
        partitioned[batch_id].append(row)
    return partitioned


def _ch_escape_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Decimal):
        return str(float(value))
    if isinstance(value, datetime):
        return "'" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"
    if isinstance(value, date):
        return "'" + value.strftime("%Y-%m-%d") + "'"
    s = str(value)
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return "'" + s + "'"


def _chunked(seq: Sequence[Any], size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def bulk_lookup_dimension_keys(
    ch_client,
    dimension_table: str,
    natural_key_column: str,
    natural_key_values: Iterable[Any],
    *,
    surrogate_key_column: Optional[str] = None,
    is_scd2: bool = True,
    chunk_size: int = 5000,
):
    surrogate_key_column = surrogate_key_column or DIM_SURROGATE_KEY_COLUMN.get(
        dimension_table
    )
    if not surrogate_key_column:
        raise ValueError(
            f"Unknown dimension table '{dimension_table}' (no surrogate key mapping)"
        )
    uniq: List[Any] = []
    seen = set()
    for v in natural_key_values:
        if v is None:
            continue
        if v in seen:
            continue
        seen.add(v)
        uniq.append(v)
    if not uniq:
        return {}
    mapping: Dict[Any, Any] = {}
    for chunk in _chunked(uniq, chunk_size):
        in_list = ",".join(_ch_escape_literal(v) for v in chunk)
        where = f"{natural_key_column} IN ({in_list})"
        if is_scd2:
            where += " AND IsCurrent = 1"
        query = (
            f"SELECT {natural_key_column}, {surrogate_key_column} "
            f"FROM ADVENTUREWORKS_DWS.{dimension_table} "
            f"WHERE {where}"
        )
        res = ch_client.query(query)
        for nat, sk in res.result_rows:
            mapping[nat] = sk
    return mapping


def build_error_record(
    *,
    source_table: str,
    record_natural_key: str,
    error_type: str,
    error_message: str,
    failed_data: Any,
    task_name: str,
    batch_id: str,
    severity: str = "Warning",
    is_recoverable: int = 1,
):
    error_id = uuid.uuid4().int & ((1 << 64) - 1)
    now = datetime.now()
    try:
        failed_json = json.dumps(failed_data, default=str, ensure_ascii=False)
    except Exception:
        failed_json = str(failed_data)
    return (
        error_id,
        now,
        source_table,
        record_natural_key,
        error_type,
        severity,
        error_message,
        failed_json,
        batch_id,
        task_name,
        int(is_recoverable),
        0,
        None,
        0,
        None,
    )


def insert_error_records(ch_client, error_rows: Sequence[Tuple[Any, ...]]):
    if not error_rows:
        return
    ch_client.insert(
        "ADVENTUREWORKS_DWS.error_records",
        list(error_rows),
        column_names=ERROR_RECORD_COLUMNS,
    )