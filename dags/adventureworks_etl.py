from datetime import datetime
import os
from dotenv import load_dotenv
from airflow.sdk import dag, task
import clickhouse_connect
import psycopg2

load_dotenv()

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
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        secure=True
    )

def extract_columns(col_type):
    return list(col_type.keys())

def make_pg_safe(cols: list[str]) -> list[str]:
    return [f'"{c}"' for c in cols]

DATABASE_CONFIG = {
  "hr": {
    "d": "department",
    "e": "employee",
    "edh": "employee_department_history",
    "jc": "job_candidate",
    "s": "shift"
  },
  "pe": {
    "a": "address",
    "at": "address_type",
    "be": "business_entity",
    "bea": "business_entity_address",
    "bec": "business_entity_contact",
    "ct": "contact_type",
    "cr": "country_region",
    "e": "email_address",
    "p": "person",
    "pa": "password",
    "pp": "person_phone",
    "pnt": "phone_number_type",
    "sp": "state_province"
  },
  "pr": {
    "bom": "bill_of_materials",
    "c": "culture",
    "d": "document",
    "i": "illustration",
    "l": "location",
    "p": "product",
    "pc": "product_category",
    "pch": "product_cost_history",
    "pd": "product_description",
    "pdoc": "product_document",
    "pi": "product_inventory",
    "plph": "product_list_price_history",
    "pm": "product_model",
    "pmi": "product_model_illustration",
    "pmpdc": "product_model_product_description_culture",
    "pp": "product_photo",
    "ppp": "product_product_photo",
    "pr": "product_review",
    "psc": "product_subcategory",
    "sr": "scrap_reason",
    "th": "transaction_history",
    "tha": "transaction_history_archive",
    "um": "unit_measure",
    "w": "work_order",
    "wr": "work_order_routing"
  },
  "pu": {
    "pod": "purchase_order_detail",
    "poh": "purchase_order_header",
    "pv": "product_vendor",
    "sm": "ship_method",
    "v": "vendor"
  },
  "sa": {
    "c": "customer",
    "cc": "credit_card",
    "cr": "currency_rate",
    "crc": "country_region_currency",
    "cu": "currency",
    "pcc": "person_credit_card",
    "s": "store",
    "sci": "shopping_cart_item",
    "so": "special_offer",
    "sod": "sales_order_detail",
    "soh": "sales_order_header",
    "sohsr": "sales_order_header_sales_reason",
    "sop": "special_offer_product",
    "sp": "sales_person",
    "spqh": "sales_person_quota_history",
    "sr": "sales_reason",
    "st": "sales_territory",
    "sth": "sales_territory_history",
    "tr": "sales_tax_rate"
  }
}

DATA_TYPES = {
    'integer': 'Int32',
    'bigint': 'Int64',
    'boolean': 'UInt8',
    'text': 'String',
    'character varying': 'String',
    'timestamp without time zone': 'DateTime',
    'timestamp with time zone': 'DateTime',
    'date': 'Date',
    'numeric': 'Decimal(38, 10)',
    'double precision': 'Decimal(18, 2)',
    'real': 'Decimal(18, 2)',
    'uuid': 'UUID'
}

@dag(
    dag_id="adventureworks_etl",
    schedule="@hourly",
    dag_display_name="ETL for AdventureWorks",
    description="AdventureWorks",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "adventureworks", "postgres", "clickhouse"],
)
def sync():
    @task
    def extract_table_columns(schema, table):

        conn = pg()
        cur = conn.cursor()

        cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = %s
                    ORDER BY ordinal_position
                    """, (schema, table))

        rows = cur.fetchall()

        col_type = {col: dtype for col, dtype in rows}

        return col_type

    @task
    def extract_pk_columns(schema, table):
        conn = pg()
        cur = conn.cursor()

        cur.execute(f"""
                SELECT kc.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kc 
                    ON kc.constraint_name = tc.constraint_name
                    AND kc.table_schema = tc.table_schema
                    AND kc.table_name = tc.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND kc.table_schema = %s
                  AND kc.table_name = %s
                ORDER BY kc.ordinal_position
        """, (schema, table))

        pk_columns = cur.fetchall()

        return pk_columns


    @task
    def make_clickhouse_safe(col_type, pk_columns):
        updated = []

        for col, ptype in col_type.items():
            if col in pk_columns and ptype in ("integer", "bigint"):
                updated.append(f"{col} UInt64")
            elif ptype in DATA_TYPES:
                updated.append(f"{col} {DATA_TYPES[ptype]}")
            else:
                updated.append(f"{col} String")

        return updated

    @task
    def create_tables(target, columns, pk):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DL.{target}
        (
            {",".join(list(columns))}
        )
        ENGINE = ReplacingMergeTree(modifieddate)
        ORDER BY ({",".join(pk)});
        """

        ddr = f"""
        CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DL.{target}_staging
        AS ADVENTUREWORKS_DL.{target}
        ENGINE = Memory;
        """

        ch().query(ddl)
        ch().query(ddr)

    @task
    def extract_delta(schema, table, columns_with_types):
        conn = pg()
        cur = conn.cursor()
        pure_columns = extract_columns(columns_with_types)
        safe_cols = make_pg_safe(pure_columns)
        qry = f"""
        SELECT {", ".join(safe_cols)}
        FROM {schema}.{table}
        WHERE modifieddate > (
            SELECT COALESCE(max(modifieddate), '1900-01-01')
            FROM {schema}.{table}
        );
        """
        cur.execute(qry)
        data = cur.fetchall()
        cur.close()
        conn.close()
        return data

    @task
    def load_staging(target, columns_with_types, rows):
        columns = extract_columns(columns_with_types)
        if rows:
            ch().insert(f"{target}_staging", rows, column_names=columns)

    @task
    def merge_upsert(target):
        ch().query(f"""
        INSERT INTO ADVENTUREWORKS_DL.{target}
        SELECT * FROM ADVENTUREWORKS_DL.{target}_staging;
        """)

        ch().query(f"""TRUNCATE TABLE ADVENTUREWORKS_DL.{target}_staging;""")

    @task
    def delete_obsolete(target, pk):
        pk_join = " AND ".join([f"t.{c}=p.{c}" for c in pk])
        ch().query(f"""
            ALTER TABLE ADVENTUREWORKS_DL.{target}
            DELETE WHERE {pk} NOT IN (SELECT {pk} FROM ADVENTUREWORKS_DL.{target}_staging);
        """)

    for schema, tables in DATABASE_CONFIG.items():
        print(schema)
        for table, target in tables.items():
            etc = extract_table_columns.override(task_id = f"get_columns_{target}")(schema, table)
            epc = extract_pk_columns.override(task_id = f"get_pk_{target}")(schema, table)
            mcs = make_clickhouse_safe.override(task_id = f"make_safe{target}")(etc, DATA_TYPES)
            ct = create_tables.override(task_id = f"create_tables_{target}")(target, mcs, epc)
            ed = extract_delta.override(task_id = f"extract_delta_{target}")(schema, table, etc)
            ls = load_staging.override(task_id = f"load_staging_{target}")(target, etc, ed)
            mu = merge_upsert.override(task_id = f"merge_upsert_{target}")(target)
            do = delete_obsolete.override(task_id = f"delete_obs_{target}")(target, epc)

            etc >> epc >> mcs >> ct >> ed >> ls >> mu >> do

sync()