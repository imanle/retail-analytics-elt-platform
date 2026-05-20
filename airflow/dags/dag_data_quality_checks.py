"""
dag_data_quality_checks.py

DAG 3: Data Quality Checks

Runs custom quality checks against raw and mart tables and writes
results to quality.data_quality_results.

Schedule: daily at 08:00 UTC (runs after ingestion + dbt transformation)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# Helper — write results to quality.data_quality_results
# ---------------------------------------------------------------------------

def _write_result(conn, check_name, table_name, column_name, check_type, passed, failed_row_count=0, details=None):
    from sqlalchemy import text
    conn.execute(text("""
        INSERT INTO quality.data_quality_results
            (check_name, table_name, column_name, check_type, check_status, failed_row_count, checked_at, details)
        VALUES
            (:check_name, :table_name, :column_name, :check_type, :check_status, :failed_row_count, NOW(), :details)
    """), {
        "check_name": check_name,
        "table_name": table_name,
        "column_name": column_name,
        "check_type": check_type,
        "check_status": "passed" if passed else "failed",
        "failed_row_count": failed_row_count,
        "details": details,
    })


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def check_raw_row_counts(**context):
    """
    Check that each raw table has at least one row.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from db import get_engine
    from sqlalchemy import text

    tables = [
        "raw.orders",
        "raw.order_items",
        "raw.customers",
        "raw.products",
        "raw.shipments",
        "raw.payments",
    ]

    engine = get_engine()
    with engine.begin() as conn:
        for table in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            passed = count > 0
            _write_result(
                conn,
                check_name=f"{table}_has_rows",
                table_name=table,
                column_name=None,
                check_type="row_count",
                passed=passed,
                failed_row_count=0 if passed else 1,
                details=f"Row count: {count}",
            )
            print(f"[quality] {table}: {count} rows -> {'PASS' if passed else 'FAIL'}")


def check_null_primary_keys(**context):
    """
    Check that primary key columns are not null in raw tables.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from db import get_engine
    from sqlalchemy import text

    checks = [
        ("raw.orders",      "order_id"),
        ("raw.order_items", "order_item_id"),
        ("raw.customers",   "customer_id"),
        ("raw.products",    "product_id"),
        ("raw.shipments",   "shipment_id"),
        ("raw.payments",    "payment_id"),
    ]

    engine = get_engine()
    with engine.begin() as conn:
        for table, column in checks:
            null_count = conn.execute(text(
                f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
            )).scalar()
            passed = null_count == 0
            _write_result(
                conn,
                check_name=f"{table}_{column}_not_null",
                table_name=table,
                column_name=column,
                check_type="not_null",
                passed=passed,
                failed_row_count=null_count,
                details=f"Null count: {null_count}",
            )
            print(f"[quality] {table}.{column} null check -> {'PASS' if passed else 'FAIL'} ({null_count} nulls)")


def check_duplicate_orders(**context):
    """
    Check that order_id is unique in marts.fact_orders.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from db import get_engine
    from sqlalchemy import text

    engine = get_engine()
    with engine.begin() as conn:
        dup_count = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT order_id
                FROM marts.fact_orders
                GROUP BY order_id
                HAVING COUNT(*) > 1
            ) dupes
        """)).scalar()
        passed = dup_count == 0
        _write_result(
            conn,
            check_name="fact_orders_order_id_unique",
            table_name="marts.fact_orders",
            column_name="order_id",
            check_type="uniqueness",
            passed=passed,
            failed_row_count=dup_count,
            details=f"Duplicate order_ids: {dup_count}",
        )
        print(f"[quality] fact_orders.order_id unique check -> {'PASS' if passed else 'FAIL'} ({dup_count} dupes)")


def check_negative_amounts(**context):
    """
    Check that total_amount and payment_amount are not negative.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from db import get_engine
    from sqlalchemy import text

    checks = [
        ("marts.fact_orders",   "total_amount"),
        ("marts.fact_orders",   "payment_amount"),
        ("marts.fact_orders",   "shipping_cost"),
        ("marts.fact_order_items", "line_total"),
    ]

    engine = get_engine()
    with engine.begin() as conn:
        for table, column in checks:
            neg_count = conn.execute(text(
                f"SELECT COUNT(*) FROM {table} WHERE {column} < 0"
            )).scalar()
            passed = neg_count == 0
            _write_result(
                conn,
                check_name=f"{table}_{column}_not_negative",
                table_name=table,
                column_name=column,
                check_type="value_range",
                passed=passed,
                failed_row_count=neg_count,
                details=f"Negative values: {neg_count}",
            )
            print(f"[quality] {table}.{column} negative check -> {'PASS' if passed else 'FAIL'} ({neg_count} negatives)")


def check_invalid_dates(**context):
    """
    Check that delivered_at is never before shipped_at in shipments,
    and that order_date is not in the future.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from db import get_engine
    from sqlalchemy import text

    engine = get_engine()
    with engine.begin() as conn:

        # delivered_at should not be before shipped_at
        invalid_delivery = conn.execute(text("""
            SELECT COUNT(*)
            FROM staging.stg_shipments
            WHERE delivered_at IS NOT NULL
              AND delivered_at < shipped_at
        """)).scalar()
        passed = invalid_delivery == 0
        _write_result(
            conn,
            check_name="stg_shipments_delivered_after_shipped",
            table_name="staging.stg_shipments",
            column_name="delivered_at",
            check_type="date_logic",
            passed=passed,
            failed_row_count=invalid_delivery,
            details=f"Shipments where delivered_at < shipped_at: {invalid_delivery}",
        )
        print(f"[quality] shipments date logic -> {'PASS' if passed else 'FAIL'} ({invalid_delivery} invalid)")

        # order_date should not be in the future
        future_orders = conn.execute(text("""
            SELECT COUNT(*)
            FROM staging.stg_orders
            WHERE order_date > CURRENT_DATE
        """)).scalar()
        passed = future_orders == 0
        _write_result(
            conn,
            check_name="stg_orders_order_date_not_future",
            table_name="staging.stg_orders",
            column_name="order_date",
            check_type="date_logic",
            passed=passed,
            failed_row_count=future_orders,
            details=f"Future order dates: {future_orders}",
        )
        print(f"[quality] orders future date check -> {'PASS' if passed else 'FAIL'} ({future_orders} future dates)")


def check_table_freshness(**context):
    """
    Check that key tables were loaded recently (within 48 hours).
    Writes results to quality.table_freshness.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from db import get_engine
    from sqlalchemy import text

    tables = [
        ("raw",     "orders"),
        ("raw",     "order_items"),
        ("staging", "stg_orders"),
        ("marts",   "fact_orders"),
        ("marts",   "fact_daily_sales"),
    ]

    engine = get_engine()
    with engine.begin() as conn:
        for schema, table in tables:
            full_table = f"{schema}.{table}"
            try:
                result = conn.execute(text(
                    f"SELECT COUNT(*), MAX(_loaded_at) FROM {full_table}"
                )).fetchone()
                row_count, max_loaded_at = result

                staleness_hours = None
                is_fresh = True
                if max_loaded_at:
                    staleness_hours = round(
                        (datetime.utcnow() - max_loaded_at).total_seconds() / 3600, 2
                    )
                    is_fresh = staleness_hours <= 48

                conn.execute(text("""
                    INSERT INTO quality.table_freshness
                        (schema_name, table_name, row_count, max_loaded_at, checked_at, is_fresh, staleness_hours)
                    VALUES
                        (:schema_name, :table_name, :row_count, :max_loaded_at, NOW(), :is_fresh, :staleness_hours)
                """), {
                    "schema_name": schema,
                    "table_name": table,
                    "row_count": row_count,
                    "max_loaded_at": max_loaded_at,
                    "is_fresh": is_fresh,
                    "staleness_hours": staleness_hours,
                })
                print(f"[quality] {full_table} freshness -> {'FRESH' if is_fresh else 'STALE'} ({staleness_hours}h old, {row_count} rows)")

            except Exception as e:
                print(f"[quality] Could not check freshness for {full_table}: {e}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_data_quality_checks",
    description="Run data quality checks and write results to quality schema",
    start_date=datetime(2026, 5, 1),
    schedule_interval="0 8 * * *",
    catchup=False,
    default_args=default_args,
    tags=["quality", "retail"],
) as dag:

    start = EmptyOperator(task_id="start")

    raw_row_counts = PythonOperator(
        task_id="check_raw_row_counts",
        python_callable=check_raw_row_counts,
    )

    null_primary_keys = PythonOperator(
        task_id="check_null_primary_keys",
        python_callable=check_null_primary_keys,
    )

    duplicate_orders = PythonOperator(
        task_id="check_duplicate_orders",
        python_callable=check_duplicate_orders,
    )

    negative_amounts = PythonOperator(
        task_id="check_negative_amounts",
        python_callable=check_negative_amounts,
    )

    invalid_dates = PythonOperator(
        task_id="check_invalid_dates",
        python_callable=check_invalid_dates,
    )

    table_freshness = PythonOperator(
        task_id="check_table_freshness",
        python_callable=check_table_freshness,
    )

    end = EmptyOperator(task_id="end")

    start >> [raw_row_counts, null_primary_keys] >> duplicate_orders >> negative_amounts >> invalid_dates >> table_freshness >> end
