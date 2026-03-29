#!/usr/bin/env python3
"""
Upload files to the multi-tenancy shared stage RAW_ST_DOC_MT.
Each file is registered in ECOS_RAW_TB_STAGE_FILES_MT with the given tenant_id.

Usage:
    python upload_mt.py --connection sfseeurope-mdaeppen --tenant-id BANK1_ROLE --files a.pdf b.pdf
"""

import os
import argparse
from pathlib import Path
import snowflake.connector


def get_snowflake_connection(connection_name: str):
    if not connection_name:
        raise ValueError("Connection name is required. Use --connection or set SNOWFLAKE_CONNECTION_NAME")
    return snowflake.connector.connect(connection_name=connection_name)


def upload_file(conn, file_path: Path, tenant_id: str) -> bool:
    cursor = conn.cursor()
    stage_path = "@MD_TEST.DOC_AI.RAW_ST_DOC_MT"
    try:
        print(f"  Uploading {file_path.name}...")
        cursor.execute(f"PUT 'file://{file_path.absolute()}' '{stage_path}' AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        rows = cursor.fetchall()
        file_size = file_path.stat().st_size
        print(f"  Registering metadata for tenant {tenant_id}...")
        cursor.execute(
            "CALL MD_TEST.DOC_AI.ECOS_RAW_SP_REGISTER_FILE(%s, %s, %s, %s)",
            (tenant_id, file_path.name, file_path.name, file_size)
        )
        result = cursor.fetchone()
        print(f"  {result[0]}")
        return True
    except Exception as e:
        print(f"  Error: {e}")
        return False
    finally:
        cursor.close()


def main():
    parser = argparse.ArgumentParser(description="Upload files to Snowflake multi-tenancy stage")
    parser.add_argument("--connection", "-c", help="Snowflake connection name")
    parser.add_argument("--tenant-id", "-t", required=True, help="Tenant ID (e.g. BANK1_ROLE)")
    parser.add_argument("--files", "-f", nargs="+", required=True, help="Files to upload")
    args = parser.parse_args()

    conn_name = args.connection or os.getenv("SNOWFLAKE_CONNECTION_NAME")
    if not conn_name:
        parser.error("Connection name required. Use --connection or set SNOWFLAKE_CONNECTION_NAME")

    print(f"Connecting to Snowflake...")
    conn = get_snowflake_connection(conn_name)

    cursor = conn.cursor()
    cursor.execute("USE DATABASE MD_TEST")
    cursor.execute("USE SCHEMA DOC_AI")
    cursor.close()

    current_role = conn.cursor().execute("SELECT CURRENT_ROLE()").fetchone()[0]
    print(f"Current role: {current_role}")
    print(f"Tenant ID:    {args.tenant_id}")
    print(f"Files:        {len(args.files)}\n")

    success_count = 0
    for i, f in enumerate(args.files, 1):
        file_path = Path(f)
        print(f"[{i}/{len(args.files)}] {file_path.name}")
        if not file_path.exists():
            print(f"  File not found: {file_path}")
            continue
        if upload_file(conn, file_path, args.tenant_id):
            success_count += 1

    print(f"\nCompleted: {success_count}/{len(args.files)} files uploaded")
    conn.close()


if __name__ == "__main__":
    main()
