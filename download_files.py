#!/usr/bin/env python3
"""
Download files from the Snowflake multi-tenancy shared stage.

Usage:
    python download_files.py --connection bank1
    python download_files.py --connection bank1 --list-only
"""

import os
import argparse
import snowflake.connector
from pathlib import Path

VIEW = "MD_TEST.DOC_AI.ECOS_RAW_SV_STAGE_FILES_DOWNLOAD_MT"
STAGE = "MD_TEST.DOC_AI.RAW_ST_DOC_MT"


def get_snowflake_connection(connection_name: str):
    if not connection_name:
        raise ValueError("Connection name is required. Use --connection or set SNOWFLAKE_CONNECTION_NAME")
    return snowflake.connector.connect(connection_name=connection_name)


def get_available_files(conn) -> list[dict]:
    cursor = conn.cursor()
    cursor.execute(f"SELECT TENANT_ID, FILE_NAME, FILE_PATH, FILE_SIZE, DOWNLOAD_URL FROM {VIEW}")
    columns = [desc[0] for desc in cursor.description]
    files = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return files


def download_file(conn, file: dict, output_dir: Path) -> bool:
    cursor = conn.cursor()
    out_dir = output_dir / file["TENANT_ID"]
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        file_path = file["FILE_PATH"]
        cursor.execute(f"GET '@{STAGE}/{file_path}' 'file://{out_dir}/'")
        return True
    except Exception as e:
        print(f"  Error downloading: {e}")
        return False
    finally:
        cursor.close()


def main():
    parser = argparse.ArgumentParser(description="Download files from Snowflake multi-tenancy stage")
    parser.add_argument("--connection", "-c", help="Snowflake connection name")
    parser.add_argument("--output-dir", "-o", default="./downloads", help="Output directory")
    parser.add_argument("--role", "-r", help="Role to use (e.g. BANK1_ROLE)")
    parser.add_argument("--list-only", "-l", action="store_true", help="List files without downloading")
    args = parser.parse_args()

    conn_name = args.connection or os.getenv("SNOWFLAKE_CONNECTION_NAME")
    if not conn_name:
        parser.error("Connection name required. Use --connection or set SNOWFLAKE_CONNECTION_NAME")

    print("Connecting to Snowflake...")
    conn = get_snowflake_connection(conn_name)

    if args.role:
        print(f"Switching to role: {args.role}")
        conn.cursor().execute(f"USE ROLE {args.role}")

    current_role = conn.cursor().execute("SELECT CURRENT_ROLE()").fetchone()[0]
    print(f"Current role: {current_role}")

    print("\nFetching available files...")
    files = get_available_files(conn)

    if not files:
        print("No files available for this role.")
        conn.close()
        return

    print(f"Found {len(files)} file(s):\n")
    for i, f in enumerate(files, 1):
        size_kb = f["FILE_SIZE"] / 1024 if f["FILE_SIZE"] else 0
        print(f"  {i}. [{f['TENANT_ID']}] {f['FILE_NAME']} ({size_kb:.1f} KB)")

    if args.list_only:
        conn.close()
        return

    output_dir = Path(args.output_dir).absolute()
    print(f"\nDownloading to: {output_dir}")

    success_count = 0
    for i, f in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] Downloading {f['FILE_NAME']}...")
        if download_file(conn, f, output_dir):
            print(f"  Saved to: {output_dir / f['TENANT_ID'] / f['FILE_NAME']}")
            success_count += 1
        else:
            print(f"  Failed to download")

    print(f"\nCompleted: {success_count}/{len(files)} files downloaded")
    conn.close()


if __name__ == "__main__":
    main()
