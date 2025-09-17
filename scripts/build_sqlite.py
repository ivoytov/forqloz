#!/usr/bin/env python3
import csv
import sqlite3
from pathlib import Path


def infer_type(value: str):
    if value is None or value == "":
        return None
    # Try int
    try:
        i = int(value)
        return i
    except Exception:
        pass
    # Try float
    try:
        f = float(value)
        return f
    except Exception:
        pass
    # Leave as string
    return value


def create_table(conn: sqlite3.Connection, table: str, headers: list[str], types: list[str]):
    # Use quoted identifiers to preserve original header names (even with spaces)
    cols = ", ".join([f'"{h}" {t}' for h, t in zip(headers, types)])
    conn.execute(f"DROP TABLE IF EXISTS '{table}'")
    conn.execute(f"CREATE TABLE '{table}' ({cols})")


SCHEMA_HINTS: dict[str, dict[str, str]] = {
    # Explicitly typed columns per table (SQLite types)
    'lots': {
        'block': 'INTEGER',
        'lot': 'INTEGER',
        'BBL': 'INTEGER',
    },
    'bids': {
        'judgement': 'REAL',
        'upset_price': 'REAL',
        'winning_bid': 'REAL',
        'auction_date': 'TEXT',
    },
    'pluto': {
        'Block': 'INTEGER',
        'Lot': 'INTEGER',
        'BBL': 'INTEGER',
        'LandUse': 'INTEGER',
        'LotArea': 'INTEGER',
        'BldgArea': 'INTEGER',
        'YearBuilt': 'INTEGER',
        'YearAlter1': 'INTEGER',
        'YearAlter2': 'INTEGER',
    },
    'cases': {
        'auction_date': 'TEXT',
    },
    'auction_sales': {
        'BLOCK': 'INTEGER',
        'LOT': 'INTEGER',
        'SALE PRICE': 'REAL',
        'SALE DATE': 'TEXT',
    },
}


def infer_sqlite_type(values: list[str]) -> str:
    # Start assuming INTEGER; widen as needed
    type_kind = 'INTEGER'
    for v in values:
        if v is None or v == "":
            continue
        s = str(v)
        try:
            int(s)
            continue
        except Exception:
            pass
        try:
            float(s)
            type_kind = 'REAL'
            continue
        except Exception:
            return 'TEXT'
    return type_kind


def load_csv_into_table(conn: sqlite3.Connection, csv_path: Path, table: str):
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)

        # Drop placeholder columns in lots (last 6 like Column8..Column13). Keep 'unit'.
        if table == 'lots':
            keep_mask = [not (h.startswith('Column') and h[len('Column'):].isdigit()) for h in headers]
            headers = [h for h, keep in zip(headers, keep_mask) if keep]

        # Buffer rows first to infer column types robustly
        raw_rows: list[list[str | None]] = []
        for i, row in enumerate(reader, start=2):  # start=2 to account for header line
            if len(row) != len(headers):
                # Attempt to recover concatenated records when the total length is a multiple
                if table == 'lots':
                    # If we dropped placeholder columns, we need to align to original CSV length
                    # Compute original header length by reading raw row length; we can map by slicing
                    # Strategy: if extra columns exist, truncate tail columns to match kept headers
                    if len(row) >= len(headers):
                        row = row[:len(headers)]
                        norm = [None if (c is None or c == "") else c for c in row]
                        raw_rows.append(norm)
                        continue
                if len(row) % len(headers) == 0 and len(row) > len(headers):
                    for j in range(0, len(row), len(headers)):
                        chunk = row[j:j + len(headers)]
                        norm = [None if (c is None or c == "") else c for c in chunk]
                        raw_rows.append(norm)
                    continue
                # Otherwise, skip malformed row with a warning
                print(f"Warning: Skipping malformed row {i} in {csv_path.name}: has {len(row)} cols, expected {len(headers)}")
                continue

            # Normalize empties to None
            norm = [None if (c is None or c == "") else c for c in row]
            raw_rows.append(norm)

        # Determine types per column
        hints = SCHEMA_HINTS.get(table, {})
        col_types: list[str] = []
        for ci, h in enumerate(headers):
            if h in hints:
                col_types.append(hints[h])
            else:
                sample_values = [r[ci] for r in raw_rows]
                col_types.append(infer_sqlite_type(sample_values))

        # Create table with inferred types
        create_table(conn, table, headers, col_types)

        # Prepare insert
        placeholders = ",".join(["?"] * len(headers))
        cols_quoted = ", ".join(['"{}"'.format(h) for h in headers])
        insert_sql = f"INSERT INTO '{table}' ({cols_quoted}) VALUES ({placeholders})"

        # Cast values per column type
        def cast_val(val, typ):
            if val is None:
                return None
            if typ == 'INTEGER':
                try:
                    return int(str(val))
                except Exception:
                    return None
            if typ == 'REAL':
                try:
                    return float(str(val))
                except Exception:
                    return None
            return val

        typed_rows = [
            [cast_val(v, t) for v, t in zip(r, col_types)]
            for r in raw_rows
        ]

        # Use a transaction for speed
        with conn:
            conn.executemany(insert_sql, typed_rows)


def main():
    root = Path(__file__).resolve().parents[1]
    web_dir = root / "web" / "foreclosures"
    db_path = web_dir / "foreclosures.sqlite"

    tables = {
        "auction_sales": web_dir / "auction_sales.csv",
        "cases": web_dir / "cases.csv",
        "lots": web_dir / "lots.csv",
        "bids": web_dir / "bids.csv",
        "pluto": web_dir / "pluto.csv",
    }

    conn = sqlite3.connect(db_path)
    try:
        for table, csv_path in tables.items():
            if not csv_path.exists():
                raise FileNotFoundError(f"Missing CSV: {csv_path}")
            load_csv_into_table(conn, csv_path, table)

        # Helpful indexes
        with conn:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cases_key ON cases(\"case_number\", \"auction_date\")")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_cases_case_boro ON cases(\"case_number\", \"borough\")")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_bids_key ON bids(\"case_number\", \"auction_date\")")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lots_key ON lots(\"case_number\")")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pluto_bbl ON pluto(\"BBL\")")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_geo ON auction_sales(\"BOROUGH\", \"BLOCK\", \"LOT\")")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON auction_sales(\"SALE DATE\")")

        print(f"Built SQLite DB at: {db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
