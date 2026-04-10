"""Apply db/schema.sql to Neon Postgres via DATABASE_URL (psycopg2, no ORM)."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current_lines: list[str] = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        current_lines.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(current_lines).strip()
            if stmt:
                statements.append(stmt.rstrip(";").strip())
            current_lines = []
    return statements


def _table_names_from_schema(sql: str) -> list[str]:
    pattern = re.compile(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)",
        re.IGNORECASE,
    )
    return pattern.findall(sql)


def main() -> None:
    try:
        root = _project_root()
        load_dotenv(root / ".env")

        database_url = os.environ.get("DATABASE_URL")
        if not database_url or not database_url.strip():
            raise RuntimeError(
                "DATABASE_URL is missing or empty. Set it in .env at the project root."
            )

        schema_path = Path(__file__).resolve().parent / "schema.sql"
        if not schema_path.is_file():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        sql_text = schema_path.read_text(encoding="utf-8")
        statements = _split_sql_statements(sql_text)
        table_names = _table_names_from_schema(sql_text)

        conn = psycopg2.connect(database_url)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            for stmt in statements:
                if not stmt:
                    continue
                cur.execute(stmt + ";")
        finally:
            cur.close()
            conn.close()

        for name in table_names:
            print(f"Table created: {name}")

        print("Schema applied successfully")
    except Exception as e:
        print(f"Error applying schema: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
