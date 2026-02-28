from __future__ import annotations

from pathlib import Path

from risk_sql_pipeline import get_sql_engine


def _read_batches(sql_file: Path) -> list[str]:
    content = sql_file.read_text(encoding="utf-8")
    batches = []
    current = []
    for line in content.splitlines():
        if line.strip().upper() == "GO":
            statement = "\n".join(current).strip()
            if statement:
                batches.append(statement)
            current = []
            continue
        current.append(line)

    tail = "\n".join(current).strip()
    if tail:
        batches.append(tail)
    return batches


def main() -> None:
    sql_file = Path(__file__).parent / "sql" / "schema.sql"
    engine = get_sql_engine()
    batches = _read_batches(sql_file)

    with engine.begin() as conn:
        for statement in batches:
            conn.exec_driver_sql(statement)

    print(f"Applied schema from {sql_file}")


if __name__ == "__main__":
    main()
