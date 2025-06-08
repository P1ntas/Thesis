import duckdb
import os
import csv
import re
from pathlib import Path
from typing import List


BASE_DIR     = Path(__file__).resolve().parent
DATA_DIR     = BASE_DIR / "../data/tpch"
SCHEMA_SQL   = DATA_DIR / "parquet/schema.sql"
LOAD_SQL     = DATA_DIR / "parquet/load.sql"
QUERIES_DIR  = DATA_DIR / "queries"
RESULTS_DIR  = BASE_DIR / "../results"
CSV_OUTPUT   = RESULTS_DIR / "tpch_selectivity.csv"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def split_sql(statements: str) -> List[str]:
    stmts, buf, depth, in_s, in_d = [], [], 0, False, False
    for ch in statements:
        if ch == "'" and not in_d:
            in_s = not in_s
        elif ch == '"' and not in_s:
            in_d = not in_d
        elif ch == '(' and not (in_s or in_d):
            depth += 1
        elif ch == ')' and not (in_s or in_d):
            depth -= 1
        if ch == ';' and not (in_s or in_d) and depth == 0:
            stmt = ''.join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf.clear()
            continue
        buf.append(ch)
    tail = ''.join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


_comment_re = re.compile(r"--.*?$|/\*.*?\*/", re.S | re.M)

def strip_comments(sql: str) -> str:
    return re.sub(_comment_re, " ", sql)


def extract_from_clause(body: str) -> str:
    lower = body.lower()
    depth, start = 0, None
    for i, ch in enumerate(lower):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and lower.startswith("from", i) and not lower[i-1].isalnum():
            start = i
            break
    if start is None:
        raise ValueError("No FROM clause found")
    stop_keywords = ("group", "having", "order", "limit", "union",
                     "except", "intersect")
    depth = 0
    end = len(body)
    i = start
    while i < len(lower):
        ch = lower[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0:
            for kw in stop_keywords:
                if lower.startswith(kw, i) and lower[i+len(kw)].isspace():
                    end = i
                    i = len(lower) 
                    break
        i += 1
    return body[start:end].strip()


def count_rows_before_aggregation(con: duckdb.DuckDBPyConnection,
                                  sql: str) -> int:
    sql_nc = strip_comments(sql).strip().rstrip(';')
    with_block, body = "", sql_nc
    if body.lower().startswith("with"):
        depth = 0
        for i, ch in enumerate(body):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif depth == 0 and body[i:i+6].lower() == "select":
                with_block, body = body[:i], body[i:]
                break

    from_clause = extract_from_clause(body)
    inner = f"SELECT 1 {from_clause}"
    count_sql = f"{with_block}\nSELECT COUNT(*) FROM ({inner}) AS _x"
    return con.execute(count_sql).fetchone()[0]


def rows_in_used_tables(con: duckdb.DuckDBPyConnection,
                        sql: str,
                        base_tables: List[str]) -> int:
    used = [t for t in base_tables
            if re.search(rf"\b{re.escape(t)}\b", sql, re.I)]
    return sum(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
               for t in used)


con = duckdb.connect(database=':memory:')
con.execute(Path(SCHEMA_SQL).read_text())
con.execute(Path(LOAD_SQL).read_text())

base_tables = [r[0] for r in
               con.execute("""SELECT table_name
                              FROM information_schema.tables
                              WHERE table_schema='main'
                                AND table_type='BASE TABLE';""").fetchall()]

with CSV_OUTPUT.open('w', newline='') as csvfile:
    fieldnames = ['query_file',
                  'rows_before_agg',
                  'input_rows',
                  'selectivity',
                  'selectivity_percent']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for qfile in sorted(QUERIES_DIR.glob('*.sql'),
                        key=lambda p: int(p.stem)):        
        print(f"Processing {qfile.name}")
        statements = split_sql(qfile.read_text())
        if not statements:
            print(f"No SQL found")
            continue

        select_idxs = [i for i, s in enumerate(statements)
                       if s.lstrip().lower().startswith("select")]
        if not select_idxs:
            print(f"No stand-alone SELECT")
            continue
        main_sel_idx = select_idxs[-1]
        main_select  = statements[main_sel_idx]

        for i, stmt in enumerate(statements):
            if i == main_sel_idx:
                continue
            con.execute(stmt)

        try:
            rows_before = count_rows_before_aggregation(con, main_select)
        except Exception as ex:
            print(f" Error in pre-agg count: {ex}")
            rows_before = 0

        try:
            denom = rows_in_used_tables(con, main_select, base_tables)
        except Exception as ex:
            print(f" Error in denom calc: {ex}")
            denom = 0

        sel = rows_before / denom if denom else 0

        writer.writerow({
            'query_file'          : qfile.name,
            'rows_before_agg'     : rows_before,
            'input_rows'          : denom,
            'selectivity'         : sel,
            'selectivity_percent' : sel * 100
        })

        for i in range(main_sel_idx + 1, len(statements)):
            con.execute(statements[i])

print(f"\nSelectivity results written to {CSV_OUTPUT}")
