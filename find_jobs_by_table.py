import argparse
import re
from pathlib import Path

import pandas as pd

OUTPUT_COLUMNS = [
    "JOB ETL TARGET SCHEMA",
    "JOB ETL TARGET TABLE",
    "PROJECT_NAME",
    "JOB_NAME",
    "FOLDER_PATH",
    "DATA_CONNECTION",
    "STAGE_TYPE",
    "STAGE_NAME",
    "READ_MODE",
    "WRITE_MODE",
    "SELECT_STATEMENT",
    "INSERT_STATEMENT",
    "UPDATE_STATEMENT",
]


def normalize_table_name(name: str) -> str:
    """Normalize table references for tolerant matching across SQL dialect quirks."""
    if not isinstance(name, str):
        return ""
    x = name.strip()
    x = re.sub(r"[;\s]+$", "", x)
    x = x.replace("`", "").replace('"', "")
    x = x.replace("[", "").replace("]", "")
    x = re.sub(r"\s+", "", x)
    return x.upper()


def strip_sql_comments(sql: str) -> str:
    """Remove SQL comments so commented tables are not parsed as real dependencies."""
    if not isinstance(sql, str) or not sql:
        return ""
    # Remove /* ... */ block comments first, then single-line -- comments.
    no_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    no_line = re.sub(r"--[^\n\r]*", " ", no_block)
    return no_line


def extract_tables_from_select(sql: str) -> set[str]:
    """Extract table tokens after FROM/JOIN from SELECT SQL."""
    if not isinstance(sql, str) or not sql.strip():
        return set()
    sql = strip_sql_comments(sql)
    pattern = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z0-9_.$`\[\]\"]+)", re.IGNORECASE)
    tables = set()
    for m in pattern.finditer(sql):
        tables.add(normalize_table_name(m.group(1)))
    return tables


def extract_tables_from_insert(sql: str) -> set[str]:
    """Extract target table tokens after INSERT INTO from INSERT SQL."""
    if not isinstance(sql, str) or not sql.strip():
        return set()
    pattern = re.compile(
        r"\b(?:INSERT\s+INTO|INSERT\s+OVERWRITE\s+TABLE|MERGE\s+INTO)\s+([A-Za-z0-9_.$`\[\]\"]+)",
        re.IGNORECASE,
    )
    tables = set()
    for m in pattern.finditer(sql):
        tables.add(normalize_table_name(m.group(1)))
    return tables


def parse_insert_target(insert_sql: str) -> tuple[str, str]:
    """Return target schema/table parsed from insert SQL or plain schema.table token."""
    if not isinstance(insert_sql, str) or not insert_sql.strip():
        return "", ""

    text = insert_sql.strip()
    m = re.search(
        r"\b(?:INSERT\s+INTO|INSERT\s+OVERWRITE\s+TABLE|MERGE\s+INTO)\s+([A-Za-z0-9_.$`\[\]\"]+)",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        raw_target = m.group(1)
    else:
        token = re.match(r"^\s*([A-Za-z0-9_.$`\[\]\"]+)\s*;?\s*$", text)
        raw_target = token.group(1) if token else ""

    target = normalize_table_name(raw_target)
    if not target:
        return "", ""

    parts = target.split(".")
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return "", parts[-1]


def has_text(x) -> bool:
    return isinstance(x, str) and x.strip() != ""


def _table_name_matches(target_table: str, sql_table_token: str) -> bool:
    """Match table names tolerantly (supports db.schema.table vs schema.table input)."""
    target = normalize_table_name(target_table)
    token = normalize_table_name(sql_table_token)
    if not target or not token:
        return False
    if token == target:
        return True

    target_parts = target.split(".")
    token_parts = token.split(".")

    target_schema = target_parts[-2] if len(target_parts) >= 2 else ""
    token_schema = token_parts[-2] if len(token_parts) >= 2 else ""
    target_table_name = target_parts[-1]
    token_table_name = token_parts[-1]

    # If both sides have schema, schema must match.
    if target_schema and token_schema:
        if target_schema != token_schema:
            return False
        if target_table_name == token_table_name:
            return True

    # Common ETL naming variant: SCHEMA.TABLE vs SCHEMA.SCHEMA_TABLE
    if target_schema and token_schema:
        if token_table_name == f"{token_schema}_{target_table_name}":
            return True
        if target_table_name == f"{target_schema}_{token_table_name}":
            return True

        return False

    # If one side has no schema, fallback to table-name comparison only.
    return token_table_name == target_table_name


def scan_excel(input_path: Path, target_table: str):
    target = normalize_table_name(target_table)
    all_df = _collect_all_rows(input_path)
    if all_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    select_df = all_df[all_df["SELECT_STATEMENT"].apply(has_text)].copy()
    if not select_df.empty:
        select_df = select_df[
            select_df["SELECT_STATEMENT"].apply(
                lambda sql: any(_table_name_matches(target, t) for t in extract_tables_from_select(sql))
            )
        ].copy()

    if select_df.empty:
        related_insert_df = pd.DataFrame()
    else:
        matched_jobs = set(select_df["JOB_NAME"].dropna().astype(str).str.strip())
        related_insert_df = all_df[
            all_df["JOB_NAME"].astype(str).str.strip().isin(matched_jobs)
            & all_df["INSERT_STATEMENT"].apply(has_text)
        ].copy()
        related_insert_df["TARGET_TABLE"] = target

    return select_df, related_insert_df


def scan_excel_by_insert_target(input_path: Path, target_table: str):
    """Find jobs that INSERT INTO target table, and fetch related SELECT parts by job."""
    target = normalize_table_name(target_table)
    all_df = _collect_all_rows(input_path)
    if all_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    def _row_matches_insert_target(r: pd.Series) -> bool:
        insert_sql = r.get("INSERT_STATEMENT", "")
        if has_text(insert_sql):
            insert_tables = extract_tables_from_insert(insert_sql)
            if any(_table_name_matches(target, t) for t in insert_tables):
                return True

            parsed_schema, parsed_table = parse_insert_target(insert_sql)
            parsed_target = normalize_table_name(
                f"{parsed_schema}.{parsed_table}" if parsed_schema and parsed_table else parsed_table
            )
            if parsed_target and _table_name_matches(target, parsed_target):
                return True

        schema = normalize_table_name(r.get("JOB ETL TARGET SCHEMA", ""))
        table = normalize_table_name(r.get("JOB ETL TARGET TABLE", ""))
        if table:
            row_target = f"{schema}.{table}" if schema else table
            if _table_name_matches(target, row_target):
                return True

        return False

    insert_df = all_df[all_df["INSERT_STATEMENT"].apply(has_text)].copy()
    if not insert_df.empty:
        insert_df = insert_df[insert_df.apply(_row_matches_insert_target, axis=1)].copy()

    if insert_df.empty:
        related_select_df = pd.DataFrame()
    else:
        matched_jobs = set(insert_df["JOB_NAME"].dropna().astype(str).str.strip())
        related_select_df = all_df[
            all_df["JOB_NAME"].astype(str).str.strip().isin(matched_jobs)
            & all_df["SELECT_STATEMENT"].apply(has_text)
        ].copy()
        related_select_df["TARGET_TABLE"] = target

    return related_select_df, insert_df


def build_detail_output(select_df: pd.DataFrame, related_insert_df: pd.DataFrame) -> pd.DataFrame:
    if select_df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    insert_by_job = {}
    if not related_insert_df.empty:
        for _, r in related_insert_df.iterrows():
            job = str(r.get("JOB_NAME", "")).strip()
            if not job:
                continue
            schema = r.get("JOB ETL TARGET SCHEMA", "")
            table = r.get("JOB ETL TARGET TABLE", "")
            if not table:
                continue
            insert_by_job.setdefault(job, []).append(
                {
                    "JOB ETL TARGET SCHEMA": schema,
                    "JOB ETL TARGET TABLE": table,
                    "INSERT_STATEMENT": r.get("INSERT_STATEMENT", ""),
                }
            )

    rows = []
    for _, s in select_df.iterrows():
        job = str(s.get("JOB_NAME", "")).strip()
        inserts = insert_by_job.get(job, [])
        if not inserts:
            rows.append(
                {
                    "JOB ETL TARGET SCHEMA": "",
                    "JOB ETL TARGET TABLE": "",
                    "PROJECT_NAME": s.get("PROJECT_NAME", ""),
                    "JOB_NAME": s.get("JOB_NAME", ""),
                    "FOLDER_PATH": s.get("FOLDER_PATH", ""),
                    "DATA_CONNECTION": s.get("DATA_CONNECTION", ""),
                    "STAGE_TYPE": s.get("STAGE_TYPE", ""),
                    "STAGE_NAME": s.get("STAGE_NAME", ""),
                    "READ_MODE": s.get("READ_MODE", ""),
                    "WRITE_MODE": s.get("WRITE_MODE", ""),
                    "SELECT_STATEMENT": s.get("SELECT_STATEMENT", ""),
                    "INSERT_STATEMENT": "",
                    "UPDATE_STATEMENT": s.get("UPDATE_STATEMENT", ""),
                }
            )
            continue

        for i in inserts:
            rows.append(
                {
                    "JOB ETL TARGET SCHEMA": i["JOB ETL TARGET SCHEMA"],
                    "JOB ETL TARGET TABLE": i["JOB ETL TARGET TABLE"],
                    "PROJECT_NAME": s.get("PROJECT_NAME", ""),
                    "JOB_NAME": s.get("JOB_NAME", ""),
                    "FOLDER_PATH": s.get("FOLDER_PATH", ""),
                    "DATA_CONNECTION": s.get("DATA_CONNECTION", ""),
                    "STAGE_TYPE": s.get("STAGE_TYPE", ""),
                    "STAGE_NAME": s.get("STAGE_NAME", ""),
                    "READ_MODE": s.get("READ_MODE", ""),
                    "WRITE_MODE": s.get("WRITE_MODE", ""),
                    "SELECT_STATEMENT": s.get("SELECT_STATEMENT", ""),
                    "INSERT_STATEMENT": i.get("INSERT_STATEMENT", ""),
                    "UPDATE_STATEMENT": s.get("UPDATE_STATEMENT", ""),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    return out[OUTPUT_COLUMNS]


def build_detail_output_insert_mode(related_select_df: pd.DataFrame, insert_df: pd.DataFrame) -> pd.DataFrame:
    """Detail output for insert-target mode: base on insert rows and expand with select rows by job."""
    if insert_df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    selects_by_job = {}
    if not related_select_df.empty:
        for _, r in related_select_df.iterrows():
            job = str(r.get("JOB_NAME", "")).strip()
            if not job:
                continue
            selects_by_job.setdefault(job, []).append(
                {
                    "SELECT_STATEMENT": r.get("SELECT_STATEMENT", ""),
                    "UPDATE_STATEMENT": r.get("UPDATE_STATEMENT", ""),
                    "PROJECT_NAME": r.get("PROJECT_NAME", ""),
                    "FOLDER_PATH": r.get("FOLDER_PATH", ""),
                    "DATA_CONNECTION": r.get("DATA_CONNECTION", ""),
                    "STAGE_TYPE": r.get("STAGE_TYPE", ""),
                    "STAGE_NAME": r.get("STAGE_NAME", ""),
                    "READ_MODE": r.get("READ_MODE", ""),
                    "WRITE_MODE": r.get("WRITE_MODE", ""),
                }
            )

    rows = []
    for _, ins in insert_df.iterrows():
        job = str(ins.get("JOB_NAME", "")).strip()
        selects = selects_by_job.get(job, [])
        if not selects:
            rows.append(
                {
                    "JOB ETL TARGET SCHEMA": ins.get("JOB ETL TARGET SCHEMA", ""),
                    "JOB ETL TARGET TABLE": ins.get("JOB ETL TARGET TABLE", ""),
                    "PROJECT_NAME": ins.get("PROJECT_NAME", ""),
                    "JOB_NAME": ins.get("JOB_NAME", ""),
                    "FOLDER_PATH": ins.get("FOLDER_PATH", ""),
                    "DATA_CONNECTION": ins.get("DATA_CONNECTION", ""),
                    "STAGE_TYPE": ins.get("STAGE_TYPE", ""),
                    "STAGE_NAME": ins.get("STAGE_NAME", ""),
                    "READ_MODE": ins.get("READ_MODE", ""),
                    "WRITE_MODE": ins.get("WRITE_MODE", ""),
                    "SELECT_STATEMENT": "",
                    "INSERT_STATEMENT": ins.get("INSERT_STATEMENT", ""),
                    "UPDATE_STATEMENT": ins.get("UPDATE_STATEMENT", ""),
                }
            )
            continue

        for s in selects:
            rows.append(
                {
                    "JOB ETL TARGET SCHEMA": ins.get("JOB ETL TARGET SCHEMA", ""),
                    "JOB ETL TARGET TABLE": ins.get("JOB ETL TARGET TABLE", ""),
                    "PROJECT_NAME": s.get("PROJECT_NAME") or ins.get("PROJECT_NAME", ""),
                    "JOB_NAME": ins.get("JOB_NAME", ""),
                    "FOLDER_PATH": s.get("FOLDER_PATH") or ins.get("FOLDER_PATH", ""),
                    "DATA_CONNECTION": s.get("DATA_CONNECTION") or ins.get("DATA_CONNECTION", ""),
                    "STAGE_TYPE": s.get("STAGE_TYPE") or ins.get("STAGE_TYPE", ""),
                    "STAGE_NAME": s.get("STAGE_NAME") or ins.get("STAGE_NAME", ""),
                    "READ_MODE": s.get("READ_MODE") or ins.get("READ_MODE", ""),
                    "WRITE_MODE": s.get("WRITE_MODE") or ins.get("WRITE_MODE", ""),
                    "SELECT_STATEMENT": s.get("SELECT_STATEMENT", ""),
                    "INSERT_STATEMENT": ins.get("INSERT_STATEMENT", ""),
                    "UPDATE_STATEMENT": s.get("UPDATE_STATEMENT", ""),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    return out[OUTPUT_COLUMNS]


def build_summary(
    select_df: pd.DataFrame,
    related_insert_df: pd.DataFrame,
    detail_output_df: pd.DataFrame,
    target_table: str = "",
    job_name_filter: str = "",
    search_mode: str = "select_source",
) -> pd.DataFrame:
    select_jobs = 0 if select_df.empty else select_df["JOB_NAME"].nunique()
    insert_jobs = 0 if related_insert_df.empty else related_insert_df["JOB_NAME"].nunique()

    rows = [
        {"METRIC": "SEARCH_MODE", "VALUE": search_mode},
        {"METRIC": "TARGET_TABLE", "VALUE": normalize_table_name(target_table) if target_table else ""},
        {"METRIC": "SELECT_ROWS_MATCHED", "VALUE": len(select_df)},
        {"METRIC": "RELATED_INSERT_ROWS", "VALUE": len(related_insert_df)},
        {"METRIC": "MATCHED_JOB_NAME_COUNT", "VALUE": select_jobs},
        {"METRIC": "MATCHED_JOB_WITH_INSERT_COUNT", "VALUE": insert_jobs},
        {"METRIC": "DETAIL_OUTPUT_ROWS", "VALUE": len(detail_output_df)},
    ]
    if job_name_filter:
        rows.insert(1, {"METRIC": "JOB_NAME_FILTER", "VALUE": job_name_filter})
    return pd.DataFrame(rows)


def generate_outputs(input_path, target_table: str, search_mode: str = "select_source"):
    mode = (search_mode or "select_source").strip().lower()

    if mode == "insert_target":
        select_df, related_insert_df = scan_excel_by_insert_target(input_path, target_table)
        detail_output_df = build_detail_output_insert_mode(select_df, related_insert_df)
    else:
        select_df, related_insert_df = scan_excel(input_path, target_table)
        detail_output_df = build_detail_output(select_df, related_insert_df)

    summary_df = build_summary(
        select_df,
        related_insert_df,
        detail_output_df,
        target_table,
        search_mode=mode,
    )
    return summary_df, select_df, related_insert_df, detail_output_df


def _collect_all_rows(input_path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(input_path)
    all_rows = []
    for sheet in xls.sheet_names:
        df = pd.read_excel(input_path, sheet_name=sheet)
        cols = {c.upper(): c for c in df.columns}
        if "JOB_NAME" not in cols:
            continue

        has_stmt_normsql = "STMT_TYPE" in cols and "NORM_SQL" in cols
        has_select_insert = "SELECT_STATEMENT" in cols or "INSERT_STATEMENT" in cols
        if not has_stmt_normsql and not has_select_insert:
            continue

        for idx, row in df.iterrows():
            # Prefer explicit statement columns from sheet; only fallback to NORM_SQL when needed.
            raw_select = row.get(cols.get("SELECT_STATEMENT", ""), "") if "SELECT_STATEMENT" in cols else ""
            raw_insert = row.get(cols.get("INSERT_STATEMENT", ""), "") if "INSERT_STATEMENT" in cols else ""
            raw_update = row.get(cols.get("UPDATE_STATEMENT", ""), "") if "UPDATE_STATEMENT" in cols else ""

            if has_stmt_normsql:
                stmt_type = str(row.get(cols.get("STMT_TYPE", ""), "")).strip().upper()
                norm_sql = row.get(cols.get("NORM_SQL", ""), "")

                select_sql = raw_select if has_text(raw_select) else (norm_sql if stmt_type == "SELECT" else "")
                insert_sql = raw_insert if has_text(raw_insert) else (norm_sql if stmt_type == "INSERT" else "")
                update_sql = raw_update if has_text(raw_update) else (norm_sql if stmt_type == "UPDATE" else "")
                stage_type = row.get(cols.get("STAGE_TYPE", ""), "") or stmt_type
            else:
                select_sql = raw_select
                insert_sql = raw_insert
                update_sql = raw_update
                stage_type = row.get(cols.get("STAGE_TYPE", ""), "")

            target_schema, target_table_name = parse_insert_target(insert_sql)
            all_rows.append(
                {
                    "SHEET_NAME": sheet,
                    "ROW_NO_IN_SHEET": idx + 2,
                    "PROJECT_NAME": row.get(cols.get("PROJECT_NAME", ""), ""),
                    "JOB_NAME": row.get(cols["JOB_NAME"], ""),
                    "FOLDER_PATH": row.get(cols.get("FOLDER_PATH", ""), ""),
                    "DATA_CONNECTION": row.get(cols.get("DATA_CONNECTION", ""), ""),
                    "STAGE_TYPE": stage_type,
                    "STAGE_NAME": row.get(cols.get("STAGE_NAME", ""), ""),
                    "READ_MODE": row.get(cols.get("READ_MODE", ""), ""),
                    "WRITE_MODE": row.get(cols.get("WRITE_MODE", ""), ""),
                    "SELECT_STATEMENT": select_sql,
                    "INSERT_STATEMENT": insert_sql,
                    "UPDATE_STATEMENT": update_sql,
                    "JOB ETL TARGET SCHEMA": target_schema,
                    "JOB ETL TARGET TABLE": target_table_name,
                }
            )
    out = pd.DataFrame(all_rows)
    required_cols = [
        "SHEET_NAME",
        "ROW_NO_IN_SHEET",
        "PROJECT_NAME",
        "JOB_NAME",
        "FOLDER_PATH",
        "DATA_CONNECTION",
        "STAGE_TYPE",
        "STAGE_NAME",
        "READ_MODE",
        "WRITE_MODE",
        "SELECT_STATEMENT",
        "INSERT_STATEMENT",
        "UPDATE_STATEMENT",
        "JOB ETL TARGET SCHEMA",
        "JOB ETL TARGET TABLE",
    ]
    for c in required_cols:
        if c not in out.columns:
            out[c] = ""
    return out[required_cols]


def _filter_df_job_contains(df: pd.DataFrame, job_kw: str) -> pd.DataFrame:
    if df.empty or "JOB_NAME" not in df.columns:
        return df.copy()
    return df[
        df["JOB_NAME"].fillna("").astype(str).str.contains(job_kw, case=False, regex=False)
    ].copy()


def generate_outputs_by_job(input_path, job_name_keyword: str):
    all_df = _collect_all_rows(Path(input_path))
    matched = _filter_df_job_contains(all_df, job_name_keyword)

    # Job-only mode behaves like an Excel filter:
    # return raw matched rows as-is, without select/insert/detail derivation.
    select_df = matched.copy()
    insert_df = pd.DataFrame(columns=matched.columns if not matched.empty else [])
    detail_output_df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    summary_df = build_summary(
        select_df,
        insert_df,
        detail_output_df,
        target_table="",
        job_name_filter=job_name_keyword,
        search_mode="job_name",
    )
    return summary_df, select_df, insert_df, detail_output_df


def apply_job_filter_to_outputs(
    summary_df: pd.DataFrame,
    select_df: pd.DataFrame,
    insert_df: pd.DataFrame,
    detail_output_df: pd.DataFrame,
    job_name_keyword: str,
    target_table: str = "",
):
    filtered_select = _filter_df_job_contains(select_df, job_name_keyword)
    filtered_insert = _filter_df_job_contains(insert_df, job_name_keyword)
    filtered_detail = _filter_df_job_contains(detail_output_df, job_name_keyword)
    filtered_summary = build_summary(
        filtered_select,
        filtered_insert,
        filtered_detail,
        target_table=target_table,
        job_name_filter=job_name_keyword,
        search_mode="filtered",
    )
    return filtered_summary, filtered_select, filtered_insert, filtered_detail


def main():
    parser = argparse.ArgumentParser(
        description="Find job_name that SELECT FROM / INSERT INTO a target table and export to Excel."
    )
    parser.add_argument(
        "--input",
        default="etl_script_20Apr2026.xlsx",
        help="Input Excel file path (default: etl_script_20Apr2026.xlsx)",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Target table in format schema.table (example: EOC.FM_PROFIT_CENTRE)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output Excel file path (default: job_lookup_<schema_table>.xlsx)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    out_path = (
        Path(args.output)
        if args.output
        else Path(f"job_lookup_{normalize_table_name(args.table).replace('.', '_')}.xlsx")
    )

    summary_df, select_df, related_insert_df, detail_output_df = generate_outputs(input_path, args.table)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        if detail_output_df.empty:
            pd.DataFrame([{"INFO": "No result rows for detail output"}]).to_excel(
                writer, sheet_name="detail_output", index=False
            )
        else:
            detail_output_df.to_excel(writer, sheet_name="detail_output", index=False)

        if select_df.empty:
            pd.DataFrame([{"INFO": "No SELECT match found"}]).to_excel(
                writer, sheet_name="select_part", index=False
            )
        else:
            select_df.sort_values(["JOB_NAME", "SHEET_NAME", "ROW_NO_IN_SHEET"]).to_excel(
                writer, sheet_name="select_part", index=False
            )

        if related_insert_df.empty:
            pd.DataFrame([{"INFO": "No related INSERT found for matched jobs"}]).to_excel(
                writer, sheet_name="insert_part", index=False
            )
        else:
            related_insert_df.sort_values(["JOB_NAME", "SHEET_NAME", "ROW_NO_IN_SHEET"]).to_excel(
                writer, sheet_name="insert_part", index=False
            )

    print(f"Done. Output written to: {out_path.resolve()}")


if __name__ == "__main__":
    main()
