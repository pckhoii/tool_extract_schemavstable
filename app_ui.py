from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

from find_jobs_by_table import generate_outputs


def to_excel_bytes(summary_df, select_df, insert_df, detail_df) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)

        if select_df.empty:
            pd.DataFrame([{"INFO": "No SELECT match found"}]).to_excel(
                writer, sheet_name="select_part", index=False
            )
        else:
            select_df.sort_values(["JOB_NAME", "SHEET_NAME", "ROW_NO_IN_SHEET"]).to_excel(
                writer, sheet_name="select_part", index=False
            )

        if insert_df.empty:
            pd.DataFrame([{"INFO": "No related INSERT found for matched jobs"}]).to_excel(
                writer, sheet_name="insert_part", index=False
            )
        else:
            insert_df.sort_values(["JOB_NAME", "SHEET_NAME", "ROW_NO_IN_SHEET"]).to_excel(
                writer, sheet_name="insert_part", index=False
            )

        if detail_df.empty:
            pd.DataFrame([{"INFO": "No result rows for detail output"}]).to_excel(
                writer, sheet_name="detail_output", index=False
            )
        else:
            detail_df.to_excel(writer, sheet_name="detail_output", index=False)
    return buf.getvalue()


def main():
    st.set_page_config(page_title="ETL Job Lookup", layout="wide")
    st.title("ETL Job Lookup Tool")

    st.write("Nhap schema + table de tim SELECT job, INSERT job va detail_output.")

    default_input = "etl_script_20Apr2026.xlsx"
    input_path = st.text_input("Excel file path", value=default_input)

    c1, c2 = st.columns(2)
    with c1:
        schema = st.text_input("Schema", value="EOC")
    with c2:
        table = st.text_input("Table", value="FM_DEPARTMENT")

    run_btn = st.button("Run")
    if not run_btn:
        return

    path = Path(input_path)
    if not path.exists():
        st.error(f"Khong tim thay file: {path}")
        return

    target_table = f"{schema.strip()}.{table.strip()}"
    if not schema.strip() or not table.strip():
        st.error("Vui long nhap day du schema va table.")
        return

    summary_df, select_df, insert_df, detail_df = generate_outputs(path, target_table)

    st.subheader("1) Job phan SELECT")
    st.dataframe(select_df, use_container_width=True, hide_index=True)

    st.subheader("2) Job phan INSERT")
    st.dataframe(insert_df, use_container_width=True, hide_index=True)

    st.subheader("3) Detail Output")
    st.dataframe(detail_df, use_container_width=True, hide_index=True)

    st.caption(
        f"Summary: SELECT rows={len(select_df)}, INSERT rows={len(insert_df)}, DETAIL rows={len(detail_df)}"
    )

    excel_bytes = to_excel_bytes(summary_df, select_df, insert_df, detail_df)
    file_name = f"job_lookup_{schema.strip().upper()}_{table.strip().upper()}.xlsx"
    st.download_button(
        label="Download Output Excel",
        data=excel_bytes,
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    main()

