import os
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

TYPE_OPTIONS = [
    "Normal",
    "Voiture",
    "FraisPro",
    "Immobilier",
    "Depenses",
    "Life",
    "Economies",
    "Anormal",
    "Cash",
    "Salaires",
]


def resolve_sqlite_path() -> Path:
    env_path = os.getenv("COMPTE_B_SQLITE_PATH")
    if env_path:
        path = Path(env_path).expanduser()
        return path if path.is_absolute() else (Path.cwd() / path).resolve()
    return (Path.cwd() / "data" / "partage.sqlite").resolve()


@st.cache_data(ttl=10)
def load_data(db_path: str, limit: int) -> pd.DataFrame:
    query = (
        "SELECT rowid AS _rowid, * "
        "FROM stg_operations_with_type "
        "ORDER BY date DESC, rowid DESC "
        f"LIMIT {int(limit)}"
    )
    with sqlite3.connect(db_path) as connection:
        return pd.read_sql_query(query, connection)


st.set_page_config(page_title="Partage SQLite Viewer", layout="wide")
st.title("Partage SQLite - stg_operations_with_type")

sqlite_path = resolve_sqlite_path()
st.caption(f"SQLite file: {sqlite_path}")

if not sqlite_path.exists():
    st.error(f"SQLite file not found: {sqlite_path}")
    st.stop()

limit = st.sidebar.slider(
    "Rows to display",
    min_value=10,
    max_value=2000,
    value=200,
    step=10,
)

try:
    dataframe = load_data(str(sqlite_path), limit)
except Exception as exc:
    st.exception(exc)
    st.stop()

col_left, col_right = st.columns(2)
with col_left:
    st.metric("Rows shown", len(dataframe))
with col_right:
    st.metric("Columns", len(dataframe.columns))

edited_dataframe = st.data_editor(
    dataframe,
    use_container_width=True,
    hide_index=True,
    disabled=["_rowid", "date", "libelle", "montant_euros"],
    column_config={
        "_rowid": st.column_config.NumberColumn("_rowid", disabled=True),
        "Type": st.column_config.SelectboxColumn("Type", options=TYPE_OPTIONS, required=True),
    },
)

if st.button("Enregistrer les modifications", type="primary"):
    original = dataframe.set_index("_rowid")
    edited = edited_dataframe.set_index("_rowid")
    changed_mask = original["Type"] != edited["Type"]
    changed_rows = edited.loc[changed_mask, ["Type"]].reset_index()

    if changed_rows.empty:
        st.info("Aucune modification detectee.")
    else:
        with sqlite3.connect(sqlite_path) as connection:
            connection.executemany(
                'UPDATE "stg_operations_with_type" SET "Type" = ? WHERE rowid = ?',
                [(row["Type"], int(row["_rowid"])) for _, row in changed_rows.iterrows()],
            )
            connection.commit()

        st.success(f"{len(changed_rows)} ligne(s) mise(s) a jour.")
        st.cache_data.clear()
        st.rerun()

csv_data = edited_dataframe.drop(columns=["_rowid"]).to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download displayed rows as CSV",
    data=csv_data,
    file_name="stg_operations_with_type_preview.csv",
    mime="text/csv",
)
