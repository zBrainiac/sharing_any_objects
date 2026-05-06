from pathlib import Path

import streamlit as st
import snowflake.connector

DATABASE = "ECO_DEV"
SCHEMA = "ECOS_RAW_V001"
VIEW = f"{DATABASE}.{SCHEMA}.ECOS_RAW_VW_STAGE_FILES_DOWNLOAD_MT"
STAGE = f"{DATABASE}.{SCHEMA}.ECOS_RAW_ST_DOC_MT"
OUTPUT_DIR = Path(__file__).parent.parent / "downloads"

ROLE_CONNECTION_MAP = {
    "BANK1_ROLE": "bank1",
    "BANK2_ROLE": "bank2",
}


@st.cache_resource
def get_connection(connection_name: str):
    return snowflake.connector.connect(connection_name=connection_name)


def get_files(conn) -> list[dict]:
    cursor = conn.cursor()
    cursor.execute(f"SELECT TENANT_ID, BUSINESS_UNIT, FILE_TYPE, FILE_NAME, FILE_PATH, FILE_SIZE FROM {VIEW}")
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return rows


def download_file(conn, file: dict) -> bool:
    out_dir = OUTPUT_DIR / file["TENANT_ID"]
    out_dir.mkdir(parents=True, exist_ok=True)
    cursor = conn.cursor()
    try:
        file_path = file["FILE_PATH"]
        cursor.execute(f"GET '@{STAGE}/{file_path}' 'file://{out_dir}/'")
        return True
    except Exception as e:
        st.error(f"Error downloading {file['FILE_NAME']}: {e}")
        return False
    finally:
        cursor.close()


def reset_selection(files: list[dict]) -> None:
    st.session_state.select_all = False
    st.session_state.checked = {f["FILE_PATH"]: False for f in files}
    for f in files:
        st.session_state[f"cb_{f['FILE_PATH']}"] = False


def on_select_all(files: list[dict]) -> None:
    val = st.session_state.select_all
    for f in files:
        st.session_state.checked[f["FILE_PATH"]] = val
        st.session_state[f"cb_{f['FILE_PATH']}"] = val


st.title("File Download Portal")

col_role, col_bu, col_type = st.columns(3)
role = col_role.selectbox("Select Role", list(ROLE_CONNECTION_MAP.keys()), key="selected_role")
conn = get_connection(ROLE_CONNECTION_MAP[role])
files = get_files(conn)

business_units = sorted({f.get("BUSINESS_UNIT", "") for f in files if f.get("BUSINESS_UNIT")})
file_types = sorted({f.get("FILE_TYPE", "") for f in files if f.get("FILE_TYPE")})

selected_bu = col_bu.selectbox("Business Unit", ["All"] + business_units, key="filter_bu")
selected_type = col_type.selectbox("File Type", ["All"] + file_types, key="filter_type")

if selected_bu != "All":
    files = [f for f in files if f.get("BUSINESS_UNIT") == selected_bu]
if selected_type != "All":
    files = [f for f in files if f.get("FILE_TYPE") == selected_type]

st.divider()

if not files:
    st.info("No files available for this role.")
else:
    if st.session_state.get("prev_role") != role:
        reset_selection(files)
        st.session_state.prev_role = role

    st.checkbox(
        "Download all",
        key="select_all",
        on_change=on_select_all,
        kwargs={"files": files},
    )

    with st.container(border=True):
        c1, c2, c3, c4, c5 = st.columns([1, 4, 2, 2, 2])
        c1.caption("Select")
        c2.caption("File name")
        c3.caption("Business Unit")
        c4.caption("Type")
        c5.caption("Size")
        for f in files:
            c1, c2, c3, c4, c5 = st.columns([1, 4, 2, 2, 2])
            checked = c1.checkbox(
                "select",
                key=f"cb_{f['FILE_PATH']}",
                label_visibility="collapsed",
            )
            c2.write(f["FILE_NAME"])
            c3.write(f.get("BUSINESS_UNIT", "---"))
            c4.write(f.get("FILE_TYPE", "---"))
            c5.write(f"{f['FILE_SIZE'] / 1024:.1f} KB" if f["FILE_SIZE"] else "---")
            st.session_state.checked[f["FILE_PATH"]] = checked

    selected = [f for f in files if st.session_state.checked.get(f["FILE_PATH"])]

    if st.button("Download", type="primary", disabled=not selected):
        progress = st.progress(0, text="Starting download...")
        results = []
        for i, f in enumerate(selected):
            progress.progress((i + 1) / len(selected), text=f"Downloading {f['FILE_NAME']}...")
            results.append((f["FILE_NAME"], download_file(conn, f)))
        progress.empty()

        succeeded = [name for name, ok in results if ok]
        failed = [name for name, ok in results if not ok]
        if succeeded:
            st.success(f"Downloaded {len(succeeded)} file(s) to `{OUTPUT_DIR}`")
        if failed:
            st.error(f"Failed: {', '.join(failed)}")
