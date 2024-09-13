import streamlit as st
import pandas as pd
from timetable_for_web import get_data_for_web, load_data_from_web


def start_timetable_page() -> None:
    st.title("Расписание процессов")
    df = pd.DataFrame(get_data_for_web(), columns=["day", "start_period", "all_count", "i_count", "e_count"])
    if "df" not in st.session_state:
        st.session_state["df"] = df
    st.data_editor(st.session_state["df"], hide_index=True, key="df_editor", on_change=df_on_change, args=[df])


def df_on_change(df) -> None:
    state = st.session_state["df_editor"]
    for index, updates in state["edited_rows"].items():
        for key, value in updates.items():
            load_data_from_web(index, key, value)


if __name__ == "__main__":
    start_timetable_page()
