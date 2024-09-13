import streamlit as st
import pandas as pd
import requests
import json
import time


def dict_to_list(d: dict, l: list, active: bool) -> list:
    list_for_df = []
    for k in l:
        key = str(k)
        if active:
            list_for_df.append([key,
                                d[key]["task_type"],
                                d[key]["ip_request"],
                                "Active" if d[key]["active"] else "Not Active",
                                time.ctime(d[key]["end_time"]),
                                d[key]["root"],
                                d[key]["cmd"],
                                d[key]["cwd"],
                                d[key]["retcode"]
                                ])
        elif d[key]["active"]:
            list_for_df.append([key,
                                d[key]["task_type"],
                                d[key]["ip_request"],
                                "Active" if d[key]["active"] else "Not Active",
                                time.ctime(d[key]["end_time"]),
                                d[key]["root"],
                                d[key]["cmd"],
                                d[key]["cwd"],
                                d[key]["retcode"]
                                ])
    return list_for_df


def start_list_page() -> None:
    st.title("Список процессов")

    try:
        response = requests.get("http://127.0.0.1:8000/get")
        dict_nodes, list_nodes = response.json()

        if dict_nodes == {}:
            st.write("Нет запущенных процессов")
        else:
            col_1, col_2 = st.columns([9, 2])
            with col_2:
                dop_info = st.checkbox("More information")

            if dop_info:
                list_for_df = dict_to_list(dict_nodes, list_nodes, True)
            else:
                list_for_df = dict_to_list(dict_nodes, list_nodes, False)

            df = pd.DataFrame(list_for_df, columns=["Token",
                                                    "task_type",
                                                    "ip_server",
                                                    "Active",
                                                    "end_time",
                                                    "root",
                                                    "cmd",
                                                    "cwd",
                                                    "retcode"])
            df.rename(index=lambda x: x + 1, inplace=True)
            st.dataframe(df)

            st.sidebar.title("Информация")
            show_data = st.sidebar.text_input("Введите номер процесса, который вы хотитие изменить")

            if show_data:
                try:
                    start_sidebar(int(show_data), len(list_for_df), list_for_df)
                except ValueError:
                    st.sidebar.write("Неправильно введено значение")
            else:
                st.sidebar.write("Нет значения")
    except requests.ConnectionError:
        st.write("Сервер с процессами не запущен")


def start_sidebar(value: int, count: int, list_for_df: list) -> None:
    if 0 < value <= count:
        if list_for_df[value - 1][3] == 'Active':
            col_1, col_2, col_3 = st.sidebar.columns([2, 4, 1])

            with col_2:
                button_stop = st.button("Завершить процесс")

            if button_stop:
                data = json.dumps({"token": list_for_df[value - 1][0]})
                response = requests.post("http://127.0.0.1:8000/stop", data=data)
                if response:
                    st.sidebar.write("Процесс завершается")

        st.sidebar.write(f"Токен процесса: {list_for_df[value - 1][0]}")
        st.sidebar.write(f"Тип задачи: {list_for_df[value - 1][1]}")
        st.sidebar.write(f"IP: {list_for_df[value - 1][2]}")
        st.sidebar.write(f"Выполняется: {'Да' if list_for_df[value - 1][3] == 'Active' else 'Нет'}")
        st.sidebar.write(f"Время завершения процесса: {list_for_df[value - 1][4]}")
        st.sidebar.write(f"Путь: {list_for_df[value - 1][5]}")
        st.sidebar.write(f"Команда: {list_for_df[value - 1][6]}")
        st.sidebar.write(f"Рабочая папка: {list_for_df[value - 1][7]}")
        st.sidebar.write(f"Код завершения: {list_for_df[value - 1][8]}")
    else:
        st.sidebar.write("Данного значения нет в таблице")


if __name__ == "__main__":
    requests.get("http://127.0.0.1:8000/fill")
    start_list_page()
