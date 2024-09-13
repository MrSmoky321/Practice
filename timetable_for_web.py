import datetime
import yaml


def get_data_for_web() -> list:
    with open("files_yaml/setting_time.yaml", "r") as file:
        data = yaml.safe_load(file)

    list_for_timetable = []
    day_of_weeks = {
        0: "Пн",
        1: "Вт",
        2: "Ср",
        3: "Чт",
        4: "Пт",
        5: "Сб",
        6: "Вс"
    }

    for i in range(7):
        list_for_timetable.append([
            day_of_weeks[i],
            data[i]["start_time"]["value"],
            data[i]["start_time"]["all_count"],
            data[i]["start_time"]["i_count"],
            data[i]["start_time"]["e_count"]
        ])

        list_for_timetable.append([
            day_of_weeks[i],
            data[i]["end_time"]["value"],
            data[i]["end_time"]["all_count"],
            data[i]["end_time"]["i_count"],
            data[i]["end_time"]["e_count"]
        ])

    return list_for_timetable


def load_data_from_web(index: int, key: str, value: int | str) -> None:
    if key != "day":
        with open("files_yaml/setting_time.yaml", "r") as file:
            data = yaml.safe_load(file)

        if index % 2 == 0:
            time = "start_time"
        else:
            time = "end_time"

        index = int(index // 2)

        if key == "start_period":
            key = "value"
            value = str(value)

        data[index][time][key] = value

        with open("files_yaml/setting_time.yaml", "w") as file:
            yaml.safe_dump(data, file)


def get_settings(day: int) -> dict:
    with open("files_yaml/setting_time.yaml", "r") as file:
        text = yaml.safe_load(file)

    start_params = text[day]["start_time"]
    end_params = text[day]["end_time"]
    end_params_yesterday = text[day - 1 if day > 0 else 6]["end_time"]

    start_time = start_params["value"].split(":")
    end_time = end_params["value"].split(":")

    start_value = 0
    end_value = 0

    for i in range(2):
        if start_time[i][0] == "0":
            start_value += int(start_time[i][1]) * (60 ** (2 - i))
        else:
            start_value += int(start_time[i]) * (60 ** (2 - i))

        if end_time[i][0] == "0":
            end_value += int(end_time[i][1]) * (60 ** (2 - i))
        else:
            end_value += int(end_time[i]) * (60 ** (2 - i))

    start_params["value"] = start_value
    end_params["value"] = end_value

    return {"start_params": start_params,
            "end_params": end_params,
            "end_params_yesterday": end_params_yesterday}


def get_counts() -> dict:
    cur_time = datetime.datetime.today()
    day = cur_time.weekday()
    cur_time_seconds = cur_time.hour * 3600 + cur_time.minute * 60 + cur_time.second

    settings = get_settings(day)
    if settings["start_params"]["value"] <= cur_time_seconds < settings["end_params"]["value"]:
        return settings["start_params"]
    elif cur_time_seconds >= settings["end_params"]["value"]:
        return settings["end_params"]
    elif settings["start_params"]["value"] > cur_time_seconds:
        return settings["end_params_yesterday"]
