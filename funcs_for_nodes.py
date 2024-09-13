import os
import yaml
import datetime
import shutil
import threading
import asyncio
from uuid_extensions import uuid7
from CmdType import get_params
from timetable_for_web import get_counts
from node_runner_general import NodeRunner
from pathlib import Path

dict_of_nodes = {}
list_of_nodes = []


def fill_list() -> None:
    if len(list_of_nodes) != 20:
        path = Path(__file__).parent / "dumps"
        files = sorted([[f, os.path.getmtime(path / str(f))] for f in os.listdir(path)], key=lambda x: x[1])
        for f, t in files[::-1][:20]:
            file_path_1 = path / str(f)
            file_path_2 = path.parent / "logs" / str(f).replace(".yaml", "") / "start_log.yaml"

            with open(file_path_1, "r") as file:
                data_1 = yaml.safe_load(file)

            with open(file_path_2, "r") as file:
                data_2 = yaml.safe_load(file)

            if int(data_2["token_process"]) not in dict_of_nodes:
                list_of_nodes.insert(0, int(data_2["token_process"]))

                dict_of_nodes[data_2["token_process"]] = {"task_type": data_2["task_type"],
                                                          "cmd": data_2["params"]["cmd"],
                                                          "ip_request": data_2["ip_request"],
                                                          "cwd": data_1["process_info"]["data"]["process_info"]["cwd"],
                                                          "root": data_1["process_info"]["data"]["process_info"]["root"],
                                                          "active": data_1["run_info"]["data"]["run_info"]["active"],
                                                          "end_time": data_1["run_info"]["data"]["run_info"]["end_time"],
                                                          "retcode": data_1["run_info"]["data"]["run_info"]["retcode"],
                                                          "object": None}


def start_waiting_process(token_process, output_path, result, ip_request, type_request, task_type) -> None:
    async def end_process() -> None:
        nr = NodeRunner(
            output_path,
            max_runtime=result["max_runtime"],
            terminate_timeout=result["terminate_timeout"],
            encoding=result["encoding"]
        )

        dict_of_nodes[token_process] = {"object": nr,
                                        "task_type": task_type,
                                        "params": result,
                                        "type_request": type_request,
                                        "ip_request": ip_request}

        success, _ = await nr.run(
            cmd=result["cmd"],
            shell=result["shell"],
            env=result["env"],
            cwd=output_path,
            files=result["files"],
            artifacts=result["artifacts"],
            logs=result["logs"]
        )

        write_log(output_path, token_process)
        await dict_of_nodes[token_process]["object"].wait()
        await dict_of_nodes[token_process]["object"].exit()

        dump_path = make_path_for_dump(__file__)
        await write_dump(dump_path, token_process, task_type)

    asyncio.run(end_process())


def check_list_count() -> None:
    max_length = 20
    if len(list_of_nodes) >= max_length:
        del dict_of_nodes[list_of_nodes.pop()]


def get_token() -> int:
    token = uuid7()
    return token.int


def make_path_for_log(file_path: Path | str, token: str) -> Path | str:
    output_path = Path(file_path).parent / "logs" / token
    if not output_path.exists():
        os.makedirs(output_path)
    shutil.copy2(
        Path(__file__).parent / "test.py",
        output_path / "test.py"
    )
    return output_path


def make_path_for_dump(file_path: Path | str) -> Path | str:
    output_path = Path(file_path).parent / "dumps"
    if not output_path.exists():
        os.makedirs(output_path)
    return output_path


def get_type_request(ip: str) -> bool:
    if ip == "127.0.0.1":
        return True
    else:
        return False


def write_log(path: Path | str, token: int) -> None:
    file_path = path / "start_log.yaml"
    info = {
        "token_process": token,
        "start_time": str(datetime.datetime.today()),
        "ip_request": dict_of_nodes[token]["ip_request"],
        "type_request": dict_of_nodes[token]["type_request"],
        "params": dict_of_nodes[token]["params"],
        "task_type": dict_of_nodes[token]["task_type"]
    }
    with open(file_path, "w") as file:
        yaml.safe_dump(info, file)


async def write_dump(path: Path | str, token: int, task_type: str) -> None:
    file_path = path / f"{token}.yaml"
    success_process, data_process = await dict_of_nodes[token]["object"].process_info(False)
    success_run, data_run = await dict_of_nodes[token]["object"].run_info(False)
    info = {
        "token_process": token,
        "end_time": str(datetime.datetime.today()),
        "ip_request": dict_of_nodes[token]["ip_request"],
        "task_type": task_type,
        "text": "Process completed",
        "process_info": data_process,
        "run_info": data_run
    }
    with open(file_path, "w") as file:
        yaml.safe_dump(info, file)


async def check_active_process(type_request: bool) -> tuple[bool, str | None]:
    iactive_count = 0
    eactive_count = 0
    counts = get_counts()

    for element in dict_of_nodes:
        if dict_of_nodes[element]["object"]:
            success, d = await dict_of_nodes[element]["object"].run_info(False)
            active_bool = d["data"]["run_info"]["active"]

            if active_bool:
                if dict_of_nodes[element]["type_request"]:
                    iactive_count += 1
                else:
                    eactive_count += 1

    if iactive_count + eactive_count < counts["all_count"]:
        if type_request:
            if iactive_count < counts["i_count"]:
                return True, None
            else:
                err_msg = "Превышен лимит внутренних запросов"
        else:
            if eactive_count < counts["e_count"]:
                return True, None
            else:
                err_msg = "Превышен лимит внешних запросов"
    else:
        err_msg = "Превышен общий лимит запросов"

    return False, err_msg


async def start_running(params: dict, ip_request: str) -> tuple[bool, str | None, int | None]:
    task_type = params["task_type"]
    task_args = params["task_args"]
    task_env = params["task_env"]

    type_request = get_type_request(ip_request)
    check, err_msg = await check_active_process(type_request)

    if check:
        token_process = get_token()
        output_path = make_path_for_log(__file__, str(token_process))
        check_2, err_msg, result = get_params(task_type, task_args, task_env)

        if check_2:
            check_list_count()
            list_of_nodes.insert(0, token_process)

            daemon_thread = threading.Thread(target=start_waiting_process, kwargs={"token_process": token_process,
                                                                                   "output_path": output_path,
                                                                                   "result": result,
                                                                                   "ip_request": ip_request,
                                                                                   "type_request": type_request,
                                                                                   "task_type": task_type})
            daemon_thread.start()

            return True, None, token_process

    return False, err_msg, None


async def get_data() -> tuple[dict, list]:
    d = {}
    l = []
    for key in list_of_nodes:
        l.append(key)
        if dict_of_nodes[key]["object"]:
            success_process, data_process = await dict_of_nodes[key]["object"].process_info(False)
            success_run, data_run = await dict_of_nodes[key]["object"].run_info(False)
            d[key] = {
                "task_type": dict_of_nodes[key]["task_type"],
                "cmd": dict_of_nodes[key]["params"]["cmd"],
                "ip_request": dict_of_nodes[key]["ip_request"],
                "cwd": data_process["data"]["process_info"]["cwd"],
                "root": data_process["data"]["process_info"]["root"],
                "active": data_run["data"]["run_info"]["active"],
                "end_time": data_run["data"]["run_info"]["end_time"],
                "retcode": data_run["data"]["run_info"]["retcode"]
            }
        else:
            d[key] = dict_of_nodes[key]
    return d, l


async def stop_process(token: int) -> None:
    await dict_of_nodes[token]["object"].stop()


async def get_data_for_process(token: int) -> tuple[dict | None, str | None]:
    dump_path = make_path_for_dump(__file__)
    path = dump_path / f"{token}.yaml"

    if token in dict_of_nodes:
        success_process, data_process = await dict_of_nodes[token]["object"].process_info(False)
        success_run, data_run = await dict_of_nodes[token]["object"].run_info(False)
        data = {
            "token_process": token,
            "end_time": str(datetime.datetime.today()),
            "ip_request": dict_of_nodes[token]["ip_request"],
            "text": "Process is running",
            "process_info": data_process,
            "run_info": data_run
        }
        return data, None
    elif path.exists():
        with open(path, "r") as file:
            data = yaml.safe_load(file)
        return data, None
    else:
        err_message = "Такого процесса нет"
        return None, err_message
