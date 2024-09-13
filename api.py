from fastapi import FastAPI, Request
from pydantic import BaseModel
from funcs_for_nodes import start_running, get_data, stop_process, get_data_for_process, fill_list


class RequestInfo(BaseModel):
    task_type: str
    task_args: dict
    task_env: dict


class RequestToken(BaseModel):
    token: int


app = FastAPI()


@app.post("/start")
async def start(tasks: RequestInfo, request: Request) -> tuple[bool, str | None, int | None]:
    success, msg, process_token = await start_running(tasks.dict(), request.client.host)
    return success, msg, process_token


@app.post("/stop")
async def stop(data: RequestToken) -> None:
    await stop_process(data.token)


@app.get("/get")
async def get_info() -> tuple[dict, list]:
    data = await get_data()
    return data


@app.get("/process/{token}")
async def get_info_of_process(token: int) -> tuple[dict | None, str | None]:
    data = await get_data_for_process(token)
    return data


@app.get("/fill")
async def fill_info() -> None:
    fill_list()
