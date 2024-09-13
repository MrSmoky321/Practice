import jinja2
import yaml
import re


def make_cmd(args: dict, template: list[str], task_type: str) -> tuple[bool, str | None, list[str] | None]:
    cmd = [task_type]
    for element in template:
        tmp = jinja2.Template(element)
        text = tmp.render(args=args)
        cmd.append(text)
    return True, None, cmd


def get_params(task_type: str, task_args: dict, task_env: dict) -> tuple[bool, str | None, dict | None]:
    with open("files_yaml/commands.yaml", "r") as file:
        info = yaml.safe_load(file)

    if task_type in info:
        for key in task_args:
            if key in info[task_type]["possible_args"]:
                if not re.match(info[task_type]["possible_args"][key], str(task_args[key])):
                    err_msg = "Неправильно заданы параметры cmd"
                    return False, err_msg, None
            else:
                err_msg = "Такого аргумента для cmd нет"
                return False, err_msg, None

        for key in task_env:
            if key in info[task_type]["possible_env"]:
                if not re.match(info[task_type]["possible_env"][key], task_env[key]):
                    err_msg = "Неправильно заданы параметры env"
                    return False, err_msg, None
            else:
                err_msg = "Такого аргумента для env нет"
                return False, err_msg, None

        final_args = {**info[task_type]["args"], **task_args}
        final_env = {**info[task_type]["env"], **task_env}

        success, err_msg, result = make_cmd(final_args, info[task_type]["template"], task_type)

        if success:
            params = {
                "max_runtime": info[task_type]["max_runtime"],
                "terminate_timeout": info[task_type]["terminate_timeout"],
                "encoding": info[task_type]["encoding"],
                "cmd": result,
                "shell": info[task_type]["shell"],
                "env": final_env,
                "files": info[task_type]["files"],
                "artifacts": info[task_type]["artifacts"],
                "logs": info[task_type]["logs"],
            }
            return True, None, params
        else:
            return False, err_msg, None
    else:
        err_msg = "Нет такой команды"
        return False, err_msg, None
