import requests
import json

params = {
    "task_type": "python",
    "task_args": {
        "file": "test.py",
        "time": 7,
        "text": "test_text"
    },
    "task_env": {}}
json_params = json.dumps(params)

try:
    #response = requests.get("http://127.0.0.1:8000/fill")
    #print(response.text)
    response = requests.post("http://127.0.0.1:8000/start", data=json_params)
    print(response.json())
except requests.ConnectionError:
    print("Произошла ошибка")
