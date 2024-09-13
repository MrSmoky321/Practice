import datetime
import time
import argparse


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--time", default=5)
    parser.add_argument("--text", default="Hello, World")
    return parser


def start():
    start_time = datetime.datetime.today()
    print(f"{start_time.hour}:{start_time.minute}:{start_time.second}")

    parser = create_parser()
    parser_args = parser.parse_args()

    try:
        time.sleep(int(parser_args.time))
        print(parser_args.text)
    except ValueError:
        print("Неверно введено значение")

    end_time = datetime.datetime.today()
    print(f"{end_time.hour}:{end_time.minute}:{end_time.second}")


if __name__ == "__main__":
    print("Начало")
    start()
    print("Конец")