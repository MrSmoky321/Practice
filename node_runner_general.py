import os
import asyncio
import asyncio.subprocess
import functools
import time
import datetime
from threading import Lock
from pathlib import Path
from base64 import b64decode
from helpers import *
import yaml
import shutil


# https://stackoverflow.com/questions/1191374/using-module-subprocess-with-timeout/4825933#4825933
# https://gist.github.com/kirpit/1306188/ab800151c9128db3b763bb9f9ec19fda0df3a843
# https://blog.dalibo.com/2022/09/12/monitoring-python-subprocesses.html


def success(**kwargs):
    result = {}
    if len(kwargs) is not None:
        result['data'] = to_dict(**kwargs)
    return True, result


def fail(message):
    return False, to_dict(err_msg=message)


def successful(retval: tuple[bool, dict]):
    return retval[0]


def get_result(retval: tuple[bool, dict], **fields):
    if successful(retval):
        values = retval[1]['data']
        result = [values[k] for k in fields.keys()]
    else:
        result = [v for v in fields.values()]
    if len(result) == 1:
        return result[0]
    return result


class StreamDispatcher(asyncio.subprocess.SubprocessStreamProtocol):
    def __init__(self, targets, limit: int, loop):
        super().__init__(limit=limit, loop=loop)
        self._targets = targets

    def pipe_data_received(self, fd, data):
        """Called when the child process writes data into its stdout
        or stderr pipe.
        """
        super().pipe_data_received(fd, data)
        if self._targets is not None:
            if fd in self._targets:
                target = self._targets[fd]
                kwargs = {}
            elif "*" in self._targets:
                target = self._targets["*"]
                kwargs = to_dict(source=fd)
            target.feed_data(data, **kwargs)

    def pipe_connection_lost(self, fd, exc):
        """Called when one of the pipes communicating with the child
        process is closed.
        """
        super().pipe_connection_lost(fd, exc)
        if self._targets is not None:
            if fd in self._targets:
                target = self._targets[fd]
                if exc:
                    target.set_exception(exc)
                else:
                    target.feed_eof()

            if "*" in self._targets:
                target = self._targets["*"]
                if exc:
                    target.set_exception(exc)
                else:
                    target.feed_eof()


class StreamReaderHighjack:

    def __init__(self, on_exception, on_eof):
        self._on_exception = on_exception
        self._on_eof = on_eof

    def feed_data(self, data, **kwargs):
        pass

    def set_exception(self, exc):
        self._on_exception(exc)

    def feed_eof(self, ):
        self._on_eof()


class ProcessInfo(Dictable):

    def __init__(
            self,
            root: Path = None,
            cmd: list = None,
            shell: bool = None,
            env: dict = None,
            cwd: Path = None,
            files: list = None,
            artifacts: list = None,
            logs: list = None,  # NOTE: use logs to gather extra artifacts on exit
    ):
        self.root = root
        self.cmd = cmd
        self.shell = shell
        self.env = env
        self.cwd = cwd
        self.files = files
        self.artifacts = artifacts
        self.logs = logs


class RunInfo(Dictable):

    def __init__(
            self,
            start_time: float = None,
            end_time: float = None,
            cpu_time: float = None,
            active: bool = None,
            stopping: bool = None,
            pid: int = None,
            retcode: int = None,
            exception=None,
    ):
        self._start_time = start_time
        self._end_time = end_time
        self._cpu_time = cpu_time
        self._active = active
        self._stopping = stopping
        self._pid = pid
        self._retcode = retcode
        self._exception = exception

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, value):
        if self._start_time is not None:
            raise ValueError("Overriding start_time is not allowed!")
        self._start_time = value

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, value):
        if self._end_time is not None:
            raise ValueError("Overriding end_time is not allowed!")
        self._end_time = value

    @property
    def cpu_time(self):
        return self._cpu_time

    @cpu_time.setter
    def cpu_time(self, value):
        if self._cpu_time is not None:
            raise ValueError("Overriding cpu_time is not allowed!")
        self._cpu_time = value

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value):
        if not isinstance(value, bool):
            print(f"[INFO] 'active' for process with pid is set to '{value}'")
        if self._active != True and value is True:
            print(f"[INFO] Process with pid {self.pid} become active")
        if self._active != False and value is False:
            print(f"[INFO] Process with pid {self.pid} become inactive")
        self._active = value

    @property
    def stopping(self):
        return self._stopping

    @stopping.setter
    def stopping(self, value):
        if self._stopping is not True and value is True:
            print(f"[INFO] Stop of process with pid {self.pid} is started")
        if self._stopping is True and value is False:
            print(f"[INFO] Stop of process with pid {self.pid} is ended")
        self._stopping = value

    @property
    def pid(self):
        return self._pid

    @pid.setter
    def pid(self, value):
        if self._pid is not None:
            raise ValueError("Overriding pid is not allowed!")
        self._pid = value

    @property
    def retcode(self):
        return self._retcode

    @retcode.setter
    def retcode(self, value):
        if self._retcode is not None:
            raise ValueError("Overriding retcode is not allowed!")
        self._retcode = value

    @property
    def exception(self):
        return self._exception

    @exception.setter
    def exception(self, value):
        if self._exception is not None:
            raise ValueError("Overriding exception is not allowed!")
        self._exception = value


class UploadState(Dictable):

    def __init__(
            self,
            source,
            target,
            missing,
    ):
        self.source = source
        self.target = target
        self.missing = missing
        self.uploading = None
        self.progress = 0
        self.size = 0


class NodeRunner:

    def __init__(self, root, max_runtime: float = None, terminate_timeout: float = 30.0, encoding=None):
        # Provide necessary base for multi-threaded run
        # (noderunner runs subprocess in separate thread)
        self._max_runtime = max_runtime
        self._terminate_timeout = terminate_timeout or 30.0
        self._encoding = encoding
        self._process_info = ProcessInfo(Path(root).absolute().resolve(), None, None, None, None, None, None, None)
        self._run_info = RunInfo(None, None, None, None)
        self._subprocess = None

        self._stdout_coroutine = None
        self._stderr_coroutine = None
        self._uploader_coroutine = None
        self._supervisor_coroutine = None

        self._stdin = []
        self._stdout = []
        self._stderr = []
        self._stdout_file = None
        self._stderr_file = None

        self._upload_state = {}
        self._uploaders = []
        self._thread_lock = Lock()
        self._wait_lock = Lock()
        self._waiting = False

    @staticmethod
    async def _stream_capture(reader, target, target_file):
        async for line in reader:
            if target is not None:
                target.append((time.time(), line))
            if target_file is not None:
                target_file.write(line)

    def _on_proc_closing(self):
        if self._run_info.end_time is None:
            self._run_info.end_time = time.time()
        self._run_info.active = False

    def _on_proc_exception(self, exc):
        if self._run_info.end_time is None:
            self._run_info.end_time = time.time()
        self._run_info.active = False
        self._run_info.exception = exc

    async def _supervise(self):
        if self._max_runtime is not None:
            while self._run_info.active is True:
                await asyncio.sleep(1.)
                if time.time() - self._run_info.start_time > self._max_runtime:
                    await self.ensure_stopped()
        await self.wait()

    async def _subprocess_run(self):
        # Starts subprocess
        # NOTE: that section should be overriden depending on usage
        # code below is for local process runner
        self._run_info.start_time = time.time()
        loop = asyncio.get_event_loop()

        # TODO: get_event_loop deprecated? use get_running_loop?
        # https://stackoverflow.com/questions/44630676/how-can-i-call-an-async-function-without-await

        stdout_reader = asyncio.StreamReader(loop=loop)
        stderr_reader = asyncio.StreamReader(loop=loop)

        close_monitor = StreamReaderHighjack(
            on_eof=self._on_proc_closing,
            on_exception=self._on_proc_exception
        )

        protocol_factory = functools.partial(
            StreamDispatcher,
            targets={
                1: stdout_reader,
                2: stderr_reader,
                "*": close_monitor
            },
            limit=2 ** 16,
            loop=loop,
        )

        self._stdout_file = open(self._process_info.root / ".stdout", "wb")
        self._stderr_file = open(self._process_info.root / ".stderr", "wb")

        self._stdout_coroutine = loop.create_task(self._stream_capture(stdout_reader, self._stdout, self._stdout_file))
        self._stderr_coroutine = loop.create_task(self._stream_capture(stderr_reader, self._stderr, self._stderr_file))

        transport, protocol = await loop.subprocess_exec(
            protocol_factory,
            *self._process_info.cmd,
            shell=self._process_info.shell,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self._process_info.cwd,
            env=self._process_info.env,
        )
        self._subprocess = asyncio.subprocess.Process(transport, protocol, loop)
        self._run_info.pid = self._subprocess.pid
        self._run_info.active = True
        self._supervisor_coroutine = loop.create_task(self._supervise())

    async def run(
            self,
            cmd: list,
            shell: bool,
            env: dict,
            cwd: str,
            files: dict,
            artifacts: list,
            logs: list,  # NOTE: use logs to gather extra artifacts on exit
    ) -> tuple[bool, dict]:
        """
        Run process
        :rtype: object
        """
        if self._run_info.active is not None:
            raise ValueError("Process is running already!")
        self._run_info.active = "__preparing__"

        # Make cwd
        cwd = cwd or "."
        cwd_abs = (self._process_info.root / cwd).resolve()
        if cwd_abs.is_relative_to(self._process_info.root) is False:
            raise ValueError(f"cwd '{cwd}' sets working directory outside root, which is not allowed!")
        cwd_abs.mkdir(parents=True, exist_ok=True)

        # Populate files
        files_abs = []
        for k, v in files.items():
            k_abs = (self._process_info.root / str(k)).resolve()
            if k_abs.is_relative_to(self._process_info.root) is False:
                raise ValueError(f"file '{k}' points outside root, which is not allowed!")
            k_abs.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(v, str):
                v = b64decode(v)
            with open(k_abs, "wb") as f:
                f.write(v)
            files_abs.append(k_abs)

        # Update artifacts location
        artifacts_abs = []
        for k in artifacts:
            k_abs = (self._process_info.root / str(k)).resolve()
            if k_abs.is_relative_to(self._process_info.root) is False:
                raise ValueError(f"artifact '{k}' points outside root, which is not allowed!")
            artifacts_abs.append(k_abs)

        # Update logs location
        logs_abs = []
        for k in logs:
            k_abs = (self._process_info.root / str(k)).resolve()
            if k_abs.is_relative_to(self._process_info.root) is False:
                raise ValueError(f"log '{k}' points outside root, which is not allowed!")
            logs_abs.append(k_abs)

        # Update env
        env_new = os.environ.copy()
        if "*" in env and env["*"] is None:
            for k in env_new:
                del env_new[k]
        for k, v in env.items():
            k = str(k)
            if v is None:
                if k in env_new:
                    del env_new[k]
            else:
                env_new[k] = str(v)

        try:
            self._process_info = ProcessInfo(
                root=self._process_info.root,
                cmd=cmd,
                shell=shell,
                env=env_new,
                cwd=cwd_abs,
                files=files_abs,
                artifacts=artifacts_abs,
                logs=logs_abs,
            )
            await self._subprocess_run()
        except Exception as e:
            self._run_info.active = False
            self._run_info.stopping = False
            self._run_info.exception = e
            return fail(f"Failed to start process due to exception: {e}")
        return success()

    async def stop(self) -> tuple[bool, dict]:
        """
        Initiate process termination
        """
        if self._subprocess is None:
            raise ValueError("Stopping but subprocess is not launched!")
        if self._run_info.active is True \
                and self._run_info.stopping is None:
            print("[INFO] Terminating process")
            self._run_info.stopping = True
            self._subprocess.terminate()
        return success()

    async def kill(self) -> tuple[bool, dict]:
        """
        Initiate process kill
        """
        if self._subprocess is None:
            raise ValueError("Killing but subprocess is not launched!")
        if self._run_info.active is True:
            print("[INFO] Killing process")
            self._run_info.stopping = True
            self._subprocess.kill()
        return success()

    async def wait(self) -> tuple[bool, dict]:
        """
        Wait until runnuing process finish
        """
        if self._subprocess is None:
            raise ValueError("Process is not started!")
        if self._run_info.active is False:
            return success()
        do_wait = False
        with self._wait_lock:
            if not self._waiting:
                self._waiting = True
                do_wait = True
        if do_wait:
            if self._run_info.retcode is None:
                retcode, _, _ = await asyncio.gather(
                    self._subprocess.wait(),
                    self._stdout_coroutine,
                    self._stderr_coroutine,
                )
                self._run_info.retcode = retcode
                do_wait = False

                if self._stdout_file is not None:
                    self._stdout_file.close()
                    self._stdout_file = None

                if self._stderr_file is not None:
                    self._stderr_file.close()
                    self._stderr_file = None
        else:
            while self._run_info.active is True:
                await asyncio.sleep(1.)
        return success(retcode=self._run_info.retcode)

    async def ensure_stopped(self) -> tuple[bool, dict]:
        """
        Make sure that if process were started then in finished
        Terminate (kill after timeout) process if necessary
        """
        if self._subprocess is not None:
            if self._run_info.retcode is None:
                if self._run_info.active is True:
                    if self._run_info.stopping is None:
                        await self.stop()
                    wait_until = time.time() + self._terminate_timeout
                    while self._run_info.active and time.time() < wait_until:
                        await asyncio.sleep(.1)
                    if self._run_info.active:
                        await self.kill()
                await self.wait()
        return success()

    async def exit(self, cancel_upload=False) -> tuple[bool, dict]:
        """
        Stop all processes (if any is running)
        """
        await self.ensure_stopped()
        if self._uploader_coroutine is not None:
            if self._uploader_coroutine.done() is False:
                if cancel_upload:
                    await self.cancel_upload()
                await self._uploader_coroutine
        if self._supervisor_coroutine is not None:
            await self._supervisor_coroutine
        return success()

    async def put(self, data) -> tuple[bool, dict]:
        """
        Put data onto subprocess STDIN
        Send None to send EOF
        """
        if self._subprocess is None:
            raise ValueError("Putting but subprocess is not launched!")
        if self._run_info.active is False:
            return fail("Process is not active anymore!")
        if self._run_info.stopping is not None:
            raise ValueError("Process is stopping!")
        if data is None:
            self._subprocess.stdin.write_eof()
        else:
            if isinstance(data, str):
                enc_args = []
                if self._encoding is not None:
                    enc_args.append(self._encoding)
                data = data.encode(*enc_args)
            self._subprocess.stdin.write(data)
        return success()

    async def start_upload(
            self,
            path: str,
            stats: bool = True,
            stdout: bool = True,
            stderr: bool = True,
            logs: bool = True,
            artifacts: bool = True,
    ) -> tuple[bool, dict]:
        """
        Initiate artifcats etc. upload process
        Don't start if it is already uploading
        """
        if self._uploader_coroutine is not None:
            raise ValueError("Uploading were started already!")
        files = []
        if stats:
            data = to_dict(
                process_info=self._process_info.to_dict(),
                run_info=self._run_info.to_dict(),
            )
            stats_file_path = self._process_info.root / ".stats.yaml"
            with open(stats_file_path, "w") as f:
                yaml.safe_dump(data, f)
            files.append(stats_file_path)

        if stdout:
            files.append(self._process_info.root / ".stdout")

        if stderr:
            files.append(self._process_info.root / ".stderr")

        if logs:
            files += self._process_info.logs

        if artifacts:
            files += self._process_info.artifacts

        loop = asyncio.get_event_loop()

        # TODO: get_event_loop deprecated? use get_running_loop?
        # https://stackoverflow.com/questions/44630676/how-can-i-call-an-async-function-without-await

        self._uploader_coroutine = loop.create_task(self._upload(self._process_info.root, files, Path(path)))
        return success()

    def _is_uploading(self):
        return self._uploader_coroutine is not None \
               and not self._uploader_coroutine.done()

    async def is_uploading(self) -> tuple[bool, dict]:
        """
        Check if is uploading
        Result is in 'value' field
        """
        return success(self._is_uploading())

    async def cancel_upload(self, msg: str) -> bool:
        """
        Cancel uppload
        """
        if self._is_uploading():
            result = self._uploader_coroutine.cancel(msg)
            if result:
                return success()
            else:
                return fail("Failed to cancel upload for some reason")
        return success()

    async def _upload(self, src_root: Path, files: list[Path], destination: Path):
        """
        Copy/upload specified files from source to destination
        """
        for fp in files:
            rel_path = fp.relative_to(src_root)
            if rel_path not in self._upload_state:
                rel_path = str(rel_path)
                missing = not fp.exists()
                if not missing:
                    size = os.path.getsize(destination / rel_path)
                else:
                    size = 0
                self._upload_state[rel_path] = UploadState(fp, destination / rel_path, missing)
                self._upload_state[rel_path].size = size
        for k, v in self._upload_state.items():
            skip = True
            if v.missing:
                continue
            with self._thread_lock:
                if v.uploading is None:
                    v.uploading = True
                    skip = False
            if not skip:
                # TODO: replace with something wrapped, not raw copy
                shutil.copy2(v.source, v.target)
                v.uploading = False
                v.progress = v.size

    async def upload_state(self, ):
        """
        Current uppload state
        """
        return success(
            upload_state={k: v.to_dict() for k, v in self._upload_state.items()}
        )

    async def process_info(self, conv_to_str: bool):
        """
        Info for started process (cmd, env etc.)
        """
        return success(process_info=self._process_info.to_dict(conv_to_str=conv_to_str))

    async def run_info(self, conv_to_str: bool):
        """
        Current state of running process
        """
        return success(run_info=self._run_info.to_dict(conv_to_str=conv_to_str))

    @staticmethod
    def _log(log_lines, start, end, time_format, encoding):
        result = []
        for v in log_lines[start:end]:
            v0 = v[0]
            v1 = v[1]
            if time_format is not None:
                if time_format == "":
                    v0 = str(datetime.datetime.fromtimestamp(v0))
                else:
                    v0 = datetime.datetime.fromtimestamp(v0).strftime(time_format)
            if encoding is not None and encoding is not False:
                v1 = v1.decode(encoding)
            result.append((v0, v1))
        return result

    async def stdout(
            self,
            start=None,
            end=None,
            time_format="",
            encoding=None
    ):
        """
        Process' STDOUT in form of list
        Each item is a tuple with
        - time
        - line data

        If time_format is specified (not None) then time converted to date string.
        Specify "" to use default string represnetation of datetime

        If encoding is specified (not False) then lines data are strings
        Othrewise lines data are raw bytes
        """
        return success(stdout=self._log(self._stdout, start, end, time_format, encoding or self._encoding))

    async def stderr(
            self,
            start=None,
            end=None,
            time_format="",
            encoding=None
    ):
        """
        Process' STDERR in form of list
        Each item is a tuple with
        - time
        - line data

        If time_format is specified (not None) then time converted to date string.
        Specify "" to use default string represnetation of datetime

        If encoding is specified (not False) then lines data are strings
        Othrewise lines data are raw bytes
        """
        return success(stderr=self._log(self._stderr, start, end, time_format, encoding or self._encoding))
