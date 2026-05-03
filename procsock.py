# Copyright (c) 2025 Jifeng Wu
# Licensed under the MIT License. See LICENSE file in the project root for full license information.
#!/usr/bin/env python
from __future__ import print_function, unicode_literals
import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from itertools import count
from typing import Any, Dict, List, Optional, Set, Text, Tuple

import dockpoint

import ctypes_unicode_proclaunch
from read_unicode_environment_variables_dictionary import (
    read_unicode_environment_variables_dictionary,
)
from send_recv_json import recv_json, send_json
from textcompat import filesystem_str_to_text

JSONRPC_VERSION = "2.0"
DEFAULT_APP_NAME = "procsock"


class UnknownPidError(Exception):
    pass


class TempCwd(object):
    __slots__ = ("cwd", "previous_cwd")

    _lock = threading.RLock()

    def __init__(self, cwd):
        # type: (Text) -> None
        self.cwd = cwd  # type: Text
        self.previous_cwd = None  # type: Optional[Text]

    def __enter__(self):
        # type: () -> "TempCwd"
        self._lock.acquire()
        try:
            self.previous_cwd = os.getcwd()
            os.chdir(self.cwd)
            return self
        except Exception:
            self._lock.release()
            raise

    def __exit__(self, exc_type, exc, tb):
        # type: (Any, Any, Any) -> None
        try:
            if self.previous_cwd is not None:
                os.chdir(self.previous_cwd)
        finally:
            self._lock.release()


class Process(object):
    __slots__ = (
        "pid",
        "argv",
        "cwd",
        "stdin_path",
        "stdout_path",
        "stderr_path",
        "finished",
        "started_at",
        "finished_at",
        "exit_code",
        "wait_thread",
    )

    def __init__(
        self,
        pid,
        argv,
        cwd,
        stdin_path,
        stdout_path,
        stderr_path,
        finished,
        started_at,
        finished_at,
        exit_code,
        wait_thread=None,
    ):
        # type: (int, List[Text], Text, Text, Text, Text, bool, float, Optional[float], Optional[int], Optional[threading.Thread]) -> None
        self.pid = pid  # type: int
        self.argv = argv  # type: List[Text]
        self.cwd = cwd  # type: Text
        self.stdin_path = stdin_path  # type: Text
        self.stdout_path = stdout_path  # type: Text
        self.stderr_path = stderr_path  # type: Text
        self.finished = finished  # type: bool
        self.started_at = started_at  # type: float
        self.finished_at = finished_at  # type: Optional[float]
        self.exit_code = exit_code  # type: Optional[int]
        self.wait_thread = wait_thread  # type: Optional[threading.Thread]

    def to_dict(self):
        # type: () -> Dict[Text, Any]
        return {
            "pid": self.pid,
            "argv": self.argv,
            "cwd": self.cwd,
            "stdin_path": self.stdin_path,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "finished": self.finished,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "exit_code": self.exit_code,
        }


class ProcessTable(object):
    __slots__ = ("_lock", "_processes")

    def __init__(self):
        # type: () -> None
        self._lock = threading.RLock()
        self._processes = {}  # type: Dict[int, Process]

    def add(self, process):
        # type: (Process) -> None
        with self._lock:
            if process.pid in self._processes:
                raise RuntimeError("pid already tracked: {}".format(process.pid))
            self._processes[process.pid] = process

    def get(self, pid):
        # type: (int) -> Process
        with self._lock:
            try:
                return self._processes[pid]
            except KeyError as exc:
                raise UnknownPidError("unknown pid: {}".format(pid))

    def list(self):
        # type: () -> List[Process]
        with self._lock:
            return sorted(
                self._processes.values(),
                key=lambda process: (process.started_at, process.pid),
            )

    def mark_finished(self, pid, exit_code):
        # type: (int, Optional[int]) -> None
        with self._lock:
            process = self._processes.get(pid)
            if process is None:
                return
            process.finished = True
            process.finished_at = time.time()
            process.exit_code = exit_code

    def unfinished(self):
        # type: () -> List[Process]
        with self._lock:
            return [
                process for process in self._processes.values() if not process.finished
            ]


class ProcSockServer(object):
    __slots__ = (
        "app_name",
        "instance",
        "processes",
        "stop_event",
        "dockpoint_ref",
        "connection_threads",
        "connection_threads_lock",
    )

    def __init__(self, app_name=DEFAULT_APP_NAME, instance=dockpoint.DEFAULT_INSTANCE):
        # type: (Text, Text) -> None
        self.app_name = app_name  # type: Text
        self.instance = instance  # type: Text
        self.processes = ProcessTable()
        self.stop_event = threading.Event()
        self.dockpoint_ref = None  # type: Optional[dockpoint.Dockpoint]
        self.connection_threads = set()  # type: Set[threading.Thread]
        self.connection_threads_lock = threading.Lock()

    def serve_forever(self):
        # type: () -> None
        self.dockpoint_ref = dockpoint.claim(self.app_name, self.instance)
        if self.dockpoint_ref is None:
            raise RuntimeError(
                "could not claim dockpoint for app_name=%r instance=%r"
                % (self.app_name, self.instance)
            )
        logging.info(
            "claimed dockpoint app_name=%r instance=%r",
            self.app_name,
            self.instance,
        )

        self._install_signal_handlers()

        try:
            while not self.stop_event.is_set():
                try:
                    conn = self.dockpoint_ref.accept()
                except (OSError, ValueError):
                    if self.stop_event.is_set():
                        break
                    raise
                thread = threading.Thread(
                    target=self._handle_connection, args=(conn,), daemon=True
                )
                with self.connection_threads_lock:
                    self.connection_threads.add(thread)
                thread.start()
        finally:
            self.shutdown()

    def _install_signal_handlers(self):
        # type: () -> None
        def handler(signum, frame):
            # type: (int, Any) -> None
            logging.info("received signal %s, shutting down", signum)
            self.stop_event.set()
            if self.dockpoint_ref is not None:
                try:
                    self.dockpoint_ref.close()
                except (OSError, ValueError):
                    pass

        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, handler)

    def shutdown(self):
        # type: () -> None
        if self.stop_event.is_set():
            pass
        else:
            self.stop_event.set()
        if self.dockpoint_ref is not None:
            try:
                self.dockpoint_ref.close()
            except (OSError, ValueError):
                pass
            self.dockpoint_ref = None

        unfinished = self.processes.unfinished()
        for process in unfinished:
            try:
                ctypes_unicode_proclaunch.terminate(process.pid)
            except Exception as exc:  # noqa: BLE001
                logging.warning("failed to terminate pid=%s: %s", process.pid, exc)

        for process in unfinished:
            thread = process.wait_thread
            if thread is None:
                continue
            thread.join()

    def _handle_connection(self, conn):
        # type: (dockpoint.DockpointConnection) -> None
        request = None  # type: Any
        try:
            with conn:
                try:
                    request = recv_json(conn.read)
                    response = self._dispatch_request(request)
                except Exception as exc:  # noqa: BLE001
                    logging.warning(
                        "request handling failed: %s: %s",
                        type(exc).__name__,
                        exc,
                    )
                    response = make_error_response(
                        request_id=(
                            request.get("id") if isinstance(request, dict) else None
                        ),
                        exc=exc,
                    )
                send_json(conn.write, response)
        finally:
            with self.connection_threads_lock:
                thread = threading.current_thread()
                self.connection_threads.discard(thread)

    def _dispatch_request(self, request):
        # type: (Any) -> Dict[Text, Any]
        request_id = request["id"]
        method = request["method"]
        params = request.get("params", {})

        if method == "launch":
            result = self.launch(
                params["argv"],
                params["cwd"],
                params.get("stdin_path", os.devnull),
                params.get("stdout_path", os.devnull),
                params.get("stderr_path", os.devnull),
                params.get("env"),
            )
        elif method == "list":
            result = self.list_processes()
        elif method == "terminate":
            result = self.terminate(params["pid"])
        else:
            raise ValueError("unknown method: {}".format(method))

        return {"jsonrpc": JSONRPC_VERSION, "id": request_id, "result": result}

    def launch(self, argv, cwd, stdin_path, stdout_path, stderr_path, env):
        # type: (List[Text], Text, Text, Text, Text, Optional[Dict[Text, Text]]) -> Dict[Text, Any]
        ready = threading.Event()
        outcome = {}  # type: Dict[Text, Any]

        def launcher_target():
            # type: () -> None
            try:
                pid = self._launch_process(
                    argv,
                    cwd,
                    stdin_path,
                    stdout_path,
                    stderr_path,
                    env,
                )
                outcome["pid"] = pid
            except Exception as exc:  # noqa: BLE001
                outcome["exception"] = exc
            finally:
                ready.set()

        launcher_thread = threading.Thread(target=launcher_target, daemon=True)
        launcher_thread.start()
        ready.wait()

        if "exception" in outcome:
            raise outcome["exception"]

        process = self.processes.get(outcome["pid"])
        return process.to_dict()

    def _launch_process(self, argv, cwd, stdin_path, stdout_path, stderr_path, env):
        # type: (List[Text], Text, Text, Text, Text, Optional[Dict[Text, Text]]) -> int
        stdin_file = None  # type: Any
        stdout_file = None  # type: Any
        stderr_file = None  # type: Any
        try:
            stdin_file = open(stdin_path, "rb")
            stdout_file = open(stdout_path, "wb")
            stderr_file = open(stderr_path, "wb")

            with TempCwd(cwd):
                raw_pid = ctypes_unicode_proclaunch.launch(
                    argv,
                    environment=env,
                    stdin_file_descriptor=stdin_file.fileno(),
                    stdout_file_descriptor=stdout_file.fileno(),
                    stderr_file_descriptor=stderr_file.fileno(),
                )

            pid = int(raw_pid)
            started_at = time.time()
            process = Process(
                pid=pid,
                argv=list(argv),
                cwd=cwd,
                stdin_path=stdin_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                finished=False,
                started_at=started_at,
                finished_at=None,
                exit_code=None,
            )
            wait_thread = threading.Thread(
                target=self._wait_for_process,
                args=(pid,),
                daemon=True,
                name="wait-pid-{}".format(pid),
            )
            process.wait_thread = wait_thread
            self.processes.add(process)
            wait_thread.start()
            logging.info("launched pid=%s argv=%r cwd=%s", pid, argv, cwd)
            return pid
        finally:
            for file_obj in (stdin_file, stdout_file, stderr_file):
                if file_obj is not None:
                    file_obj.close()

    def _wait_for_process(self, pid):
        # type: (int) -> None
        try:
            exit_code = int(ctypes_unicode_proclaunch.wait(pid))
            self.processes.mark_finished(pid, exit_code)
            logging.info("process finished pid=%s exit_code=%s", pid, exit_code)
        except Exception as exc:  # noqa: BLE001
            logging.exception("wait failed for pid=%s", pid)
            self.processes.mark_finished(pid, None)

    def list_processes(self):
        # type: () -> List[Dict[Text, Any]]
        return [process.to_dict() for process in self.processes.list()]

    def terminate(self, pid):
        # type: (int) -> Dict[Text, Any]
        process = self.processes.get(pid)
        if not process.finished:
            ctypes_unicode_proclaunch.terminate(process.pid)
            logging.info("terminate requested pid=%s", process.pid)
        return process.to_dict()


def make_error_response(request_id, exc):
    # type: (Any, Exception) -> Dict[Text, Any]
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "error": {
            "type": type(exc).__name__,
            "message": str(exc),
        },
    }


_REQUEST_IDS = count(1)


def _connect_with_auto_start(app_name, instance):
    # type: (Text, Text) -> dockpoint.DockpointConnection
    """Connect to the dockpoint server, auto-starting it if unavailable."""
    conn = dockpoint.connect(app_name, instance)
    if conn is not None:
        return conn

    logging.info(
        "server not running for app_name=%r instance=%r, auto-starting...",
        app_name,
        instance,
    )
    devnull_r = open(os.devnull, "rb")
    devnull_w = open(os.devnull, "wb")
    try:
        ctypes_unicode_proclaunch.launch(
            [sys.executable, sys.argv[0], "server", "--instance", instance],
            stdin_file_descriptor=devnull_r.fileno(),
            stdout_file_descriptor=devnull_w.fileno(),
            stderr_file_descriptor=devnull_w.fileno(),
        )
    finally:
        devnull_r.close()
        devnull_w.close()

    deadline = time.time() + 10.0
    while time.time() < deadline:
        time.sleep(0.05)
        conn = dockpoint.connect(app_name, instance)
        if conn is not None:
            logging.info(
                "server auto-started successfully app_name=%r instance=%r",
                app_name,
                instance,
            )
            return conn

    raise RuntimeError(
        "could not connect to dockpoint app_name=%r instance=%r"
        " (server auto-start timed out)"
        % (app_name, instance)
    )


def send_jsonrpc_request(app_name, instance, method, params):
    # type: (Text, Text, Text, Dict[Text, Any]) -> Any
    request = {
        "jsonrpc": JSONRPC_VERSION,
        "id": next(_REQUEST_IDS),
        "method": method,
        "params": params,
    }
    conn = _connect_with_auto_start(app_name, instance)
    with conn:
        send_json(conn.write, request)
        response = recv_json(conn.read)
    if "error" in response:
        error = response["error"]
        error_type = error.get("type", "Exception")
        error_message = error.get("message", "")
        raise RuntimeError("{}: {}".format(error_type, error_message))
    return response["result"]


def request_launch(app_name, instance, argv, cwd, stdin_path, stdout_path, stderr_path, env):
    # type: (Text, Text, List[Text], Text, Text, Text, Text, Dict[Text, Text]) -> Any
    return send_jsonrpc_request(
        app_name,
        instance,
        "launch",
        {
            "argv": argv,
            "cwd": cwd,
            "stdin_path": stdin_path,
            "stdout_path": stdout_path,
            "stderr_path": stderr_path,
            "env": env,
        },
    )


def request_list(app_name, instance):
    # type: (Text, Text) -> Any
    return send_jsonrpc_request(app_name, instance, "list", {})


def request_terminate(app_name, instance, pid):
    # type: (Text, Text, int) -> Any
    return send_jsonrpc_request(app_name, instance, "terminate", {"pid": pid})


def main(argv=None):
    # type: (Optional[List[Text]]) -> int
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(prog="procsock")
    subparsers = parser.add_subparsers(dest="command")

    server_parser = subparsers.add_parser("server", help="run the procsock server")
    server_parser.add_argument("--instance", default=str(dockpoint.DEFAULT_INSTANCE))

    launch_parser = subparsers.add_parser("launch", help="launch a process")
    launch_parser.add_argument("--instance", default=str(dockpoint.DEFAULT_INSTANCE))
    launch_parser.add_argument("--cwd", default=None)
    launch_parser.add_argument("--stdin", dest="stdin_path", default=os.devnull)
    launch_parser.add_argument("--stdout", dest="stdout_path", default=os.devnull)
    launch_parser.add_argument("--stderr", dest="stderr_path", default=os.devnull)
    launch_parser.add_argument("argv", nargs=argparse.REMAINDER)

    list_parser = subparsers.add_parser("list", help="list tracked processes")
    list_parser.add_argument("--instance", default=str(dockpoint.DEFAULT_INSTANCE))

    terminate_parser = subparsers.add_parser("terminate", help="terminate a process")
    terminate_parser.add_argument("--instance", default=str(dockpoint.DEFAULT_INSTANCE))
    terminate_parser.add_argument("pid", type=int)
    args = parser.parse_args(argv)

    if args.command == "server":
        server = ProcSockServer(
            app_name=DEFAULT_APP_NAME,
            instance=filesystem_str_to_text(args.instance)
        )
        server.serve_forever()

    elif args.command == "launch":
        launch_argv = list(map(filesystem_str_to_text, args.argv))
        if len(launch_argv) < 2 or launch_argv[0] != "--":
            raise ValueError("missing command after '--'")

        result = request_launch(
            DEFAULT_APP_NAME,
            filesystem_str_to_text(args.instance),
            launch_argv[1:],
            filesystem_str_to_text(args.cwd if args.cwd is not None else os.getcwd()),
            filesystem_str_to_text(args.stdin_path),
            filesystem_str_to_text(args.stdout_path),
            filesystem_str_to_text(args.stderr_path),
            read_unicode_environment_variables_dictionary(),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "list":
        result = request_list(
            DEFAULT_APP_NAME,
            filesystem_str_to_text(args.instance),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif args.command == "terminate":
        result = request_terminate(
            DEFAULT_APP_NAME,
            filesystem_str_to_text(args.instance),
            args.pid
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(main())
