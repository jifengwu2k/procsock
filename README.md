# procsock

A small tool for running and tracking background processes over a local TCP port.

It is **not** a terminal multiplexer and does **not** use PTYs. It launches plain child processes, redirects their stdin/stdout/stderr to files, and lets clients query whether a process is still running or has finished.

## Overview

`procsock` consists of two pieces:

- a **server**
- a **client** that connects over a local TCP port when the server is available

The server accepts JSON-RPC requests to start and inspect processes. Each process is started with:

- argv
- client's current working directory
- client's current environment
- optional stdin file
- optional stdout file
- optional stderr file

When not specified, stdin/stdout/stderr default to `os.devnull`.

The launcher returns a process identifier:

- on Unix, it is the process ID
- on Windows/NT, it is the process handle

The server keeps the process state in memory only. If the server exits or restarts, all state is lost.

## Example session

Start server:

```bash
procsock server --port 9000
```

Launch process:

```bash
procsock launch \
  --port 9000 \
  --stdin /tmp/in.txt \
  --stdout /tmp/out.txt \
  --stderr /tmp/err.txt \
  -- /usr/bin/python3 -c 'print("hello")'
```

List:

```bash
procsock list --port 9000
```

Terminate:

```bash
procsock terminate --port 9000 12345
```

## How it works

1. Start the server in the foreground.
2. The server binds a local TCP port.
3. A client connects and sends JSON-RPC commands.
4. The server launches child processes with `ctypes-unicode-proclaunch`.
5. Each launched process is started from a dedicated launcher thread.
6. That launcher thread uses the client's current working directory as the process working directory.
7. This is implemented with a small synchronized `TempCwd` helper class exposing `__enter__` and `__exit__`.
8. A waiter thread waits for process completion and updates the in-memory process status on exit.
9. The client can list the status of all processes or terminate a process.
10. If the server exits, managed children are terminated, and the in-memory process state is lost.

## Commands

### `launch`

Launch a new child process via `ctypes-unicode-proclaunch`.

For the CLI, the launched process inherits the current environment of the `procsock launch` client process.

For each launched process:

- stdin is opened from the requested file path, typically read-only
- stdout is opened to the requested file path
- stderr is opened to the requested file path
- any unspecified stdin/stdout/stderr path defaults to `os.devnull`

Behavior should match normal process redirection semantics:

- stdin: if a path is provided, it must already exist
- stdout: if a path is provided, open with create/truncate behavior
- stderr: if a path is provided, open with create/truncate behavior
- parent directories are not created automatically

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "launch",
  "params": {
    "argv": ["/bin/sh", "-c", "echo hello; sleep 2"],
    "cwd": "/tmp",
    "stdin_path": "/tmp/in.txt",
    "stdout_path": "/tmp/out.txt",
    "stderr_path": "/tmp/err.txt"
  }
}
```

Response:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "pid": 12345,
    "argv": ["/bin/sh", "-c", "echo hello; sleep 2"],
    "cwd": "/tmp",
    "stdin_path": "/tmp/in.txt",
    "stdout_path": "/tmp/out.txt",
    "stderr_path": "/tmp/err.txt",
    "finished": false,
    "started_at": 1760000000.0,
    "finished_at": null,
    "exit_code": null
  }
}
```

### `list`

Return the status of all known in-memory processes.

Request:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "list",
  "params": {}
}
```

Response:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": [
    {
      "pid": 12345,
      "argv": ["/bin/sh", "-c", "echo hello; sleep 2"],
      "cwd": "/tmp",
      "stdin_path": "/tmp/in.txt",
      "stdout_path": "/tmp/out.txt",
      "stderr_path": "/tmp/err.txt",
      "finished": false,
      "started_at": 1760000000.0,
      "finished_at": null,
      "exit_code": null
    }
  ]
}
```

### `terminate`

Terminate a process with `SIGTERM`.

## Contributing

Contributions are welcome! Please submit pull requests or open issues on the GitHub repository.

## License

This project is licensed under the [MIT License](LICENSE).
