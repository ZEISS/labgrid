import errno
import getpass
import logging
import os
import queue
import re
import select
import subprocess
import threading
from contextlib import closing
from socket import AF_INET, SOCK_STREAM, socket

try:
    import fcntl
except ImportError:
    # fcntl is not available, e. g. on Microsoft Windows
    fcntl = None

try:
    import pty
except ImportError:
    # pty is not available, presumably as we're on Microsoft Windows
    pty = None

import attr

from ..step import step
from .timeout import Timeout

re_vt100 = re.compile(r"(\x1b\[|\x9b)[^@-_a-z]*[@-_a-z]|\x1b[@-_a-z]")

def get_free_port():
    """Helper function to always return an unused port."""
    with closing(socket(AF_INET, SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def get_user():
    """Get the username of the current user."""
    user = os.environ.get("USER")
    if user:
        return user
    return getpass.getuser()

def set_nonblocking(fd):
    if fcntl is not None:
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    else:
        # This shouldn't even show up on Microsoft Windows later, as we can't use pty.openpty anyways...
        logging.warning("Can't make socket non-blocking on this system due to missing fcntl!")

@attr.s
class ProcessWrapper:
    callbacks = attr.ib(default=attr.Factory(list))
    loglevel = logging.INFO

    @step(args=['command'], result=True, tag='process')
    def check_output(
        self, command, *, print_on_silent_log=False, input=None, stdin=None, stderr=subprocess.STDOUT, timeout=None
    ):  # pylint: disable=redefined-builtin
        """Run a command and supply the output to callback functions"""

        if stderr not in (None, subprocess.STDOUT, subprocess.DEVNULL):
            raise NotImplementedError(f"{stderr=} not supported")

        logger = logging.getLogger("Process")

        if pty is None:
            return self._check_output_windows(
                command, logger, print_on_silent_log, input, stdin, stderr, timeout)

        res = []
        mfd, sfd = pty.openpty()
        set_nonblocking(mfd)

        kwargs = {}

        stdin_r = None
        stdin_w = None
        if input is not None:
            stdin_r, stdin_w = os.pipe()
            kwargs['stdin'] = stdin_r
            set_nonblocking(stdin_w)
        elif stdin is not None:
            kwargs['stdin'] = stdin

        process = subprocess.Popen(command, stderr=stderr, stdout=sfd, bufsize=0, **kwargs)

        logger.log(ProcessWrapper.loglevel, "[%d] command: %s", process.pid, " ".join(command))

        # do not register/unregister already registered print_callback
        if ProcessWrapper.print_callback in self.callbacks:
            print_on_silent_log = False

        if print_on_silent_log and logger.getEffectiveLevel() > ProcessWrapper.loglevel:
            self.enable_print()

        if stdin_r is not None:
            os.close(stdin_r)

        # close sfd so we notice when the child is gone
        os.close(sfd)
        # get a file object from the fd
        buf = b""
        read_fds = [mfd]
        write_fds = []
        if stdin_w is not None:
            write_fds.append(stdin_w)

        if timeout is not None:
            timeout = Timeout(timeout)

        while True:
            ready_r, ready_w, _ = select.select(read_fds, write_fds, [], 0.1)

            if mfd in ready_r:
                raw = None

                try:
                    raw = os.read(mfd, 4096)
                except BlockingIOError:
                    pass
                except OSError as e:
                    if e.errno == errno.EIO:
                        break
                    raise

                if raw:
                    buf += raw
                    *parts, buf = buf.split(b'\r')
                    res.extend(parts)
                    for part in parts:
                        for callback in self.callbacks:
                            callback(part, process)

            if stdin_w in ready_w:
                if input:
                    amt = 0
                    try:
                        amt = os.write(stdin_w, input)
                    except BlockingIOError:
                        pass
                    except OSError as e:
                        if e.errno == errno.EIO:
                            break
                        raise
                    input = input[amt:]

                if not input:
                    write_fds.remove(stdin_w)
                    os.close(stdin_w)
                    stdin_w = None

            process.poll()
            if process.returncode is not None and len(ready_r) == 0:
                break

            if timeout is not None and timeout.expired:
                process.kill()
                break

        if stdin_w is not None:
            os.close(stdin_w)

        os.close(mfd)
        process.wait()
        if buf:
            # process incomplete line
            res.append(buf)
            if not buf.endswith(b'\n'):
                buf += b'\n'
            for callback in self.callbacks:
                callback(buf, process)

        if print_on_silent_log and logger.getEffectiveLevel() > ProcessWrapper.loglevel:
            self.disable_print()

        if timeout is not None and timeout.expired:
            raise TimeoutError(
                f"Timeout of {timeout.timeout} seconds exceeded while waiting for process: [{process.pid}] command: {command}"
            )

        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode,
                                                command,
                                                output=b'\r'.join(res))
        # this converts '\r\n' to '\n' to be more compatible to the behaviour
        # of the normal subprocess module
        return b'\n'.join([r.strip(b'\n') for r in res])

    def _check_output_windows(self, command, logger, print_on_silent_log, input, stdin,
                              stderr, timeout):
        if input is not None and stdin is not None:
            raise ValueError("stdin and input cannot both be supplied")
        if input is not None and not isinstance(input, (bytes, bytearray, memoryview)):
            raise TypeError("input must be bytes-like")
        deadline = Timeout(float(timeout)) if timeout is not None else None
        with subprocess.Popen(
            command, stdin=subprocess.PIPE if input is not None else stdin,
            stdout=subprocess.PIPE, stderr=stderr,
        ) as process:
            logger.log(self.loglevel, "[%d] command: %s", process.pid, " ".join(command))
            events = queue.Queue()

            def read_output():
                try:
                    while data := process.stdout.read1(65536):
                        events.put(data)
                except OSError as error:
                    events.put(error)
                finally:
                    events.put(None)

            def write_input():
                try:
                    with process.stdin:
                        process.stdin.write(input)
                except BrokenPipeError:
                    pass
                except OSError as error:
                    events.put(error)

            reader = threading.Thread(target=read_output, daemon=True)
            reader.start()
            writer = None
            if input is not None:
                writer = threading.Thread(target=write_input, daemon=True)
                writer.start()
            show_output = (
                print_on_silent_log
                and self.print_callback not in self.callbacks
                and logger.getEffectiveLevel() > self.loglevel
            )
            if show_output:
                self.enable_print()
            result = []
            buf = b""
            try:
                while True:
                    if deadline is not None and deadline.expired:
                        raise TimeoutError(f"Timeout of {timeout} seconds exceeded: {command}")
                    try:
                        data = events.get(timeout=0.05)
                    except queue.Empty:
                        continue
                    if data is None:
                        break
                    if isinstance(data, OSError):
                        raise data
                    result.append(data)
                    *lines, buf = (buf + data).split(b"\n")
                    for line in lines:
                        for callback in self.callbacks:
                            callback(line.rstrip(b"\r"), process)
                remaining = deadline.remaining if deadline is not None else None
                try:
                    process.wait(timeout=remaining)
                except subprocess.TimeoutExpired as error:
                    raise TimeoutError(f"Timeout of {timeout} seconds exceeded: {command}") from error
                if buf:
                    for callback in self.callbacks:
                        callback(buf, process)
                output = b"".join(result).replace(b"\r\n", b"\n")
                if process.returncode:
                    raise subprocess.CalledProcessError(process.returncode, command, output=output)
                return output
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                reader.join()
                if writer is not None:
                    writer.join()
                if show_output:
                    self.disable_print()

    def register(self, callback):
        """Register a callback with the ProcessWrapper"""
        if callback in self.callbacks:
            return
        self.callbacks.append(callback)

    def unregister(self, callback):
        """Unregister a callback with the ProcessWrapper"""
        if callback not in self.callbacks:
            return
        self.callbacks.remove(callback)

    @staticmethod
    def log_callback(message, process):
        """Logs process output message along with its pid."""
        logger = logging.getLogger("Process")
        message = message.decode(encoding="utf-8", errors="replace").strip("\n")
        if message:
            logger.log(ProcessWrapper.loglevel, "[%d] %s", process.pid, message)

    @staticmethod
    def print_callback(message, _):
        """Prints process output message."""
        message = message.decode(encoding="utf-8", errors="replace")
        print(f"\r{message}", end='')

    def enable_logging(self):
        """Enables process output to the logging interface."""
        self.register(ProcessWrapper.log_callback)

    def disable_logging(self):
        """Disables process output logging."""
        self.unregister(ProcessWrapper.log_callback)

    def enable_print(self):
        """Enables process output to print."""
        self.register(ProcessWrapper.print_callback)

    def disable_print(self):
        """Disables process output printing."""
        self.unregister(ProcessWrapper.print_callback)


processwrapper = ProcessWrapper()
