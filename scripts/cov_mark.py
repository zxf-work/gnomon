"""cov_mark: phase marks for the memory pass. With COV_MARK_FILE set, mark(s) appends one line "<unix time> <pid> <s>" to that file (O_APPEND, one
write); unset, it does nothing and costs one attribute test. The sampler's samples are placed in a phase from these lines afterwards. No timed path of the kit sets it."""
import os
import time

_P = os.environ.get("COV_MARK_FILE")


def mark(s):
    if _P:
        fd = os.open(_P, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, f"{time.time():.3f} {os.getpid()} {s}\n".encode())
        finally:
            os.close(fd)
