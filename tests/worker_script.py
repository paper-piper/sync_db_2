# worker_script.py
from database.synchronizer_db import SynchronizerDB
import sys
import time

def reader_work(filename, key, loop_times):
    sync_db = SynchronizerDB(filename, max_readers=10)
    for _ in range(loop_times):
        sync_db.get_value(key)
        time.sleep(0.1)  # Simulate some work


def writer_work(filename, key, value, loop_times):
    sync_db = SynchronizerDB(filename, max_readers=10)
    for _ in range(loop_times):
        sync_db.set_value(key, value)
        time.sleep(0.1)  # Simulate some work


if __name__ == "__main__":
    role = sys.argv[1]
    filename = sys.argv[2]
    key = int(sys.argv[3])
    value = int(sys.argv[4]) if role == "writer" else None
    loop_times = int(sys.argv[5])

    if role == "reader":
        reader_work(filename, key, loop_times)
    elif role == "writer":
        writer_work(filename, key, value, loop_times)
