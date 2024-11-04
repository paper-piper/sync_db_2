import multiprocessing
from logging_utils import setup_logger
from database.synchronizer_db import SynchronizerDB, SyncState

logger = setup_logger('multiprocessing_test')

FILENAME = "assertions_test.pickle"
MAX_READERS = 10
READERS_NUM = 10
WRITERS_NUM = 20
LOOP_TIMES = 10
DATABASE_LENGTH = 20


def reader_work(sync_db, key):
    for i in range(LOOP_TIMES):
        sync_db.get_value(key)


def writer_work(sync_db, key, value):
    for i in range(LOOP_TIMES):
        sync_db.set_value(key, value)


def special_writer_work(sync_db):
    for i in range(LOOP_TIMES):
        sync_db.set_value(i % (DATABASE_LENGTH // 2), 1)


def assert_synchronizer_multiprocessing():
    """
    Run the same simple db assertion using multiple processes.
    :return: None
    """
    db = {}
    for i in range(DATABASE_LENGTH):
        db[i] = False
    sync_db = SynchronizerDB(
        FILENAME,
        SyncState.PROCESSES,  # Assuming this supports processes
        MAX_READERS,
        db
    )

    processes = []

    for i in range(READERS_NUM):
        # make processes work in range between 0-20
        index = i % DATABASE_LENGTH
        p = multiprocessing.Process(target=reader_work, args=(sync_db, index))
        processes.append(p)

    for i in range(2, WRITERS_NUM):
        # make processes work in range between 10-20
        index = (i % (DATABASE_LENGTH // 2)) + DATABASE_LENGTH // 2
        p = multiprocessing.Process(target=writer_work, args=(sync_db, index, i))
        processes.append(p)

    special_process = multiprocessing.Process(target=special_writer_work, args=(sync_db,))
    processes.append(special_process)

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    for i in range(DATABASE_LENGTH):
        print(sync_db.get_value(i))


if __name__ == "__main__":
    assert_synchronizer_multiprocessing()
