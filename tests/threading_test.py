import threading
from logging_utils import setup_logger
from database.synchronizer_db import SynchronizerDB, SyncState

logger = setup_logger('threading_test')


FILENAME = "assertions_test.pickle"
MAX_READERS = 10
READERS_NUM = 100
WRITERS_NUM = 20
LOOP_TIMES = 100
DATABASE_LENGTH = 20


def reader_work(sync_db, key):
    for i in range(LOOP_TIMES):
        sync_db.get_value(key)


def writer_work(sync_db, key, value):
    for i in range(LOOP_TIMES):
        sync_db.set_value(key, value)


def special_writer_work(sync_db):
    for i in range(LOOP_TIMES):
        sync_db.set_value(i % (DATABASE_LENGTH/2), 1)


def assert_synchronizer_threads():
    """
    make multiple threads run the same simple db assertion at the same time.
    :return: None
    """
    db = {}
    for i in range(DATABASE_LENGTH):
        db[i] = False
    sync_db = SynchronizerDB(
        FILENAME,
        SyncState.THREADS,
        MAX_READERS,
        db
    )
    threads = []
    for i in range(READERS_NUM):
        # make threads work in range between 0-20
        index = i % DATABASE_LENGTH
        threads.append(threading.Thread(target=reader_work, args=(sync_db, index)))

    for i in range(2, WRITERS_NUM):
        # make threads work in range between 10-20
        index = (i % (DATABASE_LENGTH/2)) + DATABASE_LENGTH/2
        threads.append(threading.Thread(target=writer_work, args=(sync_db, index, i)))

    special_thread = threading.Thread(target=special_writer_work, args=(sync_db,))
    threads.append(special_thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    for i in range(DATABASE_LENGTH):
        print(sync_db.get_value(i))


if __name__ == "__main__":
    assert_synchronizer_threads()
