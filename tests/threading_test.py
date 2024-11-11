from multiprocessing import Process
from database.synchronizer_db import SynchronizerDB  # Ensure synchronizer_db.py is the file containing SynchronizerDB
import random
import time

# Configuration parameters
MAX_READERS = 10
READERS_NUM = 10
WRITERS_NUM = 20
LOOP_TIMES = 10
DATABASE_LENGTH = 20


def reader_work(filename, key):
    sync_db = SynchronizerDB(filename, max_readers=MAX_READERS)
    for i in range(LOOP_TIMES):
        sync_db.get_value(key)
        time.sleep(random.uniform(0.1, 0.5))  # Random sleep to simulate variable read times


def writer_work(filename, key, value):
    sync_db = SynchronizerDB(filename, max_readers=MAX_READERS)
    for i in range(LOOP_TIMES):
        sync_db.set_value(key, value)
        time.sleep(random.uniform(0.1, 0.5))  # Random sleep to simulate variable write times


def special_writer_work(filename):
    sync_db = SynchronizerDB(filename, max_readers=MAX_READERS)
    for i in range(LOOP_TIMES):
        key = i % (DATABASE_LENGTH // 2)
        sync_db.set_value(key, 1)
        time.sleep(random.uniform(0.1, 0.5))  # Random sleep to simulate variable write times


def main():
    # Path to database file for SynchronizerDB
    db_file = "database.db"  # This would be the filename for your FileDB superclass

    # Generate random keys and values for writers
    writer_key = 0
    writer_value = 5

    # Create reader processes
    reader_processes = [
        Process(target=reader_work, args=(db_file, reader_key))
        for reader_key in range(READERS_NUM)
    ]

    # Create writer processes
    writer_processes = [
        Process(target=writer_work, args=(db_file, writer_key, writer_value))
        for _ in range(WRITERS_NUM)
    ]

    # Create a special writer process
    special_writer = Process(target=special_writer_work, args=(db_file,))

    # Start all processes
    for process in reader_processes + writer_processes + [special_writer]:
        process.start()

    # Wait for all processes to complete
    for process in reader_processes + writer_processes + [special_writer]:
        process.join()

    # Print the final key-value pairs in the database
    final_db = SynchronizerDB(db_file, max_readers=MAX_READERS)
    print("Final key-value pairs in the database:")
    for key in range(DATABASE_LENGTH):
        value = final_db.get_value(key)
        print(f"Key {key}: Value {value}")


if __name__ == "__main__":
    main()
