import threading

from database.file_db import FileDB
from logging_utils import setup_logger
from enum import Enum
import win32event
import win32con
import win32api
import win32process
import multiprocessing
# Setup logger for file
logger = setup_logger('synchronizer_db')


class SyncState(Enum):
    THREADS = 0
    PROCESSES = 1


class SynchronizerDB(FileDB):
    def __init__(self, filename, state, max_readers=10, database=None, read_semaphore=None, write_lock=None):
        """
        A database synchronizer that handles concurrency using either threads or processes.
        :param filename: The name of the file to use for the database.
        :param state: SyncState specifying whether to use threads or processes.
        :param max_readers: Maximum number of concurrent readers.
        :param database: Optional database object to initialize the FileDB superclass.
        :param read_semaphore: Semaphore for managing readers (optional for external sync).
        :param write_lock: Lock for managing writers (optional for external sync).
        :return: None
        """
        super().__init__(filename, database)
        self.state = state
        self.max_readers = max_readers

        # Initialize synchronization objects based on the state
        if state == SyncState.THREADS:
            self.read_semaphore = threading.Semaphore(max_readers)
            self.write_lock = threading.Lock()
        elif state == SyncState.PROCESSES:
            # Use external locks and semaphores if provided
            self.read_semaphore = read_semaphore or multiprocessing.Semaphore(max_readers)
            self.write_lock = write_lock or multiprocessing.Lock()
        else:
            raise Exception("Invalid state")

    def get_value(self, key):
        """
        Retrieves a value from the database by key.
        :param key: The key for which the value is requested.
        :return: The value associated with the provided key.
        """
        with self.read_semaphore:
            super().load_file()
            return super().get_value(key)

    def set_value(self, key, value):
        """
        Sets a value in the database for the specified key.
        :param key: The key for which the value is to be set.
        :param value: The value to set in the database.
        :return: The result of setting the value.
        """
        with self.write_lock:
            super().load_file()
            result = super().set_value(key, value)
            super().dump_file()
            return result

    def delete_value(self, key):
        """
        Deletes a value from the database for the specified key.
        :param key: The key for which the value is to be deleted.
        :return: The result of deleting the value.
        """
        self.get_write_lock()  # Acquire write lock

        try:
            super().load_file()
            results = super().delete_value(key)
            super().dump_file()
        finally:
            self.release_write_lock()  # Release write lock

        return results

    def get_write_lock(self):
        """
        Acquires exclusive access for writing by using a mutex and blocking readers.
        """
        # Wait for the write mutex
        win32event.WaitForSingleObject(self.write_mutex, -1)

        # Block all readers by acquiring the read semaphore to the max count
        for _ in range(self.max_readers):
            win32event.WaitForSingleObject(self.read_semaphore, -1)

    def release_write_lock(self):
        """
        Releases exclusive access for writing by releasing the mutex and unblocking readers.
        """
        # Release read semaphore for all max readers
        for _ in range(self.max_readers):
            win32event.ReleaseSemaphore(self.read_semaphore, 1)

        # Release the write mutex
        win32event.ReleaseMutex(self.write_mutex)

    def check_for_max_readers(self):
        """
        Checks if the maximum number of readers has been reached.
        """
        result = win32event.WaitForSingleObject(self.read_semaphore, 0)  # Non-blocking wait
        if result == win32con.WAIT_TIMEOUT:
            logger.info("Reached max readers!")
        else:
            win32event.ReleaseSemaphore(self.read_semaphore, 1)

