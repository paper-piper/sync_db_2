import win32event
import win32con
from database.file_db import FileDB
from logging_utils import setup_logger

# Setup logger for file
logger = setup_logger('synchronizer_db')


class SynchronizerDB(FileDB):
    def __init__(self, filename, max_readers=10, database=None):
        """
        A database synchronizer that handles concurrency using Windows native synchronization mechanisms.
        :param filename: The name of the file to use for the database.
        :param max_readers: Maximum number of concurrent readers.
        :param database: Optional database object to initialize the FileDB superclass.
        """
        super().__init__(filename, database)
        self.max_readers = max_readers

        # Initialize synchronization objects
        self.read_semaphore = win32event.CreateSemaphore(None, max_readers, max_readers, None)
        self.write_mutex = win32event.CreateMutex(None, False, None)

    def get_value(self, key):
        """
        Retrieves a value from the database by key.
        :param key: The key for which the value is requested.
        :return: The value associated with the provided key.
        """
        # Wait for the read semaphore (blocking readers if max reached)
        win32event.WaitForSingleObject(self.read_semaphore, -1)
        try:
            super().load_file()
            return super().get_value(key)
        finally:
            # Release the read semaphore after reading
            win32event.ReleaseSemaphore(self.read_semaphore, 1)

    def set_value(self, key, value):
        """
        Sets a value in the database for the specified key.
        :param key: The key for which the value is to be set.
        :param value: The value to set in the database.
        :return: The result of setting the value.
        """
        # Acquire exclusive access for writing using the write mutex
        win32event.WaitForSingleObject(self.write_mutex, -1)
        try:
            super().load_file()
            result = super().set_value(key, value)
            super().dump_file()
            return result
        finally:
            # Release the write mutex after writing
            win32event.ReleaseMutex(self.write_mutex)

    def delete_value(self, key):
        """
        Deletes a value from the database for the specified key.
        :param key: The key for which the value is to be deleted.
        :return: The result of deleting the value.
        """
        # Acquire write lock using the write mutex
        self.get_write_lock()
        try:
            super().load_file()
            result = super().delete_value(key)
            super().dump_file()
            return result
        finally:
            # Release write lock
            self.release_write_lock()

    def get_write_lock(self):
        """
        Acquires exclusive access for writing by blocking readers.
        """
        # Wait for the write mutex
        win32event.WaitForSingleObject(self.write_mutex, -1)

        # Block all readers by fully acquiring the read semaphore
        for _ in range(self.max_readers):
            win32event.WaitForSingleObject(self.read_semaphore, -1)

    def release_write_lock(self):
        """
        Releases exclusive access for writing by unblocking readers.
        """
        # Release the read semaphore for all max readers
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
            # If successful, release the semaphore since we're just checking
            win32event.ReleaseSemaphore(self.read_semaphore, 1)
