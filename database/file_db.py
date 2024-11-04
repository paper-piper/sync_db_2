from database.base_db import Database
import pickle
from logging_utils import setup_logger
import win32file
import win32con

# Setup logger for file
logger = setup_logger('file_db')


class FileDB(Database):

    def __init__(self, filename, database=None):
        """
        Initiate a database which can write to file.
        :param filename: The name of the file where the data will be saved.
        :param database: Optional, will run over the old database and create a new one.
        """
        super().__init__(database)
        self.filename = filename
        if database is not None:
            self.dump_file()

    def dump_file(self):
        """
        Serializes and writes the object to the current database using WinAPI.
        :return: True if the operation is successful, False otherwise.
        """
        try:
            # Serialize the database to bytes
            data = pickle.dumps(self.db)

            # Open the file using WinAPI CreateFile
            handle = win32file.CreateFile(
                self.filename,
                win32con.GENERIC_WRITE,
                0,  # No sharing
                None,
                win32con.CREATE_ALWAYS,  # Overwrite the file if it exists
                win32con.FILE_ATTRIBUTE_NORMAL,
                None
            )

            # Write data to the file using WriteFile
            win32file.WriteFile(handle, data)

            # Close the file handle
            win32file.CloseHandle(handle)

            logger.info("Database successfully written to file")
            return True
        except Exception as e:
            logger.error(f"Failed to write database to file: {e}")
            return False

    def load_file(self):
        """
        Reads a database from a file in pickle format using WinAPI and loads it into the current database.
        :return: True if the operation is successful, False otherwise.
        """
        try:
            # Open the file using WinAPI CreateFile
            handle = win32file.CreateFile(
                self.filename,
                win32con.GENERIC_READ,
                0,  # No sharing
                None,
                win32con.OPEN_EXISTING,  # Open only if the file exists
                win32con.FILE_ATTRIBUTE_NORMAL,
                None
            )

            # Read data from the file using ReadFile
            _, data = win32file.ReadFile(handle, win32file.GetFileSize(handle))

            # Close the file handle
            win32file.CloseHandle(handle)

            # Deserialize the data
            self.db = pickle.loads(data)

            logger.info("Database successfully loaded")
            return True
        except Exception as e:
            logger.error(f"Failed to read database from file '{self.filename}': {e}")
            return False


def assert_file_db():
    """
    Make sure the FileDB class works fine.
    :return:
    """
    file_db = FileDB("assertions_test.pickle", {"test": 1, "another value": 0})
    file_db.dump_file()
    assert file_db.get_value("test") == 1

    file_db.delete_value("test")
    file_db.dump_file()

    file_db_2 = FileDB("assertions_test.pickle")
    file_db_2.load_file()
    assert not file_db_2.get_value("test")


if __name__ == "__main__":
    assert_file_db()
