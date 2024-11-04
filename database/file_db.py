from database.base_db import Database
import pickle
from logging_utils import setup_logger

# Setup logger for file
logger = setup_logger('file_db')


class FileDB(Database):

    def __init__(self, filename, database=None):
        """
        initiate a database which can write to file
        :param filename: the name of the file where the data will be saved.
        :param database: optional, will run over the old database and create a new one.
        """
        super().__init__(database)
        self.filename = filename
        if database is not None:
            self.dump_file()

    def dump_file(self):
        """
        Serializes and writes the object to the current database.
        :return: True if the operation is successful, False otherwise.
        """
        try:
            with open(self.filename, 'wb') as file:
                pickle.dump(self.db, file)
            logger.info(f"Database successfully written to file")
            return True
        except Exception as e:
            logger.error(f"Failed to write database to file : {e}")
            return False

    def load_file(self):
        """
        Reads a database from a file in pickle format and loads it into the current database.
        :return: True if the operation is successful, False otherwise.
        """
        try:
            with open(self.filename, 'rb') as f:
                self.db = pickle.load(f)
            logger.info(f"Database successfully loaded")
            return True
        except FileNotFoundError:
            logger.error(f"File '{self.filename}' not found.")
            return False
        except Exception as e:
            logger.error(f"Failed to read database from file '{self.filename}': {e}")
            return False


def assert_file_db():
    """
    make sure the file db class works fine
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
