from logging_utils import setup_logger

# Setup logger for file
logger = setup_logger('base_db')


class Database:
    def __init__(self, db=None):
        """
        Initializes the Database class.
        :param db: The initial dictionary to use as the database, defaults to an empty dictionary.
        """
        if db is None:
            db = {}
        self.db = db

    def set_value(self, key, val):
        """
        Sets the value for a given key in the database.
        :param key: The key to be added or updated in the database.
        :param val: The value to associate with the key.
        :return: True if the operation is successful, False otherwise.
        """
        try:
            self.db[key] = val
            logger.info(f"Set value for key '{key}' to '{val}'")
            return True
        except KeyError as e:
            logger.info(f"Failed to set value for key '{key}': key doesn't exist.")
            return False

    def get_value(self, key):
        """
        Retrieves the value associated with a given key from the database.
        :param key: The key whose value needs to be fetched.
        :return: The value associated with the key, or None if the key does not exist.
        """
        try:
            value = self.db[key]
            return value
        except KeyError:
            logger.info(f"Failed to retrieve value for key '{key}': key doesn't exist")
            return None

    def delete_value(self, key):
        """
        Deletes a key-value pair from the database.

        :param key: The key to be deleted from the database.
        :return: The value associated with the deleted key.
        :raises: KeyError if the key does not exist.
        """
        try:
            value = self.db.pop(key)
            logger.info(f"Deleted key '{key}' with value '{value}'")
            return value
        except KeyError:
            logger.error(f"Failed to delete key '{key}': Key does not exist.")
            raise KeyError(f"Key '{key}' not found in the database.")


def assert_base_db():
    db = Database({1: "1"})
    assert db.get_value(1) == "1"

    db.set_value(1, "2")
    assert db.get_value(1) == "2"

    db.delete_value(1)
    assert not db.get_value(1)


if __name__ == "__main__":
    assert_base_db()

