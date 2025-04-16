"""
people.py

This module provides the `PickleCRUD` class for managing `PersonData` records stored in a pickle file.
It includes methods for creating, reading, updating, deleting, and importing metadata, as well as
utility functions for working with JSON files and logging.

Classes:
    PickleCRUD: A class for performing CRUD operations on a pickle file containing `PersonData` records.
"""

import json
import logging
import os
import pickle
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from models import PersonData

# Configure logging
load_dotenv()  # Load environment variables from .env file

LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "application.log")

rotating_handler = RotatingFileHandler(
    LOG_FILE_PATH,
    maxBytes=50 * 1024 * 1024,
    backupCount=10,  # 50 MB, keep last 10 files
)

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "DEBUG").upper(), logging.DEBUG),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        rotating_handler,  # Write logs to a rotating file
        logging.StreamHandler(),  # Also output logs to the console
    ],
)

logger = logging.getLogger(__name__)


class PersonDataHandler:
    """
    A class for performing CRUD operations on a pickle file containing `PersonData` records.

    Attributes:
        PEOPLE_PICKLE_FILE_PATH (str): The path to the pickle file.
    """

    def __init__(self, people_pickle_file_path: str):
        """
        Initialize the PickleCRUD instance and ensure the pickle file is initialized.

        Args:
            people_pickle_file_path (str): The path to the pickle file.
        """
        self.pickle_file_path = people_pickle_file_path
        self._initialize_pickle_file()

    def _initialize_pickle_file(self):
        """
        Ensure the pickle file exists and is initialized as an empty dictionary if it does not exist.
        """
        if not os.path.exists(self.pickle_file_path):
            logger.info("Initializing pickle file at %s", self.pickle_file_path)
            self._write_pickle({})

    def create(self, value: str) -> PersonData:
        """
        Create a new record in the pickle file.

        Args:
            key (str): The unique key for the record.
            value (PersonData): The `PersonData` object to store.

        Raises:
            KeyError: If the key already exists in the pickle file.
        """
        key = self.get_next_key()
        data = self._read_pickle()
        data[key] = PersonData(person_id=key, person_name=value)
        self._write_pickle(data)
        logger.info("Record with key '%s' created successfully.", key)
        return data[key]

    def read(self, key: int) -> Optional[PersonData]:
        """
        Read a record by key.

        Args:
            key (str): The unique key for the record.

        Returns:
            Optional[PersonData]: The `PersonData` object if found, otherwise None.
        """
        data = self._read_pickle()
        if key in data:
            logger.info("Record with key '%s' retrieved successfully.", key)
            return data[key]
        logger.warning("Record with key '%s' not found.", key)
        return None

    def read_all(self) -> Dict[Any, PersonData]:
        """
        Read all records from the pickle file.

        Returns:
            Dict[str, PersonData]: A dictionary of all `PersonData` records.

        Raises:
            ValueError: If the pickle file does not contain a valid dictionary.
        """
        logger.info("Retrieving all records from the pickle file.")
        data = self._read_pickle()
        if not isinstance(data, dict):
            logger.error("Pickle file does not contain a valid dictionary.")
            raise ValueError("Pickle file does not contain a valid dictionary.")
        return data

    def update(self, key: int, value: PersonData):
        """
        Update an existing record in the pickle file.

        Args:
            key (str): The unique key for the record.
            value (PersonData): The updated `PersonData` object.

        Raises:
            KeyError: If the key does not exist in the pickle file.
        """
        data = self._read_pickle()
        if key not in data:
            logger.error("Key '%s' does not exist. Cannot update record.", key)
            raise KeyError(f"Key '{key}' does not exist.")
        data[key] = value
        self._write_pickle(data)
        logger.info("Record with key '%s' updated successfully.", key)

    def delete(self, key: int):
        """
        Delete a record by key.

        Args:
            key (str): The unique key for the record.

        Raises:
            KeyError: If the key does not exist in the pickle file.
        """
        data = self._read_pickle()
        if key not in data:
            logger.error("Key '%s' does not exist. Cannot delete record.", key)
            raise KeyError(f"Key '{key}' does not exist.")
        del data[key]
        self._write_pickle(data)
        data = self._read_pickle()  # Reload the data to ensure consistency
        logger.info("Record with key '%s' deleted successfully.", key)

    def _read_pickle(self) -> Dict[int, PersonData]:
        """
        Read data from the pickle file.

        Returns:
            Dict[str, PersonData]: A dictionary of `PersonData` records.

        Raises:
            FileNotFoundError: If the pickle file does not exist.
            Exception: If there is an error reading the pickle file.
        """
        try:
            with open(self.pickle_file_path, "rb") as f:
                logger.debug("Reading data from pickle file: %s", self.pickle_file_path)
                return pickle.load(f)
        except FileNotFoundError:
            logger.error("Pickle file not found: %s", self.pickle_file_path)
            raise
        except Exception as e:
            logger.error("Error reading pickle file: %s", e)
            raise

    def _write_pickle(self, data: Dict[int, PersonData]):
        """
        Write data to the pickle file.

        Args:
            data (Dict[str, PersonData]): The data to write to the pickle file.

        Raises:
            Exception: If there is an error writing to the pickle file.
        """
        try:
            with open(self.pickle_file_path, "wb") as f:
                pickle.dump(data, f)
                logger.debug("Data written to pickle file: %s", self.pickle_file_path)
        except Exception as e:
            logger.error("Error writing to pickle file: %s", e)
            raise

    def generate_people_data(self, ids: List[Any], names_file_path):
        """
        Import multiple JSON files as records into the pickle file.

        Args:
            metadata_folder (str): The folder containing JSON files to import.
        """
        logger.info("Importing metadata from folder: %s", names_file_path)
        data = self._read_pickle()
        with open(names_file_path, "r", encoding="utf-8") as f:
            for _id in ids:
                data[_id] = PersonData(
                    person_id=int(_id), person_name=f.readline().strip()
                )
        self._write_pickle(data)
        logger.info("People data extracted and generated successfully.")

    def get_next_key(self) -> int:
        """
        Fetch the next incremental key from the pickle file.

        Returns:
            int: The next incremental key.
        """
        data = self._read_pickle()
        if not data:
            logger.info("Pickle file is empty. Starting with key 0.")
            return 0
        max_key = max(int(key) for key in data.keys())
        next_key = max_key + 1
        logger.info("Next incremental key is %d.", next_key)
        return next_key


if __name__ == "__main__":
    # Example usage
    handler = PersonDataHandler("people.pkl")
    person = handler.create("John Doe")
    print(handler.read(person.person_id))
    handler.update(
        person.person_id, PersonData(person_id=person.person_id, person_name="Jane Doe")
    )
    print(handler.read_all())
    handler.delete(person.person_id)
    print(handler.read_all())
