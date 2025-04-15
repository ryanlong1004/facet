"""
gateway.py

This module provides the `PickleCRUD` class for managing `FaceData` records stored in a pickle file.
It includes methods for creating, reading, updating, deleting, and importing metadata, as well as
utility functions for working with JSON files and logging.

Classes:
    PickleCRUD: A class for performing CRUD operations on a pickle file containing `FaceData` records.
"""

import json
import os
import pickle
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional

from models import FaceData, BoundingBox, Attributes
from dotenv import load_dotenv

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


class PickleCRUD:
    """
    A class for performing CRUD operations on a pickle file containing `FaceData` records.

    Attributes:
        pickle_file_path (str): The path to the pickle file.
    """

    def __init__(self, pickle_file_path: str):
        """
        Initialize the PickleCRUD instance and ensure the pickle file is initialized.

        Args:
            pickle_file_path (str): The path to the pickle file.
        """
        self.pickle_file_path = pickle_file_path
        self._initialize_pickle_file()

    def _initialize_pickle_file(self):
        """
        Ensure the pickle file exists and is initialized as an empty dictionary if it does not exist.
        """
        if not os.path.exists(self.pickle_file_path):
            logger.info("Initializing pickle file at %s", self.pickle_file_path)
            self._write_pickle({})

    def create(self, key: str, value: FaceData):
        """
        Create a new record in the pickle file.

        Args:
            key (str): The unique key for the record.
            value (FaceData): The `FaceData` object to store.

        Raises:
            KeyError: If the key already exists in the pickle file.
        """
        data = self._read_pickle()
        if key in data:
            logger.error("Key '%s' already exists. Cannot create record.", key)
            raise KeyError(f"Key '{key}' already exists.")
        data[key] = value
        self._write_pickle(data)
        logger.info("Record with key '%s' created successfully.", key)

    def read(self, key: str) -> Optional[FaceData]:
        """
        Read a record by key.

        Args:
            key (str): The unique key for the record.

        Returns:
            Optional[FaceData]: The `FaceData` object if found, otherwise None.
        """
        data = self._read_pickle()
        if key in data:
            logger.info("Record with key '%s' retrieved successfully.", key)
            return data[key]
        logger.warning("Record with key '%s' not found.", key)
        return None

    def read_all(self) -> Dict[str, FaceData]:
        """
        Read all records from the pickle file.

        Returns:
            Dict[str, FaceData]: A dictionary of all `FaceData` records.

        Raises:
            ValueError: If the pickle file does not contain a valid dictionary.
        """
        logger.info("Retrieving all records from the pickle file.")
        data = self._read_pickle()
        if not isinstance(data, dict):
            logger.error("Pickle file does not contain a valid dictionary.")
            raise ValueError("Pickle file does not contain a valid dictionary.")
        return data

    def update(self, key: str, value: FaceData):
        """
        Update an existing record in the pickle file.

        Args:
            key (str): The unique key for the record.
            value (FaceData): The updated `FaceData` object.

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

    def delete(self, key: str):
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
        logger.info("Record with key '%s' deleted successfully.", key)

    def import_metadata(self, metadata_folder: str):
        """
        Import multiple JSON files as records into the pickle file.

        Args:
            metadata_folder (str): The folder containing JSON files to import.
        """
        logger.info("Importing metadata from folder: %s", metadata_folder)
        data = self._read_pickle()
        json_files = self._get_json_files(metadata_folder)
        for file_path in json_files:
            try:
                json_data = self._load_json(file_path)
                face_id = json_data.get("face_id")
                if face_id:
                    face_data = self._convert_to_facedata(json_data)
                    data[face_id] = face_data
                    logger.info(
                        "Imported record with face_id '%s' from %s", face_id, file_path
                    )
            except Exception as e:
                logger.error("Failed to import file %s: %s", file_path, e)
        self._write_pickle(data)
        logger.info("Metadata import completed successfully.")

    def find_by_group_id(self, group_id: int) -> List[FaceData]:
        """
        Find all `FaceData` records with the specified group_id.

        Args:
            group_id (int): The group ID to search for.

        Returns:
            List[FaceData]: A list of `FaceData` objects with the specified group_id.
        """
        logger.info("Looking up records with group_id: %s", group_id)
        data = self._read_pickle()
        results = [
            face_data for face_data in data.values() if face_data.group_id == group_id
        ]
        logger.info("Found %d records with group_id: %s", len(results), group_id)
        return results

    def get_all_group_ids(self) -> List[int]:
        """
        Retrieve all unique group_ids from the pickle file.

        Returns:
            List[int]: A list of unique group IDs.
        """
        logger.info("Retrieving all unique group_ids from the pickle file.")
        data = self._read_pickle()
        group_ids = {
            face_data.group_id
            for face_data in data.values()
            if face_data.group_id is not None
        }
        logger.info("Found %d unique group_ids.", len(group_ids))
        return list(group_ids)

    def _read_pickle(self) -> Dict[str, FaceData]:
        """
        Read data from the pickle file.

        Returns:
            Dict[str, FaceData]: A dictionary of `FaceData` records.

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

    def _write_pickle(self, data: Dict[str, FaceData]):
        """
        Write data to the pickle file.

        Args:
            data (Dict[str, FaceData]): The data to write to the pickle file.

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

    @staticmethod
    def _get_json_files(folder: str) -> List[str]:
        """
        Get a list of JSON file paths in a folder.

        Args:
            folder (str): The folder to search for JSON files.

        Returns:
            List[str]: A list of JSON file paths.

        Raises:
            Exception: If there is an error accessing the folder.
        """
        try:
            files = [
                os.path.join(folder, file)
                for file in os.listdir(folder)
                if file.endswith(".json")
            ]
            logger.debug("Found %d JSON files in folder: %s", len(files), folder)
            return files
        except Exception as e:
            logger.error("Error accessing folder %s: %s", folder, e)
            raise

    @staticmethod
    def _load_json(file_path: str) -> Dict[str, Any]:
        """
        Load JSON data from a file.

        Args:
            file_path (str): The path to the JSON file.

        Returns:
            Dict[str, Any]: The loaded JSON data.

        Raises:
            Exception: If there is an error loading the JSON file.
        """
        try:
            with open(file_path, "r") as f:
                logger.debug("Loading JSON file: %s", file_path)
                return json.load(f)
        except Exception as e:
            logger.error("Error loading JSON file %s: %s", file_path, e)
            raise

    @staticmethod
    def _convert_to_facedata(json_data: Dict[str, Any]) -> FaceData:
        """
        Convert JSON data to a `FaceData` instance.

        Args:
            json_data (Dict[str, Any]): The JSON data to convert.

        Returns:
            FaceData: The converted `FaceData` object.
        """
        bounding_box = BoundingBox(
            x=json_data["bounding_box"]["x"],
            y=json_data["bounding_box"]["y"],
            width=json_data["bounding_box"]["width"],
            height=json_data["bounding_box"]["height"],
        )
        attributes = Attributes(
            race=json_data["attributes"]["race"],
            race_confidence=json_data["attributes"]["race_confidence"],
        )
        return FaceData(
            face_id=json_data["face_id"],
            face_path=json_data["face_path"],
            image_path=json_data["image_path"],
            bounding_box=bounding_box,
            face_confidence=json_data["face_confidence"],
            attributes=attributes,
            group_id=json_data["group_id"],
            embedding=json_data["embedding"],
        )
