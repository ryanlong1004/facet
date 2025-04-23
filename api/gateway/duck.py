"""
duck.py

This module provides the `FaceDataHandler` class for managing `FaceData` records stored in a DuckDB database.
It includes methods for creating, reading, updating, deleting, and importing metadata, as well as
utility functions for working with JSON files and logging.

Classes:
    FaceDataHandler: A class for performing CRUD operations on a DuckDB database containing `FaceData` records.
"""

import json
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional

import duckdb
from dotenv import load_dotenv

from models import Attributes, BoundingBox, FaceData

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


class FaceDataHandler:
    """
    A class for performing CRUD operations on a DuckDB database containing `FaceData` records.

    Attributes:
        db_path (str): The path to the DuckDB database file.
    """

    def __init__(self, db_path: str):
        """
        Initialize the FaceDataHandler instance and ensure the database is initialized.

        Args:
            db_path (str): The path to the DuckDB database file.
        """
        self.db_path = db_path
        self.conn = duckdb.connect(database=db_path)
        self._initialize_database()

    def _initialize_database(self):
        """
        Ensure the DuckDB database is initialized with the required table.
        """
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS face_data (
                face_id TEXT PRIMARY KEY,
                face_path TEXT,
                image_path TEXT,
                bounding_box_x FLOAT,
                bounding_box_y FLOAT,
                bounding_box_width FLOAT,
                bounding_box_height FLOAT,
                face_confidence FLOAT,
                attributes_race TEXT,
                attributes_race_confidence FLOAT,
                group_id INTEGER,
                embedding BLOB
            )
        """)
        logger.info("Initialized DuckDB database at %s", self.db_path)

    def create(self, face_data: FaceData):
        """
        Create or update a record in the DuckDB database.

        Args:
            face_data (FaceData): The `FaceData` object to store.

        If a record with the same face_id already exists, it will be updated.
        """
        try:
            # Serialize the embedding as a JSON string
            embedding_serialized = json.dumps(face_data.embedding)

            # Use INSERT OR REPLACE to handle duplicate keys
            self.conn.execute(
                """
                INSERT OR REPLACE INTO face_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    face_data.face_id,
                    face_data.face_path,
                    face_data.image_path,
                    face_data.bounding_box.x,
                    face_data.bounding_box.y,
                    face_data.bounding_box.width,
                    face_data.bounding_box.height,
                    face_data.face_confidence,
                    face_data.attributes.race,
                    face_data.attributes.race_confidence,
                    face_data.group_id,
                    embedding_serialized,  # Store serialized embedding
                ),
            )
            logger.info(
                "Record with face_id '%s' created or updated successfully.",
                face_data.face_id,
            )
        except Exception as e:
            logger.error(
                "Error creating or updating record with face_id '%s': %s",
                face_data.face_id,
                e,
            )
            raise ValueError(f"Error creating or updating record: {e}")

    def read(self, face_id: str) -> Optional[FaceData]:
        """
        Read a record by face_id.

        Args:
            face_id (str): The unique face_id for the record.

        Returns:
            Optional[FaceData]: The `FaceData` object if found, otherwise None.
        """
        result = self.conn.execute(
            """
            SELECT * FROM face_data WHERE face_id = ?
        """,
            (face_id,),
        ).fetchone()

        if result:
            logger.info("Record with face_id '%s' retrieved successfully.", face_id)
            return self._convert_row_to_facedata(result)
        logger.warning("Record with face_id '%s' not found.", face_id)
        return None

    def read_all(self) -> List[FaceData]:
        """
        Read all records from the DuckDB database.

        Returns:
            List[FaceData]: A list of all `FaceData` records.
        """
        results = self.conn.execute("SELECT * FROM face_data").fetchall()
        logger.info("Retrieved %d records from the database.", len(results))
        return [self._convert_row_to_facedata(row) for row in results]

    def update(self, face_id: str, face_data: FaceData):
        """
        Update an existing record in the DuckDB database.

        Args:
            face_id (str): The unique face_id for the record.
            face_data (FaceData): The updated `FaceData` object.

        Raises:
            ValueError: If the record does not exist.
        """
        try:
            self.conn.execute(
                """
                UPDATE face_data
                SET face_path = ?, image_path = ?, bounding_box_x = ?, bounding_box_y = ?,
                    bounding_box_width = ?, bounding_box_height = ?, face_confidence = ?,
                    attributes_race = ?, attributes_race_confidence = ?, group_id = ?, embedding = ?
                WHERE face_id = ?
            """,
                (
                    face_data.face_path,
                    face_data.image_path,
                    face_data.bounding_box.x,
                    face_data.bounding_box.y,
                    face_data.bounding_box.width,
                    face_data.bounding_box.height,
                    face_data.face_confidence,
                    face_data.attributes.race,
                    face_data.attributes.race_confidence,
                    face_data.group_id,
                    face_data.embedding,
                    face_id,
                ),
            )
            logger.info("Record with face_id '%s' updated successfully.", face_id)
        except Exception as e:
            logger.error("Error updating record with face_id '%s': %s", face_id, e)
            raise ValueError(f"Error updating record: {e}")

    def delete(self, face_id: str):
        """
        Delete a record by face_id.

        Args:
            face_id (str): The unique face_id for the record.

        Raises:
            ValueError: If the record does not exist.
        """
        try:
            self.conn.execute("DELETE FROM face_data WHERE face_id = ?", (face_id,))
            logger.info("Record with face_id '%s' deleted successfully.", face_id)
        except Exception as e:
            logger.error("Error deleting record with face_id '%s': %s", face_id, e)
            raise ValueError(f"Error deleting record: {e}")

    def _convert_row_to_facedata(self, row: tuple) -> FaceData:
        """
        Convert a database row to a `FaceData` instance.

        Args:
            row (tuple): The database row.

        Returns:
            FaceData: The converted `FaceData` object.
        """
        bounding_box = BoundingBox(
            x=row[3],
            y=row[4],
            width=row[5],
            height=row[6],
        )
        attributes = Attributes(
            race=row[8],
            race_confidence=row[9],
        )
        # Deserialize the embedding from BLOB (JSON string)
        embedding_deserialized = (
            json.loads(row[11].decode("utf-8"))
            if isinstance(row[11], (bytes, str))
            else row[11]
        )

        return FaceData(
            face_id=row[0],
            face_path=row[1],
            image_path=row[2],
            bounding_box=bounding_box,
            face_confidence=row[7],
            attributes=attributes,
            group_id=row[10],
            embedding=embedding_deserialized,
        )

    def import_metadata(self, metadata_folder: str):
        """
        Import multiple JSON files as records into the DuckDB database.

        Args:
            metadata_folder (str): The folder containing JSON files to import.
        """
        logger.info("Importing metadata from folder: %s", metadata_folder)
        json_files = self._get_json_files(metadata_folder)
        for file_path in json_files:
            try:
                print(file_path)
                json_data = self._load_json(file_path)
                face_id = json_data.get("face_id")
                if face_id:
                    face_data = self._convert_to_facedata(json_data)
                    self.create(face_data)
                    logger.info(
                        "Imported record with face_id '%s' from %s", face_id, file_path
                    )
            except Exception as e:
                logger.error("Failed to import file %s: %s", file_path, e)
                exit()
                continue  # Skip this file and continue with the next one
        logger.info("Metadata import completed successfully.")

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

    def _load_json(self, file_path: str) -> Dict[str, Any]:
        """
        Load JSON data from a file and insert it into the DuckDB database.

        Args:
            file_path (str): The path to the JSON file.

        Returns:
            Dict[str, Any]: The JSON data as a dictionary.
        """
        try:
            with open(file_path, "r") as f:
                json_data = json.load(f)
                logger.debug("Loaded JSON data from file: %s", file_path)

            # Serialize the embedding as a JSON string
            json_data["embedding"] = json.dumps(json_data["embedding"])

            # Insert the JSON data into the database
            self.conn.execute(
                """
                INSERT OR REPLACE INTO face_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    json_data["face_id"],
                    json_data["face_path"],
                    json_data["image_path"],
                    json_data["bounding_box"]["x"],
                    json_data["bounding_box"]["y"],
                    json_data["bounding_box"]["width"],
                    json_data["bounding_box"]["height"],
                    json_data["face_confidence"],
                    json_data["attributes"]["race"],
                    json_data["attributes"]["race_confidence"],
                    json_data["group_id"],
                    json_data["embedding"],  # Serialized embedding
                ),
            )

            logger.info(
                "Inserted JSON data into DuckDB database from file: %s", file_path
            )
            return json_data
        except Exception as e:
            logger.error("Error loading JSON file %s: %s", file_path, e)
            raise

    def _convert_to_facedata(self, json_data: Dict[str, Any]) -> FaceData:
        """
        Convert a JSON dictionary to a `FaceData` object.

        Args:
            json_data (Dict[str, Any]): The JSON data representing a face.

        Returns:
            FaceData: The converted `FaceData` object.
        """
        try:
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
            face_data = FaceData(
                face_id=json_data["face_id"],
                face_path=json_data["face_path"],
                image_path=json_data["image_path"],
                bounding_box=bounding_box,
                face_confidence=json_data["face_confidence"],
                attributes=attributes,
                group_id=json_data.get("group_id"),
                embedding=json_data["embedding"],  # Keep embedding as-is for now
            )
            logger.debug("Converted JSON data to FaceData object: %s", face_data)
            return face_data
        except KeyError as e:
            logger.error("Missing key in JSON data: %s", e)
            raise ValueError(f"Missing key in JSON data: {e}")
        except Exception as e:
            logger.error("Error converting JSON to FaceData: %s", e)
            raise ValueError(f"Error converting JSON to FaceData: {e}")
