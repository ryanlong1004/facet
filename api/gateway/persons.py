import duckdb
import json
import logging
from typing import Any, Dict, List, Optional

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PersonDataHandler:
    """
    A class for performing CRUD operations on a DuckDB database containing `Person` records.

    Attributes:
        db_path (str): The path to the DuckDB database file.
    """

    def __init__(self, db_path: str):
        """
        Initialize the PersonDataHandler instance and ensure the database is initialized.

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
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS people (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    face_ids TEXT -- Store the list of FaceIds as a JSON string
                )
            """)
            logger.info("Initialized DuckDB database for people at %s", self.db_path)
        except Exception as e:
            logger.error("Error initializing database: %s", e)
            raise

    def create(self, person_id: str, name: str, face_ids: List[str]):
        """
        Create a new record in the DuckDB database.

        Args:
            person_id (str): The unique ID for the person.
            name (str): The name of the person.
            face_ids (List[str]): A list of associated Face IDs.

        Raises:
            ValueError: If a record with the same person_id already exists.
        """
        try:
            face_ids_json = json.dumps(face_ids)  # Serialize the list of FaceIds
            self.conn.execute(
                """
                INSERT INTO people (id, name, face_ids) VALUES (?, ?, ?)
            """,
                (person_id, name, face_ids_json),
            )
            logger.info("Record with person_id '%s' created successfully.", person_id)
        except Exception as e:
            if "UNIQUE constraint failed" in str(e):
                logger.error("Record with person_id '%s' already exists.", person_id)
                raise ValueError(f"Record with person_id '{person_id}' already exists.")
            else:
                logger.error(
                    "Error creating record with person_id '%s': %s", person_id, e
                )
                raise ValueError(f"Error creating record: {e}")
        except Exception as e:
            logger.error("Error creating record with person_id '%s': %s", person_id, e)
            raise ValueError(f"Error creating record: {e}")

    def read(self, person_id: str) -> Optional[Dict[str, Any]]:
        """
        Read a record by person_id.

        Args:
            person_id (str): The unique ID for the person.

        Returns:
            Optional[Dict[str, Any]]: The person data if found, otherwise None.
        """
        try:
            result = self.conn.execute(
                """
                SELECT * FROM people WHERE id = ?
            """,
                (person_id,),
            ).fetchone()

            if result:
                logger.info(
                    "Record with person_id '%s' retrieved successfully.", person_id
                )
                return {
                    "id": result[0],
                    "name": result[1],
                    "face_ids": json.loads(
                        result[2]
                    ),  # Deserialize the FaceIds JSON string
                }
            logger.warning("Record with person_id '%s' not found.", person_id)
            return None
        except Exception as e:
            logger.error("Error reading record with person_id '%s': %s", person_id, e)
            raise ValueError(f"Error reading record: {e}")

    def read_all(self) -> List[Dict[str, Any]]:
        """
        Read all records from the DuckDB database.

        Returns:
            List[Dict[str, Any]]: A list of all person records.
        """
        try:
            results = self.conn.execute("SELECT * FROM people").fetchall()
            logger.info("Retrieved %d records from the database.", len(results))
            return [
                {
                    "id": row[0],
                    "name": row[1],
                    "face_ids": json.loads(
                        row[2]
                    ),  # Deserialize the FaceIds JSON string
                }
                for row in results
            ]
        except Exception as e:
            logger.error("Error reading all records: %s", e)
            raise ValueError(f"Error reading all records: {e}")

    def read_by_name(self, name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all persons filtered by name.

        Args:
            name (str): The name to filter persons by.

        Returns:
            List[Dict[str, Any]]: A list of persons matching the given name.
        """
        try:
            # Query the database for persons with the specified name
            results = self.conn.execute(
                """
                SELECT * FROM people WHERE name LIKE ?
                """,
                (f"%{name}%",),
            ).fetchall()

            logger.info("Retrieved %d records matching name '%s'.", len(results), name)

            # Deserialize the face_ids JSON string and return the results
            return [
                {
                    "id": row[0],
                    "name": row[1],
                    "face_ids": json.loads(
                        row[2]
                    ),  # Deserialize the FaceIds JSON string
                }
                for row in results
            ]
        except Exception as e:
            logger.error("Error retrieving records by name '%s': %s", name, e)
            raise ValueError(f"Error retrieving records by name: {e}")

    def update(
        self,
        person_id: str,
        name: Optional[str] = None,
        face_ids: Optional[List[str]] = None,
    ):
        """
        Update an existing record in the DuckDB database.

        Args:
            person_id (str): The unique ID for the person.
            name (Optional[str]): The updated name of the person.
            face_ids (Optional[List[str]]): The updated list of Face IDs.

        Raises:
            ValueError: If the record does not exist.
        """
        try:
            # Fetch the existing record
            existing_record = self.read(person_id)
            if not existing_record:
                raise ValueError(f"Record with person_id '{person_id}' does not exist.")

            # Update fields if provided
            updated_name = name if name is not None else existing_record["name"]
            updated_face_ids = (
                face_ids if face_ids is not None else existing_record["face_ids"]
            )
            face_ids_json = json.dumps(
                updated_face_ids
            )  # Serialize the updated FaceIds

            self.conn.execute(
                """
                UPDATE people
                SET name = ?, face_ids = ?
                WHERE id = ?
            """,
                (updated_name, face_ids_json, person_id),
            )
            logger.info("Record with person_id '%s' updated successfully.", person_id)
        except Exception as e:
            logger.error("Error updating record with person_id '%s': %s", person_id, e)
            raise ValueError(f"Error updating record: {e}")

    def delete(self, person_id: str):
        """
        Delete a record by person_id.

        Args:
            person_id (str): The unique ID for the person.

        Raises:
            ValueError: If the record does not exist.
        """
        try:
            self.conn.execute("DELETE FROM people WHERE id = ?", (person_id,))
            logger.info("Record with person_id '%s' deleted successfully.", person_id)
        except Exception as e:
            logger.error("Error deleting record with person_id '%s': %s", person_id, e)
            raise ValueError(f"Error deleting record: {e}")
