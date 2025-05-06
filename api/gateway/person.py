from typing import Any, Dict, List, Optional

import duckdb


class DuckDbGateway:
    """
    Gateway layer for interacting with the DuckDB database.
    Provides CRUD operations for the `persons` table.
    """

    def __init__(self, db_path: str):
        """
        Initialize the DuckDB gateway.

        Args:
            db_path (str): Path to the DuckDB database file.
        """
        self.db_path = db_path
        self.conn = None

    def __enter__(self):
        """
        Enter the runtime context and open the DuckDB connection.
        """
        self.conn = duckdb.connect(database=self.db_path, read_only=False)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context and close the DuckDB connection.
        """
        if self.conn:
            self.conn.close()

    def _ensure_connection(self):
        """
        Ensure the database connection is initialized.
        """
        if self.conn is None:
            raise RuntimeError(
                "Database connection is not initialized. Use the context manager or initialize the connection."
            )

    def _execute_query(
        self, query: str, params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a query and return the results as a list of dictionaries.

        Args:
            query (str): The SQL query to execute.
            params (Optional[List[Any]]): The parameters for the query.

        Returns:
            List[Dict[str, Any]]: The query results as a list of dictionaries.
        """
        self._ensure_connection()
        cursor = self.conn.execute(query, params or [])  # type: ignore
        columns = [desc[0] for desc in cursor.description]  # type: ignore
        results = cursor.fetchall()
        return [dict(zip(columns, row)) for row in results]

    def _execute_query_single(
        self, query: str, params: Optional[List[Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Execute a query and return a single result as a dictionary.

        Args:
            query (str): The SQL query to execute.
            params (Optional[List[Any]]): The parameters for the query.

        Returns:
            Optional[Dict[str, Any]]: The query result as a dictionary, or None if no result is found.
        """
        self._ensure_connection()
        cursor = self.conn.execute(query, params or [])  # type: ignore
        columns = [desc[0] for desc in cursor.description]  # type: ignore
        result = cursor.fetchone()
        return dict(zip(columns, result)) if result else None

    def create_person(self, person_data: Dict[str, Any]) -> None:
        """
        Insert a new person record into the `persons` table.

        Args:
            person_data (Dict[str, Any]): A dictionary containing the person data.
        """
        query = """
        INSERT INTO persons (
            face_id, person_id, person_tag, image_name, face_path, thumbnail_path,
            metadata_path, confidence, blur_effect, bounding_box, attributes,
            embedding, user_updated
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self._ensure_connection()
        self.conn.execute(  # type: ignore
            query,
            [
                person_data["face_id"],
                person_data["person_id"],
                person_data["person_tag"],
                person_data["image_name"],
                person_data["face_path"],
                person_data["thumbnail_path"],
                person_data["metadata_path"],
                person_data["confidence"],
                person_data["blur_effect"],
                person_data["bounding_box"],
                person_data["attributes"],
                person_data["embedding"],
                person_data["user_updated"],
            ],
        )

    def get_person(self, person_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a person record by its ID.

        Args:
            person_id (str): The unique ID of the person.

        Returns:
            Optional[Dict[str, Any]]: The person record as a dictionary if found, otherwise None.
        """
        query = "SELECT * FROM persons WHERE person_id = ?"
        return self._execute_query_single(query, [person_id])

    def get_all_persons(self) -> List[Dict[str, Any]]:
        """
        Retrieve all person records from the `persons` table.

        Returns:
            List[Dict[str, Any]]: A list of all person records as dictionaries.
        """
        query = "SELECT * FROM persons"
        return self._execute_query(query)

    def update_person(self, person_id: str, updates: Dict[str, Any]) -> None:
        """
        Update an existing person record.

        Args:
            person_id (str): The unique ID of the person to update.
            updates (Dict[str, Any]): A dictionary containing the fields to update.
        """
        set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
        query = f"UPDATE persons SET {set_clause} WHERE person_id = ?"
        self._ensure_connection()
        self.conn.execute(query, list(updates.values()) + [person_id])  # type: ignore

    def delete_person(self, person_id: str) -> None:
        """
        Delete a person record by its ID.

        Args:
            person_id (str): The unique ID of the person to delete.
        """
        query = "DELETE FROM persons WHERE person_id = ?"
        self._ensure_connection()
        self.conn.execute(query, [person_id])  # type: ignore
