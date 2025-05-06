import os
import json
import logging
from collections import defaultdict
import duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def populate_persons_table(db_path: str, metadata_folder: str):
    """
    Populate the `persons` table in the DuckDB database using JSON metadata files.

    Args:
        db_path (str): The path to the DuckDB database file.
        metadata_folder (str): The folder containing JSON metadata files.
    """
    logger.info("Starting to populate the `persons` table.")
    logger.info("Connecting to DuckDB database at %s", db_path)

    # Connect to the DuckDB database
    conn = duckdb.connect(database=db_path)

    # Ensure the `persons` table exists
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS people (
                id TEXT PRIMARY KEY,
                name TEXT,
                face_ids TEXT -- Store the list of FaceIds as a JSON string
            )
        """)
        logger.info("Ensured `persons` table exists.")
    except Exception as e:
        logger.error("Error creating `persons` table: %s", e)
        raise

    # Dictionary to group face_ids by group_id
    persons_data = defaultdict(lambda: {"name": None, "face_ids": []})

    # Process each JSON file in the metadata folder
    logger.info("Processing JSON files in folder: %s", metadata_folder)
    for file_name in os.listdir(metadata_folder):
        if file_name.endswith(".json"):
            file_path = os.path.join(metadata_folder, file_name)
            logger.info("Processing file: %s", file_path)
            with open(file_path, "r") as f:
                try:
                    json_data = json.load(f)

                    # Extract group_id, face_id, and name
                    group_id = str(json_data.get("group_id"))
                    face_id = json_data.get("face_id")
                    image_path = json_data.get("image_path")
                    if not group_id or not face_id or not image_path:
                        logger.warning(
                            "Skipping file %s due to missing data.", file_name
                        )
                        continue

                    # Extract the name from the last directory in the image_path
                    name = os.path.basename(os.path.dirname(image_path))

                    # Add the face_id to the group_id entry
                    persons_data[group_id]["face_ids"].append(face_id)

                    # Set the name for the group_id (if not already set)
                    if persons_data[group_id]["name"] is None:
                        persons_data[group_id]["name"] = name

                except Exception as e:
                    logger.error("Error processing file %s: %s", file_name, e)

    # Insert data into the `persons` table
    logger.info("Inserting data into the `persons` table.")
    for group_id, data in persons_data.items():
        face_ids_json = json.dumps(data["face_ids"])  # Serialize face_ids as JSON
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO people (id, name, face_ids) VALUES (?, ?, ?)
            """,
                (group_id, data["name"], face_ids_json),
            )
            logger.info(
                "Inserted group_id '%s' with name '%s' and %d face_ids.",
                group_id,
                data["name"],
                len(data["face_ids"]),
            )
        except Exception as e:
            logger.error("Error inserting group_id '%s': %s", group_id, e)

    logger.info("Persons table populated successfully!")


def main():
    """
    Main function to execute the import process.
    """
    # Define the database path and metadata folder
    db_path = "../faces.duckdb"  # Update this path as needed
    metadata_folder = (
        "/Users/rlong/Sandbox/facet/api/data/metadata"  # Update this path as needed
    )

    # Execute the import process
    populate_persons_table(db_path, metadata_folder)


if __name__ == "__main__":
    main()
