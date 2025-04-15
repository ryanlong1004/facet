from dataclasses import dataclass
import os
import json
import pickle
from typing import List, Optional, Dict, Any


@dataclass
class FaceData:
    face_id: str
    face_path: str
    image_path: str

    # Bounding box details
    bounding_box_x: int
    bounding_box_y: int
    bounding_box_width: int
    bounding_box_height: int

    face_confidence: float

    # Attributes
    race: str
    race_confidence: float

    group_id: Optional[str]
    embedding: List[float]


class PickleCRUD:
    def __init__(self, pickle_file_path: str):
        self.pickle_file_path = pickle_file_path
        self._initialize_pickle_file()

    def _initialize_pickle_file(self):
        """Ensure the pickle file exists and is initialized."""
        if not os.path.exists(self.pickle_file_path):
            self._write_pickle({})

    def create(self, key: str, value: FaceData):
        """Create a new record in the pickle file."""
        data = self._read_pickle()
        if key in data:
            raise KeyError(f"Key '{key}' already exists.")
        data[key] = value
        self._write_pickle(data)

    def read(self, key: str) -> Optional[FaceData]:
        """Read a record by key."""
        return self._read_pickle().get(key)

    def read_all(self) -> Dict[str, FaceData]:
        """Read all records."""
        return self._read_pickle()

    def update(self, key: str, value: FaceData):
        """Update an existing record."""
        data = self._read_pickle()
        if key not in data:
            raise KeyError(f"Key '{key}' does not exist.")
        data[key] = value
        self._write_pickle(data)

    def delete(self, key: str):
        """Delete a record by key."""
        data = self._read_pickle()
        if key not in data:
            raise KeyError(f"Key '{key}' does not exist.")
        del data[key]
        self._write_pickle(data)

    def import_metadata(self, metadata_folder: str):
        """Import multiple JSON files as records."""
        data = self._read_pickle()
        json_files = self._get_json_files(metadata_folder)
        for file_path in json_files:
            json_data = self._load_json(file_path)
            json_face_id = json_data.get("face_id")
            if json_face_id:
                face_data = self._convert_to_facedata(json_data)
                data[json_face_id] = face_data
        self._write_pickle(data)

    def _read_pickle(self) -> Dict[str, FaceData]:
        """Read data from the pickle file."""
        with open(self.pickle_file_path, "rb") as f:
            return pickle.load(f)

    def _write_pickle(self, data: Dict[str, FaceData]):
        """Write data to the pickle file."""
        with open(self.pickle_file_path, "wb") as f:
            pickle.dump(data, f)

    @staticmethod
    def _get_json_files(folder: str) -> list:
        """Get a list of JSON file paths in a folder."""
        return [
            os.path.join(folder, file)
            for file in os.listdir(folder)
            if file.endswith(".json")
        ]

    @staticmethod
    def _load_json(file_path: str) -> Dict[str, Any]:
        """Load JSON data from a file."""
        with open(file_path, "r") as f:
            return json.load(f)

    @staticmethod
    def _convert_to_facedata(json_data: Dict[str, Any]) -> FaceData:
        """Convert JSON data to a FaceData instance."""
        bounding_box = json_data["bounding_box"]
        attributes = json_data["attributes"]
        return FaceData(
            face_id=json_data["face_id"],
            face_path=json_data["face_path"],
            image_path=json_data["image_path"],
            bounding_box_x=bounding_box["x"],
            bounding_box_y=bounding_box["y"],
            bounding_box_width=bounding_box["width"],
            bounding_box_height=bounding_box["height"],
            face_confidence=json_data["face_confidence"],
            race=attributes["race"],
            race_confidence=attributes["race_confidence"],
            group_id=json_data["group_id"],
            embedding=json_data["embedding"],
        )


if __name__ == "__main__":
    # Example usage
    pickle_file_path = "metadata.pkl"
    metadata_folder = "data/metadata"

    crud = PickleCRUD(pickle_file_path)

    # Import metadata from the folder
    crud.import_metadata(metadata_folder)

    # Read all records
    all_records = crud.read_all()
    print(f"All records: {list(all_records.keys())}")

    # Read a specific face_id
    face_id = "example_face_id"
    metadata = crud.read(face_id)
    if metadata:
        print(f"Metadata for {face_id}: {metadata}")
    else:
        print(f"No metadata found for {face_id}.")
