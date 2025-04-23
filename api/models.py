"""
models.py

This module defines the data models used in the application. These models represent the structure of the data
used for face detection and recognition.

Classes:
    BoundingBox: Represents the bounding box of a detected face.
    Attributes: Represents additional attributes of a detected face, such as race and confidence.
    FaceData: Represents the complete data for a detected face, including its embedding and metadata.
    PersonData: Represents the data for a person.
"""

from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv
from pydantic import BaseModel, validator
import json
import os


@dataclass
class BoundingBox:
    """
    Represents the bounding box of a detected face.

    Attributes:
        x (int): The x-coordinate of the top-left corner of the bounding box.
        y (int): The y-coordinate of the top-left corner of the bounding box.
        width (int): The width of the bounding box.
        height (int): The height of the bounding box.
    """

    x: float
    y: float
    width: float
    height: float


@dataclass
class Attributes:
    """
    Represents additional attributes of a detected face.

    Attributes:
        race (str): The predicted race of the person.
        race_confidence (float): The confidence level of the race prediction.
    """

    race: str
    race_confidence: float


class FaceData(BaseModel):
    """
    Represents the complete data for a detected face.

    Attributes:
        face_id (str): The unique identifier for the face.
        face_path (str): The file path to the cropped face image.
        image_path (str): The file path to the original image containing the face.
        bounding_box (BoundingBox): The bounding box of the detected face.
        face_confidence (float): The confidence level of the face detection.
        attributes (Attributes): Additional attributes of the detected face.
        group_id (Optional[int]): The group ID associated with the face (e.g., person ID).
        embedding (List[float]): The embedding vector representing the face.
        url (str): The URL to access the face image in the browser.
    """

    face_id: str
    url: Optional[str] = None
    face_path: str
    image_path: str
    bounding_box: BoundingBox
    face_confidence: float
    attributes: Attributes
    group_id: Optional[int]
    embedding: List[float]

    @validator("embedding", pre=True)
    def deserialize_embedding(cls, value):
        """
        Ensure the embedding field is deserialized into a list of floats.

        Args:
            value (Any): The value of the embedding field.

        Returns:
            List[float]: The deserialized embedding.
        """
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string for embedding")
        return value

    @validator("url", always=True, pre=True)
    def generate_url(cls, _, values):
        """
        Generate a URL for the face image.

        Args:
            values (dict): The values of the model fields.

        Returns:
            str: The URL to access the face image.
        """
        image_path = values.get("image_path")
        # Load environment variables from .env file
        load_dotenv()

        # Get HOST and PORT from environment variables
        host = os.getenv("HOST", "localhost")
        port = os.getenv("PORT", "8000")

        # Construct the URL
        return f"http://{host}:{port}{image_path.split('api')[1]}"


@dataclass
class PersonData:
    """
    Represents the data for a person.

    Attributes:
        person_id (int): The unique identifier for the person.
        person_name (str): The name of the person.
    """

    person_id: int
    person_name: str
