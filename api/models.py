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

    x: int
    y: int
    width: int
    height: int


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


@dataclass
class FaceData:
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
    """

    face_id: str
    face_path: str
    image_path: str
    bounding_box: BoundingBox
    face_confidence: float
    attributes: Attributes
    group_id: Optional[int]
    embedding: List[float]


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
