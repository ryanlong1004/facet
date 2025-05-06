"""
Defines the Person model for representing person records.
"""

from typing import Any, Dict

from pydantic import BaseModel


class Person(BaseModel):
    """
    Represents a face record.
    """

    face_id: str
    person_id: int
    person_tag: str | None
    image_name: str
    face_path: str
    thumbnail_path: str
    metadata_path: str
    confidence: float
    blur_effect: float
    bounding_box: Dict[str, Any]
    attributes: Dict[str, Any]
    embedding: str
    user_updated: bool

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Person":
        """
        Create a Person object from a dictionary.

        Args:
            data (Dict[str, Any]): The dictionary containing person data.

        Returns:
            Person: The Person object.
        """
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Person object to a dictionary.

        Returns:
            Dict[str, Any]: The dictionary representation of the Person object.
        """
        return {
            "face_id": self.face_id,
            "person_id": self.person_id,
            "person_tag": self.person_tag,
            "image_name": self.image_name,
            "face_path": self.face_path,
            "thumbnail_path": self.thumbnail_path,
            "metadata_path": self.metadata_path,
            "confidence": self.confidence,
            "blur_effect": self.blur_effect,
            "bounding_box": self.bounding_box,
            "attributes": self.attributes,
            "embedding": self.embedding,
            "user_updated": self.user_updated,
        }
