"""
Defines the Person model for representing person records.
"""

from typing import Any, Dict, Optional
from pydantic import BaseModel


class Person(BaseModel):
    """
    Represents a person record.
    """

    face_id: str
    person_id: int
    person_tag: Optional[Any]
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
        return self.model_dump()
