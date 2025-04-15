from dataclasses import dataclass
from typing import Optional, List


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
