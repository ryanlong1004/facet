"""
api.py

This module provides the FastAPI application for managing face data. It includes endpoints for clients, faces,
people, and health checks. The API uses a pickle file to store and retrieve face data.

Modules:
    - Clients: Manage client-related operations.
    - Faces: Manage face-related operations.
    - People: Manage person-related operations.
    - Health: Check the health of the API.

Classes:
    CreateClient: Request model for creating a client.
    Success: Response model for successful operations.
    Error: Response model for errors.
    DeletedFaces: Response model for deleted faces.
"""

import os
from typing import List, Dict

import uvicorn
from dotenv import load_dotenv
from fastapi import APIRouter, FastAPI, HTTPException, Path, Query
from pydantic import BaseModel

from gateway import PickleCRUD
from models import FaceData

# Load environment variables from .env file
load_dotenv()

# Fetch the Pickle file path from the .env file
PICKLE_FILE_PATH = os.getenv("PICKLE_FILE_PATH", "metadata.pkl")

# Initialize FastAPI app
app = FastAPI(
    title=os.getenv("API_TITLE", "Faces API"),
    description=os.getenv(
        "API_DESCRIPTION",
        "Face detection and recognition API. Also provides blurriness, emotion, and gender operations.",
    ),
    version=os.getenv("API_VERSION", "0.1"),
)

# Initialize API routers
clients_router = APIRouter(prefix="/clients", tags=["Clients"])
faces_router = APIRouter(prefix="/faces", tags=["Faces"])
people_router = APIRouter(prefix="/people", tags=["People"])
health_router = APIRouter(prefix="/health", tags=["Health"])


# Request and Response Models
class CreateClient(BaseModel):
    """
    Request model for creating a client.

    Attributes:
        client_id (str): The unique ID of the client.
    """

    client_id: str


class Success(BaseModel):
    """
    Response model for successful operations.

    Attributes:
        client_id (str): The unique ID of the client.
        processing_time (float): The time taken to process the request.
    """

    client_id: str
    processing_time: float


class Error(BaseModel):
    """
    Response model for errors.

    Attributes:
        message (str): The error message.
    """

    message: str


class DeletedFaces(BaseModel):
    """
    Response model for deleted faces.

    Attributes:
        deletions (int): The number of faces deleted.
        processing_time (float): The time taken to process the deletion.
    """

    deletions: int
    processing_time: float


# Clients Endpoints
@clients_router.post("/", response_model=Success, responses={400: {"model": Error}})
async def create_client(client: CreateClient):
    """
    Create a new client.

    Args:
        client (CreateClient): The client data.

    Returns:
        Success: The response containing the client ID and processing time.
    """
    return {"client_id": client.client_id, "processing_time": 0.1}


@clients_router.delete(
    "/{client_id}", response_model=Success, responses={400: {"model": Error}}
)
async def delete_client(client_id: str = Path(...)):
    """
    Delete an existing client.

    Args:
        client_id (str): The unique ID of the client.

    Returns:
        Success: The response containing the client ID and processing time.
    """
    return {"client_id": client_id, "processing_time": 0.1}


# Faces Endpoints
@faces_router.get(
    "/{face_id}", response_model=FaceData, responses={404: {"model": Error}}
)
async def get_face(face_id: str = Path(...)):
    """
    Retrieve a face by its ID.

    Args:
        face_id (str): The unique ID of the face.

    Returns:
        FaceData: The face data.
    """
    try:
        face_data = PickleCRUD(PICKLE_FILE_PATH).read(face_id)
        if face_data is None:
            raise HTTPException(status_code=404, detail="Face not found")
        return face_data
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@faces_router.get("/", response_model=List[FaceData], responses={400: {"model": Error}})
async def get_all_faces(
    page_number: int = Query(1, ge=1), page_length: int = Query(10, ge=1)
):
    """
    Retrieve all faces with pagination.

    Args:
        page_number (int): The page number (default is 1).
        page_length (int): The number of records per page (default is 10).

    Returns:
        List[FaceData]: A list of face data.
    """
    try:
        faces = PickleCRUD(PICKLE_FILE_PATH).read_all()
        faces_list = list(faces.values())

        # Calculate offset based on page_number and page_length
        offset = (page_number - 1) * page_length
        paginated_faces = faces_list[offset : offset + page_length]

        return paginated_faces
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@faces_router.delete(
    "/{face_id}", response_model=DeletedFaces, responses={400: {"model": Error}}
)
async def delete_face(face_id: str = Path(...)):
    """
    Delete a face by its ID.

    Args:
        face_id (str): The unique ID of the face.

    Returns:
        DeletedFaces: The response containing the number of deletions and processing time.
    """
    try:
        PickleCRUD(PICKLE_FILE_PATH).delete(face_id)
        return {"deletions": 1, "processing_time": 0.1}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


# People Endpoints
@people_router.get(
    "/", response_model=Dict[str, List[int]], responses={400: {"model": Error}}
)
async def get_person_ids(
    page_number: int = Query(1, ge=1), page_length: int = Query(10, ge=1)
):
    """
    Retrieve all person IDs with pagination.

    Args:
        page_number (int): The page number (default is 1).
        page_length (int): The number of records per page (default is 10).

    Returns:
        Dict[str, List[int]]: A dictionary containing the paginated group IDs.
    """
    try:
        group_ids = PickleCRUD(PICKLE_FILE_PATH).get_all_group_ids()
        # Calculate offset based on page_number and page_length
        offset = (page_number - 1) * page_length
        paginated_group_ids = group_ids[offset : offset + page_length]
        return {"group_ids": paginated_group_ids}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@people_router.get(
    "/{person_id}",
    response_model=Dict[str, List[FaceData]],
    responses={404: {"model": Error}},
)
async def get_person_faces(person_id: int = Path(...)):
    """
    Retrieve all faces associated with a person ID.

    Args:
        person_id (int): The unique ID of the person.

    Returns:
        Dict[str, List[FaceData]]: A dictionary containing the person ID and associated faces.
    """
    try:
        faces = PickleCRUD(PICKLE_FILE_PATH).find_by_group_id(person_id)
        return {"person_id": person_id, "faces": faces}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


# Health Check Endpoint
@health_router.get("/healthz", response_model=Dict[str, str])
async def health_check():
    """
    Check the health of the API.

    Returns:
        Dict[str, str]: A dictionary containing the health status.
    """
    try:
        PickleCRUD(PICKLE_FILE_PATH).read_all()  # Test if the pickle file is accessible
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")


# Include routers in the main app
app.include_router(clients_router)
app.include_router(faces_router)
app.include_router(people_router)
app.include_router(health_router)


def main():
    """
    Main entry point for the application.
    """
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
