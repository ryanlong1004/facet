"""
api.py

This module provides the FastAPI application for managing person data.
"""

import logging
import os
from typing import List, Any

import uvicorn
from dotenv import load_dotenv
from fastapi import APIRouter, FastAPI, HTTPException

from gateway.person import DuckDbGateway
from model.person import Person

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Fetch the DuckDB database file path from the .env file
DB_PATH = os.getenv("DB_PATH", "faces.duckdb")

# Initialize FastAPI app
app = FastAPI(
    title="Person API",
    description="API for managing person data.",
    version="1.0.0",
)

# Initialize API router
persons_router = APIRouter(prefix="/persons", tags=["Persons"])


def handle_gateway_operation(operation, *args, model=None, **kwargs):
    """
    Helper function to handle DuckDbGateway operations with centralized exception handling.

    Args:
        operation (Callable): The DuckDbGateway method to call.
        *args: Positional arguments for the operation.
        model (Optional[Type[BaseModel]]): The Pydantic model to convert the result to.
        **kwargs: Keyword arguments for the operation.

    Returns:
        Any: The result of the operation, optionally converted to a Pydantic model.
    """
    try:
        with DuckDbGateway(DB_PATH) as gateway:
            result = operation(gateway, *args, **kwargs)
            if model and result:
                if isinstance(result, list):
                    return [model.from_dict(item) for item in result]
                return model.from_dict(result)
            return result
    except Exception as e:
        logger.error("Error during gateway operation: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error") from e


# CRUD Endpoints for Persons
@persons_router.get("/", response_model=List[Person])
async def get_all_persons():
    """
    Retrieve all persons.
    """
    return handle_gateway_operation(
        lambda gateway: gateway.get_all_persons(), model=Person
    )


@persons_router.get("/{person_id}", response_model=Person)
async def get_person(person_id: str):
    """
    Retrieve a person by their ID.
    """
    person = handle_gateway_operation(
        lambda gateway: gateway.get_person(person_id), model=Person
    )
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    return person


@persons_router.post("/", response_model=Person)
async def create_person(person: Person):
    """
    Create a new person.
    """
    handle_gateway_operation(lambda gateway: gateway.create_person(person.to_dict()))
    return person


@persons_router.put("/{person_id}", response_model=Person)
async def update_person(person_id: str, person: Person):
    """
    Update an existing person.
    """
    handle_gateway_operation(
        lambda gateway: gateway.update_person(person_id, person.to_dict())
    )
    return person


@persons_router.delete("/{person_id}")
async def delete_person(person_id: str):
    """
    Delete a person by their ID.
    """
    handle_gateway_operation(lambda gateway: gateway.delete_person(person_id))
    return {"message": f"Person with ID {person_id} deleted successfully."}


@persons_router.get("/{id}/faces", response_model=List[Any], deprecated=True)
async def list_faces_for_person(id: str):
    """
    List faces associated with a person.

    Args:
        id (str): The unique ID of the person.

    Returns:
        List[Any]: A list of faces associated with the person.
    """
    # Stub implementation
    return [{"face_id": "face_001", "image_name": "image_001.jpg"}]


@persons_router.get("/{id}/history", response_model=List[Any])
async def get_person_version_history(id: str):
    """
    Get version history for a person.

    Args:
        id (str): The unique ID of the person.

    Returns:
        List[Any]: A list of version history records for the person.
    """
    # Stub implementation
    return [{"version": 1, "updated_at": "2023-01-01T12:00:00Z"}]


@persons_router.get("/matches/{id}/persons", response_model=List[Any])
async def list_persons_in_match(id: str):
    """
    List persons in a match.

    Args:
        id (str): The unique ID of the match.

    Returns:
        List[Any]: A list of persons in the match.
    """
    # Stub implementation
    return [{"person_id": "person_001", "name": "John Doe"}]


# Include the router in the main app
app.include_router(persons_router)


def main():
    """
    Entry point for running the application.

    This function retrieves the host and port configuration from environment
    variables, with default values of "0.0.0.0" for the host and 8000 for the port.
    It then starts the application using Uvicorn.

    Environment Variables:
        HOST (str): The hostname or IP address to bind the server to. Defaults to "0.0.0.0".
        PORT (int): The port number to bind the server to. Defaults to 8000.

    Returns:
        None
    """
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
