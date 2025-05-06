"""
api.py
derp
This module provides the FastAPI application for managing person data.
"""

import logging
import os
from typing import List

import uvicorn
from dotenv import load_dotenv
from fastapi import APIRouter, FastAPI, HTTPException, Path

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


# CRUD Endpoints for Persons
@persons_router.get("/", response_model=List[Person])
async def get_all_persons():
    """
    Retrieve all persons.

    Returns:
        List[Person]: A list of all persons.
    """
    try:
        with DuckDbGateway(DB_PATH) as gateway:
            persons = (
                gateway.get_all_persons()
            )  # Assuming `get_all_faces` is used for persons
            return [Person.from_dict(person) for person in persons]
    except Exception as e:
        logger.error("Error retrieving all persons: %s", e)
        raise HTTPException(status_code=500, detail="Error retrieving persons") from e


@persons_router.get("/{person_id}", response_model=Person)
async def get_person(
    person_id: str = Path(..., description="The ID of the person to retrieve"),
):
    """
    Retrieve a person by their ID.

    Args:
        person_id (str): The unique ID of the person.

    Returns:
        Person: The person data.
    """
    try:
        with DuckDbGateway(DB_PATH) as gateway:
            person_data = gateway.get_person(
                person_id
            )  # Assuming `get_face` is used for persons
            if not person_data:
                raise HTTPException(status_code=404, detail="Person not found")
            return Person.from_dict(person_data)
    except Exception as e:
        logger.error("Error retrieving person with ID %s: %s", person_id, e)
        raise HTTPException(status_code=500, detail="Error retrieving person") from e


@persons_router.post("/", response_model=Person)
async def create_person(person: Person):
    """
    Create a new person.

    Args:
        person (Person): The person data to create.

    Returns:
        Person: The created person data.
    """
    try:
        with DuckDbGateway(DB_PATH) as gateway:
            gateway.create_person(
                person.to_dict()
            )  # Assuming `create_face` is used for persons
            return person
    except Exception as e:
        logger.error("Error creating person: %s", e)
        raise HTTPException(status_code=500, detail="Error creating person") from e


@persons_router.put("/{person_id}", response_model=Person)
async def update_person(person_id: str, person: Person):
    """
    Update an existing person.

    Args:
        person_id (str): The unique ID of the person.
        person (Person): The updated person data.

    Returns:
        Person: The updated person data.
    """
    try:
        with DuckDbGateway(DB_PATH) as gateway:
            gateway.update_person(
                person_id, person.to_dict()
            )  # Assuming `update_face` is used for persons
            return person
    except Exception as e:
        logger.error("Error updating person with ID %s: %s", person_id, e)
        raise HTTPException(status_code=500, detail="Error updating person") from e


@persons_router.delete("/{person_id}")
async def delete_person(person_id: str):
    """
    Delete a person by their ID.

    Args:
        person_id (str): The unique ID of the person.

    Returns:
        Dict[str, str]: A confirmation message.
    """
    try:
        with DuckDbGateway(DB_PATH) as gateway:
            gateway.delete_person(
                person_id
            )  # Assuming `delete_face` is used for persons
            return {"message": f"Person with ID {person_id} deleted successfully."}
    except Exception as e:
        logger.error("Error deleting person with ID %s: %s", person_id, e)
        raise HTTPException(status_code=500, detail="Error deleting person") from e


# Include the router in the main app
app.include_router(persons_router)


def main():
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
