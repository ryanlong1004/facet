"""
api.py

This module provides the FastAPI application for managing face data. It includes endpoints for accounts, faces,
people, and health checks. The API uses a pickle file to store and retrieve face data.

Modules:
    - Accounts: Manage account-related operations.
    - Faces: Manage face-related operations.
    - People: Manage person-related operations.
    - Health: Check the health of the API.

Classes:
    CreateAccount: Request model for creating a account.
    Success: Response model for successful operations.
    Error: Response model for errors.
    DeletedFaces: Response model for deleted faces.
"""

import logging
import os
from typing import Dict, List

import uvicorn
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from gateway.faces import FaceDataHandler
from gateway.people import PersonDataHandler
from models import FaceData, PersonData

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
# Load environment variables from .env file
load_dotenv()

# Fetch the Pickle file path from the .env file
FACES_PICKLE_FILE_PATH = os.getenv("FACES_PICKLE_FILE_PATH", "faces.pkl")
PEOPLE_PICKLE_FILE_PATH = os.getenv("PEOPLE_PICKLE_FILE_PATH", "people.pkl")

API_DESCRIPTION = """
This API provides a comprehensive solution for managing face data, including operations for accounts, faces, and people, as well as health checks. It is built using FastAPI and leverages a pickle file for data storage and retrieval. Below is a detailed description of the API's functionality:

### Features:
1. **Accounts Management**:
    - Retrieve all account IDs.
    - Retrieve details of a specific account by ID.
    - Create a new account.
    - Update an existing account's details.
    - Delete a account by ID.

2. **Faces Management**:
    - Retrieve face data by face ID.
    - Retrieve all face data with pagination support.
    - Delete a face by ID (deprecated).

3. **People Management**:
    - Retrieve all person IDs with pagination support.
    - Retrieve all faces associated with a specific person ID.
    - Create a new person with a unique ID and name.
    - Update an existing person's details.
    - Delete a person by ID.

4. **Health Check**:
    - Verify the health of the API and its ability to access the pickle file.


### Additional Details:
- **Environment Variables**:
  - `FACES_PICKLE_FILE_PATH`: Path to the pickle file for data storage.
  - `API_TITLE`: Title of the API.
  - `API_DESCRIPTION`: Description of the API.
  - `API_VERSION`: Version of the API.

- **Logging**:
  - The API uses Python's logging module to log important events and errors.

- **Pagination**:
  - Pagination is supported for retrieving faces and person IDs, allowing efficient data retrieval.

- **PickleCRUD**:
  - A custom gateway class is used to interact with the pickle file for CRUD operations.

"""

# Initialize FastAPI app
app = FastAPI(
    title=os.getenv("API_TITLE", "Faces API"),
    description=os.getenv(
        "API_DESCRIPTION",
        API_DESCRIPTION,
    ),
    version=os.getenv("API_VERSION", "0.1"),
)

# Bearer token authentication
security = HTTPBearer()

# Initialize API routers
accounts_router = APIRouter(prefix="/accounts", tags=["Accounts"])
faces_router = APIRouter(prefix="/faces", tags=["Faces"])
people_router = APIRouter(prefix="/people", tags=["People"])
health_router = APIRouter(prefix="/health", tags=["Health"])


# Request and Response Models
class CreateAccount(BaseModel):
    """
    Request model for creating a account.

    Attributes:
        account_id (int): The unique ID of the account.
    """

    account_id: int


class Success(BaseModel):
    """
    Response model for successful operations.

    Attributes:
        account_id (int): The unique ID of the account.
        processing_time (float): The time taken to process the request.
    """

    account_id: int
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


# Accounts Endpoints
@accounts_router.get(
    "/", response_model=Dict[str, List[str]], responses={400: {"model": Error}}
)
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verify the provided bearer token.

    Args:
        credentials (HTTPAuthorizationCredentials): The bearer token credentials.

    Raises:
        HTTPException: If the token is invalid.

    Returns:
        str: The token if valid.
    """
    token = credentials.credentials
    # Replace "your-secret-token" with your actual token or token validation logic
    if token != "your-secret-token":
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    return token


async def get_all_accounts():
    """
    Retrieve all account IDs.

    Returns:
        Dict[str, List[str]]: A dictionary containing all account IDs.
    """
    try:
        # Simulate retrieving all accounts (e.g., from a database or pickle file)
        # This is a placeholder for actual logic
        logger.info("Retrieving all accounts")
        accounts = ["account1", "account2", "account3"]  # Example data
        return {"accounts": accounts}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@accounts_router.get(
    "/{account_id}", response_model=Dict[str, str], responses={404: {"model": Error}}
)
async def get_account(account_id: str = Path(...)):
    """
    Retrieve a account by their ID.

    Args:
        account_id (str): The unique ID of the account.

    Returns:
        Dict[str, str]: A dictionary containing the account ID and additional details.
    """
    try:
        # Simulate retrieving a account (e.g., from a database or pickle file)
        # This is a placeholder for actual logic
        logger.info("Retrieving account with ID %s", account_id)
        account_data = {
            "account_id": account_id,
            "details": "Example account details",
        }  # Example data
        return account_data
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@accounts_router.post("/", response_model=Success, responses={400: {"model": Error}})
async def create_account(account: CreateAccount):
    """
    Create a new account.

    Args:
        account (CreateAccount): The account data.

    Returns:
        Success: The response containing the account ID and processing time.
    """
    try:
        # Simulate creating a account (e.g., adding to a database or pickle file)
        # This is a placeholder for actual logic
        logger.info("Creating account with ID %s", account.account_id)
        return {"account_id": account.account_id, "processing_time": 0.1}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@accounts_router.put(
    "/{account_id}", response_model=Success, responses={404: {"model": Error}}
)
async def update_account(account_id: str, account_name: str):
    """
    Update an existing account.

    Args:
        account_id (str): The unique ID of the account.
        account_name (str): The updated name of the account.

    Returns:
        Success: The response containing the account ID and processing time.
    """
    try:
        # Simulate updating a account (e.g., modifying data in a database or pickle file)
        # This is a placeholder for actual logic
        logger.info("Updating account with ID %s to name %s", account_id, account_name)
        return {"account_id": account_id, "processing_time": 0.1}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@accounts_router.delete(
    "/{account_id}", response_model=DeletedFaces, responses={404: {"model": Error}}
)
async def delete_account(account_id: str = Path(...)):
    """
    Delete a account by their ID.

    Args:
        account_id (str): The unique ID of the account.

    Returns:
        DeletedFaces: The response containing the number of deletions and processing time.
    """
    try:
        # Simulate deleting a account (e.g., removing from a database or pickle file)
        # This is a placeholder for actual logic
        logger.info("Deleting account with ID %s", account_id)
        return {"deletions": 1, "processing_time": 0.1}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


# Faces Endpoints
@faces_router.get(
    "/{face_id}",
    response_model=FaceData,
    responses={404: {"model": Error}},
    deprecated=True,
)
async def get_face(face_id: str = Path(...)):
    """
    Retrieve a face by its ID (Deprecated).

    Args:
        face_id (str): The unique ID of the face.

    Returns:
        FaceData: The face data.
    """
    try:
        face_data = FaceDataHandler(FACES_PICKLE_FILE_PATH).read(face_id)
        if face_data is None:
            raise HTTPException(status_code=404, detail="Face not found")
        return face_data
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@faces_router.get(
    "/",
    response_model=List[FaceData],
    responses={400: {"model": Error}},
)
async def get_all_faces(
    page_number: int = Query(1, ge=1), page_length: int = Query(10, ge=1)
):
    """
    Retrieve all faces with pagination (Deprecated).

    Args:
        page_number (int): The page number (default is 1).
        page_length (int): The number of records per page (default is 10).

    Returns:
        List[FaceData]: A list of face data.
    """
    try:
        faces = FaceDataHandler(FACES_PICKLE_FILE_PATH).read_all()
        faces_list = list(faces.values())

        # Calculate offset based on page_number and page_length
        offset = (page_number - 1) * page_length
        paginated_faces = faces_list[offset : offset + page_length]

        return paginated_faces
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@faces_router.post(
    "/",
    response_model=Success,
    responses={400: {"model": Error}},
)
async def create_face(face_data: FaceData):
    """
    Create a new face record (Deprecated).

    Args:
        face_data (FaceData): The face data to create.

    Returns:
        Success: The response containing the face ID and processing time.
    """
    try:
        crud = FaceDataHandler(FACES_PICKLE_FILE_PATH)
        crud.create(face_data.face_id, face_data)
        logger.info("Created face with ID %s", face_data.face_id)
        return {"account_id": face_data.face_id, "processing_time": 0.1}
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))


@faces_router.put(
    "/{face_id}",
    response_model=Success,
    responses={404: {"model": Error}},
    deprecated=True,
)
async def update_face(face_id: str, face_data: FaceData):
    """
    Update an existing face record (Deprecated).

    Args:
        face_id (str): The unique ID of the face.
        face_data (FaceData): The updated face data.

    Returns:
        Success: The response containing the face ID and processing time.
    """
    try:
        crud = FaceDataHandler(FACES_PICKLE_FILE_PATH)
        crud.update(face_id, face_data)
        logger.info("Updated face with ID %s", face_id)
        return {"account_id": face_id, "processing_time": 0.1}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@faces_router.delete(
    "/{face_id}",
    response_model=DeletedFaces,
    responses={404: {"model": Error}},
    deprecated=True,
)
async def delete_face(face_id: str = Path(...)):
    """
    Delete a face by its ID (Deprecated).

    Args:
        face_id (str): The unique ID of the face.

    Returns:
        DeletedFaces: The response containing the number of deletions and processing time.
    """
    try:
        crud = FaceDataHandler(FACES_PICKLE_FILE_PATH)
        crud.delete(face_id)
        logger.info("Deleted face with ID %s", face_id)
        return {"deletions": 1, "processing_time": 0.1}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


# People Endpoints
@people_router.get(
    "/", response_model=Dict[str, List[PersonData]], responses={400: {"model": Error}}
)
async def get_persons(
    page_number: int = Query(1, ge=1), page_length: int = Query(10, ge=1)
):
    """
    Retrieve all persons with pagination.

    Args:
        page_number (int): The page number (default is 1).
        page_length (int): The number of records per page (default is 10).

    Returns:
        Dict[str, List[PersonData]]: A dictionary containing the paginated person data.
    """
    try:
        people = PersonDataHandler(PEOPLE_PICKLE_FILE_PATH).read_all()
        people_list = list(people.values())

        # Calculate offset based on page_number and page_length
        offset = (page_number - 1) * page_length
        paginated_people = people_list[offset : offset + page_length]

        # Return the result as a dictionary
        return {"persons": paginated_people}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@people_router.get(
    "/{person_id}",
    response_model=Dict[
        str, List[PersonData]
    ],  # Ensure the response model expects a list
    responses={404: {"model": Error}},
)
async def get_person(person_id: int):
    """
    Retrieve a person by their ID.

    Args:
        person_id (int): The unique ID of the person.

    Returns:
        Dict[str, List[PersonData]]: A dictionary containing the person data as a list.
    """
    try:
        person = PersonDataHandler(PEOPLE_PICKLE_FILE_PATH).read(person_id)
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")

        # Wrap the single PersonData object in a list
        return {"people": [person]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@people_router.get(
    "/{person_id}/faces",
    response_model=Dict[str, List[FaceData]],
    responses={404: {"model": Error}},
)
async def get_person_faces(person_id: int = Path(...)):
    """
    Retrieve all faces associated with a specific person ID.

    Args:
        person_id (int): The unique ID of the person.

    Returns:
        Dict[str, List[FaceData]]: A dictionary containing the person's faces.
    """

    try:
        crud = PersonDataHandler(PEOPLE_PICKLE_FILE_PATH)
        person_faces = crud.read(person_id)
        if person_faces is None:
            raise HTTPException(status_code=404, detail="Person not found")
        return {"faces": person_faces}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@people_router.post("/", response_model=Success, responses={400: {"model": Error}})
async def create_person(person_name: str):
    """
    Create a new person.

    Args:
        person_id (int): The unique ID of the person.
        person_name (str): The name of the person.

    Returns:
        Success: The response containing the person ID and processing time.
    """
    try:
        # Simulate creating a person (e.g., adding metadata to the pickle file)
        # This is a placeholder for actual logic
        logger.info("Creating person with name %s", person_name)
        crud = PersonDataHandler(PEOPLE_PICKLE_FILE_PATH)
        person = crud.create(person_name)
        logger.info(
            "Created person with ID %s and name %s",
            person.person_id,
            person.person_name,
        )
        return {"account_id": person.person_id, "processing_time": 0.1}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@people_router.put(
    "/{person_id}", response_model=Success, responses={404: {"model": Error}}
)
async def update_person(person_id: int, person_name: str):
    """
    Update an existing person's details.

    Args:
        person_id (int): The unique ID of the person.
        person_name (str): The updated name of the person.

    Returns:
        Success: The response containing the person ID and processing time.
    """
    try:
        crud = PersonDataHandler(PEOPLE_PICKLE_FILE_PATH)
        crud.update(person_id, PersonData(person_id=person_id, person_name=person_name))
        logger.info("Updated person with ID %s to name %s", person_id, person_name)
        return {"account_id": person_id, "processing_time": 0.1}
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@people_router.delete(
    "/{person_id}", response_model=DeletedFaces, responses={404: {"model": Error}}
)
async def delete_person(person_id: int = Path(...)):
    """
    Delete a person by their ID.

    Args:
        person_id (int): The unique ID of the person.

    Returns:
        DeletedFaces: The response containing the number of deletions and processing time.
    """
    try:
        crud = PersonDataHandler(PEOPLE_PICKLE_FILE_PATH)
        crud.delete(person_id)
        logger.info("Deleted person with ID %s", person_id)
        return {"deletions": 1, "processing_time": 0.1}
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
        FaceDataHandler(
            FACES_PICKLE_FILE_PATH
        ).read_all()  # Test if the pickle file is accessible
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")


# Include routers in the main app
app.include_router(accounts_router)
app.include_router(faces_router)
app.include_router(people_router)
app.include_router(health_router)


def main():
    metadata_folder = "data/metadata"
    people_file_path = "data/test_names_10000.txt"

    faces_data_handler = FaceDataHandler(FACES_PICKLE_FILE_PATH)
    people_data_handler = PersonDataHandler(PEOPLE_PICKLE_FILE_PATH)

    # Import metadata using the new method
    faces_data_handler.import_metadata(metadata_folder)
    people_ids = faces_data_handler.get_all_group_ids()
    people_data_handler.generate_people_data(people_ids, people_file_path)
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
