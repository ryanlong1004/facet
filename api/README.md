# Person API

This project provides a FastAPI-based application for managing person data. It includes CRUD operations for person records, as well as additional endpoints for retrieving associated data such as faces and version history.

## Features

- **CRUD Operations**: Create, retrieve, update, and delete person records.
- **Version History**: Retrieve version history for a person.
- **Face Management**: List faces associated with a person.
- **Match Management**: List persons in a match.
- **Database Integration**: Uses DuckDB for data storage and retrieval.

## Requirements

- Python 3.13 or higher
- DuckDB
- FastAPI
- Pydantic
- Uvicorn
- Python Dotenv
- Typer

## Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd api
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Set up the environment variables: Create a `.env` file in the root directory and define the following variables:

   ```env
   DB_PATH=faces.duckdb
   HOST=0.0.0.0
   PORT=8000
   ```

## Usage

### Running the API

Start the FastAPI application using Uvicorn:

```bash
python api.py
```

The API will be available at `http://<HOST>:<PORT>` (default: `http://0.0.0.0:8000`).

### Endpoints

#### Persons

- **GET /persons/**: Retrieve all persons.
- **GET /persons/{person_id}**: Retrieve a person by their ID.
- **POST /persons/**: Create a new person.
- **PUT /persons/{person_id}**: Update an existing person.
- **DELETE /persons/{person_id}**: Delete a person by their ID.

#### Faces

- **GET /persons/{id}/faces**: List faces associated with a person.

#### Version History

- **GET /persons/{id}/history**: Retrieve version history for a person.

#### Matches

- **GET /matches/{id}/persons**: List persons in a match.

## Project Structure

```
facet/
├── api/
│   ├── api.py                # Main FastAPI application
│   ├── gateway/
│   │   └── person.py         # DuckDB gateway for CRUD operations
│   ├── model/
│   │   └── person.py         # Pydantic model for person data
│   ├── group_job/
│   │   └── face_groups/      # JSON files for face group data
│   ├── .env                  # Environment variables
│   ├── pyproject.toml        # Project dependencies and metadata
│   └── README.md             # Project documentation
```

## Development

### Adding New Endpoints

1. Define the endpoint in `api.py` using FastAPI decorators.
2. Implement the required logic in the `DuckDbGateway` class (`gateway/person.py`) or other relevant modules.
3. Update the `Person` model (`model/person.py`) if necessary.

### Testing

You can test the API using tools like [Postman](https://www.postman.com/) or [cURL](https://curl.se/).

Example cURL request:

```bash
curl -X GET http://0.0.0.0:8000/persons/
```

## Acknowledgments

- [FastAPI](https://fastapi.tiangolo.com/)
- [DuckDB](https://duckdb.org/)
- [Pydantic](https://docs.pydantic.dev/)
- [Uvicorn](https://www.uvicorn.org/)
