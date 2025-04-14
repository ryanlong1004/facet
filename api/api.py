from fastapi import FastAPI, HTTPException, Query, Path
from pydantic import BaseModel
from typing import List, Optional
import uvicorn

app = FastAPI(
    title="GrayMeta Faces API",
    description="Face detection and recognition API. Also provides blurriness, emotion and gender operations.",
    version="0.0.1",
)


# Request and Response Models
class CreateClient(BaseModel):
    client_id: str


class Success(BaseModel):
    client_id: str
    processing_time: float


class Error(BaseModel):
    message: str


class DeletedFaces(BaseModel):
    deletions: int
    processing_time: float


class RecognizeCelebritiesParams(BaseModel):
    image: str


class FaceDetection(BaseModel):
    image: str


class FaceEmotion(BaseModel):
    faces: List[dict]


class FaceGender(BaseModel):
    faces: List[dict]


class LearnFaces(BaseModel):
    faces: List[dict]
    person_id: str
    person_name: str


class FaceNoiseScore(BaseModel):
    image: str


class RecognizeFaces(BaseModel):
    faces: List[dict]
    grouping: bool
    only_unknowns: bool
    recognition_threshold: float


# API Endpoints
@app.post("/clients", response_model=Success, responses={400: {"model": Error}})
async def create_client(client: CreateClient):
    return {"client_id": client.client_id, "processing_time": 0.1}


@app.delete(
    "/clients/{client_id}", response_model=Success, responses={400: {"model": Error}}
)
async def delete_client(client_id: str = Path(...)):
    return {"client_id": client_id, "processing_time": 0.1}


@app.get("/clients/{client_id}", responses={204: {}, 404: {"model": Error}})
async def get_client(client_id: str = Path(...)):
    return {}


@app.delete("/faces", response_model=DeletedFaces, responses={400: {"model": Error}})
async def delete_faces(faces_ids: str = Query(...)):
    return {"deletions": len(faces_ids.split(",")), "processing_time": 0.1}


@app.post("/faces/celebrities", response_model=dict, responses={400: {"model": Error}})
async def recognize_celebrities(params: RecognizeCelebritiesParams):
    return {"processing_time": 0.1, "results": {}}


@app.post("/faces/detect", response_model=dict, responses={400: {"model": Error}})
async def detect_faces(params: FaceDetection):
    return {"processing_time": 0.1, "detections": []}


@app.post("/faces/embeddings", response_model=dict, responses={400: {"model": Error}})
async def get_embeddings(params: dict):
    return {"processing_time": 0.1, "results": []}


@app.post("/faces/emotions", response_model=dict, responses={400: {"model": Error}})
async def detect_emotions(params: FaceEmotion):
    return {"processing_time": 0.1, "results": []}


@app.post("/faces/gender", response_model=dict, responses={400: {"model": Error}})
async def detect_gender(params: FaceGender):
    return {"processing_time": 0.1, "results": []}


@app.post("/faces/learn", response_model=dict, responses={400: {"model": Error}})
async def learn_faces(params: LearnFaces):
    return {"processing_time": 0.1, "results": []}


@app.post("/faces/noise", response_model=dict, responses={400: {"model": Error}})
async def get_noise_score(params: FaceNoiseScore):
    return {"processing_time": 0.1, "results": {}}


@app.post("/faces/recognize", response_model=dict, responses={400: {"model": Error}})
async def recognize_faces(params: RecognizeFaces):
    return {"processing_time": 0.1, "results": []}


@app.post(
    "/faces/recognize-async", response_model=dict, responses={400: {"model": Error}}
)
async def recognize_faces_async(params: RecognizeFaces):
    return {"processing_time": 0.1, "results": []}


@app.delete(
    "/faces/{face_id}", response_model=DeletedFaces, responses={400: {"model": Error}}
)
async def delete_face(face_id: str = Path(...)):
    return {"deletions": 1, "processing_time": 0.1}


@app.get("/healthz", response_model=dict)
async def health_check():
    return {"status": "healthy"}


@app.get(
    "/healthz/full",
    response_model=dict,
    responses={404: {"model": Error}, 503: {"model": Error}},
)
async def full_health_check():
    return {"status": "healthy", "tests_run": 0}


@app.get("/people", response_model=dict, responses={400: {"model": Error}})
async def get_person_id():
    return {"person_id": "new_id", "processing_time": 0.1}


@app.get("/people/{person_id}", response_model=dict, responses={400: {"model": Error}})
async def get_person_faces(person_id: str = Path(...)):
    return {"faces_ids": [], "processing_time": 0.1}


@app.patch(
    "/people/{person_id}", response_model=dict, responses={400: {"model": Error}}
)
async def update_person(person_id: str = Path(...), params: dict = {}):
    return {"processing_time": 0.1}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
