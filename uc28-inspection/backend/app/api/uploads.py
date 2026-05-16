from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

from app.storage.files import kind_from_content_type, load_file, save_file

router = APIRouter(tags=["uploads"])

_id_to_path: dict[str, str] = {}


@router.post("/uploads", status_code=201)
async def upload_file(file: UploadFile = File(...)):
    data = await file.read()
    content_type = file.content_type or "application/octet-stream"
    file_id, path = save_file(data, file.filename or "upload")
    _id_to_path[file_id] = path
    return {"id": file_id, "path": path, "kind": kind_from_content_type(content_type)}


@router.get("/uploads/{file_id}")
def get_upload(file_id: str):
    path = _id_to_path.get(file_id)
    if not path:
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path)
