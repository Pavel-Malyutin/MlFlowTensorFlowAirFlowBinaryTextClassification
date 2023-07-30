from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from repository.pydantic_models import Item, TrainItem, Batch
from repository.src.db_models import DBTrainItem, DBItem

router = APIRouter()
item_db = DBItem()
train_item_db = DBTrainItem()


@router.post("/upload/raw")
async def add_item(item: Item):
    """
    Add new item to database
    """
    return await item_db.create(item=item.text, status=item.status, label=item.label)


@router.post("/upload/train")
async def add_train_item(item: TrainItem):
    """
    Add new train item to database
    """
    return await train_item_db.create(item=item.text, label=item.label)


@router.post("/upload/predicted")
async def update_item(item: Item):
    """
    Update item in database
    """
    return await item_db.update(item_id=item.id, label=item.label, status=item.status)


@router.post("/upload/predicted_batch")
async def update_item(batch: Batch):
    """
    Update item in database
    """
    return await item_db.update_batch(batch)


@router.get("/download/to_predict/{batch_size}")
async def download_to_predict(batch_size: int):
    """
    Get items to predict
    """
    data = await item_db.get_to_predict(batch_size)
    return data


@router.get("/download/predicted")
async def download_predicted():
    """
    Download predicted items
    """
    return await item_db.get_predicted()


@router.get("/download/train")
async def download_train():
    """
    Download train set
    """
    return await train_item_db.get_all()


@router.get("/health", status_code=201)
async def echo():
    """
    Echo
    """
    return "ok"


@router.get("/")
async def redirect():
    return RedirectResponse("/docs")
