from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from repository.pydantic_models import Review, TrainReview, Batch
from repository.src.db_models import DBTrainReview, DBReview

router = APIRouter()
review_db = DBReview()
train_review_db = DBTrainReview()


@router.post("/upload/raw")
async def add_review(review: Review):
    """
    Add new review to database
    """
    return await review_db.create(review=review.text, status=review.status, sentiment=review.sentiment)


@router.post("/upload/train")
async def add_train_review(review: TrainReview):
    """
    Add new train review to database
    """
    return await train_review_db.create(review=review.text, sentiment=review.sentiment)


@router.post("/upload/predicted")
async def update_review(review: Review):
    """
    Update review in database
    """
    return await review_db.update(review_id=review.id, sentiment=review.sentiment, status=review.status)


@router.post("/upload/predicted_batch")
async def update_review(batch: Batch):
    """
    Update review in database
    """
    return await review_db.update_batch(batch)


@router.get("/download/to_predict/{batch_size}")
async def download_to_predict(batch_size: int):
    """
    Get reviews to predict
    """
    data = await review_db.get_to_predict(batch_size)
    return data


@router.get("/download/predicted")
async def download_predicted():
    """
    Download predicted reviews
    """
    return await review_db.get_predicted()


@router.get("/download/train")
async def download_train():
    """
    Download train set
    """
    return await train_review_db.get_all()


@router.get("/health", status_code=201)
async def echo():
    """
    Echo
    """
    return "ok"


@router.get("/")
async def redirect():
    return RedirectResponse("/docs")
