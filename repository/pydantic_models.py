from typing import Optional

from pydantic import BaseModel


class Review(BaseModel):
    id: Optional[int]
    text: Optional[str]
    sentiment: Optional[str]
    status: Optional[str] = "New"

    class Config:
        orm_mode = True


class TrainReview(BaseModel):
    text: str
    sentiment: str

    class Config:
        orm_mode = True


class Batch(BaseModel):
    predictions: str
