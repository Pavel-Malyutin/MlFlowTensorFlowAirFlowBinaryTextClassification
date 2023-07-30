from typing import Optional

from pydantic import BaseModel


class Item(BaseModel):
    id: Optional[int]
    text: Optional[str]
    label: Optional[str]
    status: Optional[str] = "New"

    class Config:
        orm_mode = True


class TrainItem(BaseModel):
    text: str
    label: str

    class Config:
        orm_mode = True


class Batch(BaseModel):
    predictions: str
