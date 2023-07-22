import json

import pandas as pd
from sqlalchemy import Column
from sqlalchemy import Integer, String
from sqlalchemy import delete as sqlalchemy_delete
from sqlalchemy import update as sqlalchemy_update
from sqlalchemy.future import select

from repository.src.db_setup import Base, db


class DBTrainReview(Base):
    __tablename__ = "train"

    id = Column(Integer, primary_key=True, autoincrement=True)
    review = Column(String(100000), nullable=False)
    sentiment = Column(String(100), nullable=False)

    @classmethod
    async def create(cls, **kwargs):
        review = cls(**kwargs)
        db.add(review)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        return review

    @classmethod
    async def get(cls, review_id):
        query = select(cls).where(cls.id == review_id)
        reviews = await db.execute(query)
        (review,) = reviews.first()
        return review

    @classmethod
    async def get_all(cls):
        query = select(cls)
        reviews = await db.execute(query)
        reviews = reviews.scalars().all()
        return reviews

    @classmethod
    async def update(cls, review_id, **kwargs):
        query = (
            sqlalchemy_update(cls)
            .where(cls.id == review_id)
            .values(**kwargs)
            .execution_options(synchronize_session="fetch")
        )
        await db.execute(query)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            raise

    @classmethod
    async def delete(cls, review_id):
        query = sqlalchemy_delete(cls).where(cls.id == review_id)
        await db.execute(query)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        return True


class DBReview(Base):
    __tablename__ = "production"

    id = Column(Integer, primary_key=True, autoincrement=True)
    review = Column(String(100000), nullable=False)
    status = Column(String(15), nullable=False)
    sentiment = Column(String(100))

    @classmethod
    async def create(cls, **kwargs):
        review = cls(**kwargs)
        db.add(review)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        return review

    @classmethod
    async def get(cls, review_id):
        query = select(cls).where(cls.id == review_id)
        reviews = await db.execute(query)
        (review,) = reviews.first()
        return review

    @classmethod
    async def get_all(cls):
        query = select(cls)
        reviews = await db.execute(query)
        reviews = reviews.scalars().all()
        return reviews

    @classmethod
    async def get_to_predict(cls, batch_size: int):
        query = select(cls).where(cls.status == "New").limit(batch_size)
        reviews = await db.execute(query)
        reviews = reviews.scalars().all()
        for review in reviews:
            await cls.update(review_id=review.id, status="In progress")
        return reviews

    @classmethod
    async def get_predicted(cls):
        query = select(cls).where(cls.status == "Complete")
        reviews = await db.execute(query)
        reviews = reviews.scalars().all()
        return reviews

    @classmethod
    async def update(cls, review_id, **kwargs):
        query = (
            sqlalchemy_update(cls)
            .where(cls.id == review_id)
            .values(**kwargs)
            .execution_options(synchronize_session="fetch")
        )
        await db.execute(query)
        try:
            await db.commit()
            return True
        except Exception:
            await db.rollback()
            return False

    @classmethod
    async def update_batch(cls, batch, **kwargs):
        df = pd.DataFrame.from_dict(json.loads(batch.predictions))
        for _, record in df.iterrows():
            data = {
                "status": "Complete", "sentiment": record.sentiment
            }
            query = (
                sqlalchemy_update(cls)
                .where(cls.id == record.id)
                .values(data)
                .execution_options(synchronize_session="fetch")
            )
            await db.execute(query)
        try:
            await db.commit()
            return True
        except Exception:
            await db.rollback()
            return False

    @classmethod
    async def delete(cls, review_id):
        query = sqlalchemy_delete(cls).where(cls.id == review_id)
        await db.execute(query)
        try:
            await db.commit()
            return True
        except Exception:
            await db.rollback()
            return False
