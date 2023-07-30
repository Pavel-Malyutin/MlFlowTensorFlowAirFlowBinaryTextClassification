import json

import pandas as pd
from sqlalchemy import Column
from sqlalchemy import Integer, String
from sqlalchemy import delete as sqlalchemy_delete
from sqlalchemy import update as sqlalchemy_update
from sqlalchemy.future import select

from repository.src.db_setup import Base, db


class DBTrainItem(Base):
    __tablename__ = "train"

    id = Column(Integer, primary_key=True, autoincrement=True)
    text = Column(String(100000), nullable=False)
    label = Column(String(100), nullable=False)

    @classmethod
    async def create(cls, **kwargs):
        item = cls(**kwargs)
        db.add(item)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        return item

    @classmethod
    async def get(cls, item_id):
        query = select(cls).where(cls.id == item_id)
        items = await db.execute(query)
        (item,) = items.first()
        return item

    @classmethod
    async def get_all(cls):
        query = select(cls)
        items = await db.execute(query)
        items = items.scalars().all()
        return items

    @classmethod
    async def update(cls, item_id, **kwargs):
        query = (
            sqlalchemy_update(cls)
            .where(cls.id == item_id)
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
    async def delete(cls, item_id):
        query = sqlalchemy_delete(cls).where(cls.id == item_id)
        await db.execute(query)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        return True


class DBItem(Base):
    __tablename__ = "production"

    id = Column(Integer, primary_key=True, autoincrement=True)
    text = Column(String(100000), nullable=False)
    status = Column(String(15), nullable=False)
    label = Column(String(100))

    @classmethod
    async def create(cls, **kwargs):
        item = cls(**kwargs)
        db.add(item)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        return item

    @classmethod
    async def get(cls, item_id):
        query = select(cls).where(cls.id == item_id)
        items = await db.execute(query)
        (item,) = items.first()
        return item

    @classmethod
    async def get_all(cls):
        query = select(cls)
        items = await db.execute(query)
        items = items.scalars().all()
        return items

    @classmethod
    async def get_to_predict(cls, batch_size: int):
        query = select(cls).where(cls.status == "New").limit(batch_size)
        items = await db.execute(query)
        items = items.scalars().all()
        for item in items:
            await cls.update(item_id=item.id, status="In progress")
        return items

    @classmethod
    async def get_predicted(cls):
        query = select(cls).where(cls.status == "Complete")
        items = await db.execute(query)
        items = items.scalars().all()
        return items

    @classmethod
    async def update(cls, item_id, **kwargs):
        query = (
            sqlalchemy_update(cls)
            .where(cls.id == item_id)
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
                "status": "Complete", "label": record.label
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
    async def delete(cls, item_id):
        query = sqlalchemy_delete(cls).where(cls.id == item_id)
        await db.execute(query)
        try:
            await db.commit()
            return True
        except Exception:
            await db.rollback()
            return False
