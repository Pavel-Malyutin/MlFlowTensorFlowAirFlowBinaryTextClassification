import uvicorn
from fastapi import FastAPI

from repository.exceptions import exception_handler
from repository.router import router
from repository.src.db_setup import db

app = FastAPI(title="DB repository",
              description="CRUD operations for database",
              version="0.0.1",
              contact={
                  "name": "Pavel",
                  "email": "test@example.com"}
              )


@app.on_event("startup")
async def startup():
    db.init()
    await db.create_tables()


@app.on_event("shutdown")
async def shutdown():
    await db.close()


app.exception_handler(exception_handler)
app.include_router(router)


if __name__ == '__main__':
    uvicorn.run("app:app", host="localhost", port=8182, reload=True, log_level="debug")
