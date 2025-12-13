from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm import Mapped
from typing import List
from sqlalchemy import select
import asyncio

import time
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

Base    = declarative_base()

class User(Base):
    __tablename__                   = "users"
    id : Mapped[int]                = Column(Integer, primary_key=True)
    name : Mapped[str]              = Column(String)
    age : Mapped[int]               = Column(Integer)
    posts : Mapped[List["Post"]]    = relationship(back_populates="user",lazy="selectin")
    
class Post(Base):
    __tablename__           = "posts"
    id : Mapped[int]        = Column(Integer, primary_key=True)
    title : Mapped[str]     = Column(String)
    text : Mapped[str]      = Column(String)
    user_id : Mapped[int]   = Column(Integer, ForeignKey("users.id"))
    user : Mapped["User"]   = relationship(back_populates="posts", )
    
DB_USER     = "myuser"
DB_PW       = "password123"
DB_HOST     = "localhost"
PORT_NO     = "5432"

db_url      = f"postgresql+asyncpg://{DB_USER}:{DB_PW}@{DB_HOST}:{PORT_NO}/mydb"
engine      = create_async_engine(
    db_url
)

AsyncSessionLocal   = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)

from sqlalchemy import text

async def fetch_user(uid : int) -> dict:
    async with engine.connect() as connection:
        stmt        = text("SELECT id, name, age FROM users WHERE id = :uid")
        result      = await connection.execute(stmt, {"uid": uid})
        db_user_row = result.fetchone()
        if db_user_row is None:
            raise Exception(f"User with ID {uid} not found.")
        return {
            "id": db_user_row.id, 
            "name": db_user_row.name,
            "age": db_user_row.age
        }

async def main() -> None:
    start       = time.perf_counter()
    all_users   = []
    user_population     = 5000
    for uid in range(1, user_population+1):
        db_user = await fetch_user(uid)
        all_users.append(db_user)
    end     = time.perf_counter()
    elapsed = end - start
    rps     = len(all_users) / elapsed

    print(f" ")
    print(f"w/o ORM             :")
    print(f"Total time          : {elapsed/len(all_users):.10f} s")
    print(f"Throughput          : {rps:.0f} req/s")

if __name__ == "__main__":
    asyncio.run(main())