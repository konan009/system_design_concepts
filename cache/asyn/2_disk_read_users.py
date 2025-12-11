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
    posts : Mapped[List["Post"]]    = relationship(back_populates="user")
    
class Post(Base):
    __tablename__           = "posts"
    id : Mapped[int]        = Column(Integer, primary_key=True)
    title : Mapped[str]     = Column(String)
    text : Mapped[str]      = Column(String)
    user_id : Mapped[int]   = Column(Integer, ForeignKey("users.id"))
    user : Mapped["User"]   = relationship( back_populates="posts")
    

db_url              = "sqlite+aiosqlite:///data/database.db"
engine              = create_async_engine(    
    db_url,
    echo=False,
    future=True
)
AsyncSessionLocal   = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)

async def fetch_user(uid : int ) -> dict:
    async with AsyncSessionLocal() as session:
        stmt = select(User).where(User.id == str(uid))
        result = await session.execute(stmt)
        db_user = result.scalar_one()
        return {"id": db_user.id, "name": db_user.name, "age": db_user.age}

async def main() -> None:
    start       = time.perf_counter()
    all_users   = []
    for uid in range(1, 10001):
        db_user = await fetch_user(uid)
        all_users.append(db_user)
    end     = time.perf_counter()
    elapsed = end - start
    rps     = len(all_users) / elapsed

    print(f"Direct disk read    :")
    print(f"Total time          : {elapsed/len(all_users):.10f} s")
    print(f"Throughput          : {rps:.0f} req/s")

if __name__ == "__main__":
    asyncio.run(main())