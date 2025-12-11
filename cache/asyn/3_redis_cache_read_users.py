from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm import Mapped
from typing import List
from sqlalchemy import select
import asyncio
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
import uvloop
import redis.asyncio as redis
import json
import time
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

HOST    = "127.0.0.1"
PORT    = 6379
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

db_url  = "sqlite+aiosqlite:///data/database.db"
engine  = create_async_engine(    
    db_url,
    echo=False,
    future=True
)

AsyncSessionLocal = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)

pool = redis.ConnectionPool(
    host=HOST,
    port=PORT,
    db=0,
    decode_responses=True,
    max_connections=10_000,
)

cache_redis         = redis.Redis(connection_pool=pool)

async def generate_cache(cache_redis : redis.Redis) -> None:
    async with AsyncSessionLocal() as session:
        query_result = await session.execute(select(User))
        users = query_result.scalars().all()
        for user in users:
            cache_key = f"user-{user.id}"
            data = {
                "id": user.id,
                "name": user.name,
                "age": user.age
            }
            await cache_redis.set(cache_key, json.dumps(data), ex=120)

async def fetch_user(uid: int) -> dict:
    cache_key   = f"user-{uid}"
    cache_data  = await cache_redis.get(cache_key)
    if cache_data is None:
        async with AsyncSessionLocal() as session:
            result  = await session.execute(select(User).where(User.id == uid))
            db_user = result.scalar_one()
        return {
            'id': db_user.id,
            'name': db_user.name,
            'age': db_user.age
        }
    else:
        user_data = json.loads(cache_data)
        return {
            'id': user_data['id'],
            'name': user_data['name'],
            'age': user_data['age']
        }

async def main() -> None:
    await generate_cache(cache_redis)
    start       = time.perf_counter()
    all_users   = []
    for uid in range(1, 10001):
        db_user = await fetch_user(uid)
        all_users.append(db_user)
    end     = time.perf_counter()
    elapsed = end - start
    rps     = len(all_users) / elapsed

    print(f"Redis read          :")
    print(f"Total time          : {elapsed/len(all_users):.10f} s")
    print(f"Throughput          : {rps:.0f} req/s")

if __name__ == "__main__":
    asyncio.run(main())


