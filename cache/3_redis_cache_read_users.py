from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, Session, relationship
from sqlalchemy.orm import Mapped
from typing import List
import random
import redis
import json
from sqlalchemy.orm import Session, joinedload
from cachetools import TTLCache
import time
from sqlalchemy.engine import Engine

    
db_url  = "sqlite:///data/database.db"
engine  = create_engine(
    db_url
)
Base    = declarative_base()
class User(Base):
    __tablename__                   = "users"
    id                              = Column(Integer, primary_key=True)
    name                            = Column(String)
    age: Mapped[int]                = Column(Integer)
    posts : Mapped[List["Post"]]    = relationship(back_populates="user")
class Post(Base):
    __tablename__           = "posts"
    id                      = Column(Integer, primary_key=True)
    title                   = Column(String)
    text                    = Column(String)
    user_id                 = Column(Integer, ForeignKey("users.id"))
    user : Mapped["User"]   = relationship( back_populates="posts")

from sqlalchemy import select


cache_redis     = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
cache_inprocess = TTLCache(maxsize=100, ttl=60) 
def generate_cache(engine : Engine, cache_inprocess : TTLCache, cache_redis : redis.Redis) -> None:
    with Session(engine) as session:
        users = session.query(User)
        for user in users:
            cache_key = f"user-{user.id}"
            data = {
                "id": user.id,
                "name": user.name,
                "age": user.age
            }
            cache_redis.set(cache_key, json.dumps(data), ex=120)
            cache_inprocess[cache_key] = data

generate_cache(engine, cache_inprocess, cache_redis)

start = time.perf_counter()
all_users = []
for uid in range(1,1001):
    cache_key = f"user-{uid}"
    cache_data = cache_redis.get(cache_key)
    if cache_data is None:
        with Session(engine) as session:
            stmt    = select(User).where(User.id == str(uid))
            db_user = session.execute(stmt).scalar_one()
        all_users.append({
            'id': db_user.id,
            'name': db_user.name,
            'age': db_user.age
        })
    else:
        user_data = json.loads(cache_data)
        all_users.append({
            'id': user_data['id'],
            'name': user_data['name'],
            'age': user_data['age']
        })
        
end     = time.perf_counter()
diff    = (end - start)/len(all_users)
print(f"Redis Cache Took        : {diff:.8f} seconds")

