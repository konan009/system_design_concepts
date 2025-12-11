from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, Session, relationship
from sqlalchemy.orm import Mapped
from typing import List
from sqlalchemy.orm import Session, joinedload
from cachetools import TTLCache
import time
from sqlalchemy.engine import Engine
from sqlalchemy import select

cache_inprocess = TTLCache(maxsize=10_000, ttl=60) 
db_url          = "sqlite:///data/database.db"
engine          = create_engine(
    db_url
)
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



def generate_cache(engine : Engine, cache_inprocess : TTLCache) -> None:
    with Session(engine) as session:
        users = session.query(User)
        for user in users:
            cache_key = f"user-{user.id}"
            data = {
                "id": user.id,
                "name": user.name,
                "age": user.age
            }
            cache_inprocess[cache_key] = data
            
generate_cache(engine, cache_inprocess)
start = time.perf_counter()
all_users = []

for uid in range(1, 10001):
    cache_key = f"user-{uid}"
    cache_data = cache_inprocess.get(cache_key)

    if cache_data is None:
        with Session(engine) as session:
            stmt    = select(User).where(User.id == uid)
            db_user = session.execute(stmt).scalar_one()
        user_data = {
            'id': db_user.id,
            'name': db_user.name,
            'age': db_user.age
        }
    else:
        user_data = cache_data
    
    all_users.append(user_data)

end = time.perf_counter()

elapsed = end - start
rps = len(all_users) / elapsed


print(f"")
print(f"In-process          :")
print(f"Total time          : {elapsed/len(all_users):.10f} s")
print(f"Throughput          : {rps:.0f} req/s")

