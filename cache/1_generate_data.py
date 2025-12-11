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

db_url  = "sqlite:///data/database.db"
engine  = create_engine(
    db_url,
    execution_options={
        "compiled_cache": None
    }
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

Base.metadata.drop_all(engine) 
Base.metadata.create_all(engine)

user_population = 1000
with Session(engine) as session:
    users = [User(name=f"User {index}",age=random.randint(20, 40)) for index in range(user_population)]
    session.add_all(users)
    session.flush() 
    
    posts = []
    for index in range(1000):
        rand_number = random.randint(0, user_population-1)
        user_object = users[rand_number]
        posts.append(Post(title=f"Title {index}", text=f"Post Content {index}", user=user_object))
        
    session.add_all(posts)
    session.commit()