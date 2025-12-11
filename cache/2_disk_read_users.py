from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, Session, relationship
from sqlalchemy.orm import Mapped
from typing import List
import time

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
start = time.perf_counter()
all_users = []
for uid in range(1,1001):
    with Session(engine) as session:
        stmt    = select(User).where(User.id == str(uid))
        db_user = session.execute(stmt).scalar_one()
        all_users.append({
            'id': db_user.id,
            'name': db_user.name,
            'age': db_user.age
        })  
end = time.perf_counter()
diff = (end - start)/len(all_users)
print(f"Disk Read took          : {diff:.8f} seconds")