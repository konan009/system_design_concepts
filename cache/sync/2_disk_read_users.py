from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, Session, relationship
from sqlalchemy.orm import Mapped
from typing import List
import time
from sqlalchemy import select


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



db_url  = "sqlite:///data/database.db"
engine  = create_engine(
    db_url
)

start = time.perf_counter()
all_users = []
for uid in range(1,10001):
    with Session(engine) as session:
        stmt    = select(User).where(User.id == str(uid))
        db_user = session.execute(stmt).scalar_one()
        all_users.append({
            'id': db_user.id,
            'name': db_user.name,
            'age': db_user.age
        })  
end = time.perf_counter()

elapsed = end - start
rps = len(all_users) / elapsed

print(f"")
print(f"Direct disk read    :")
print(f"Total time          : {elapsed/len(all_users):.10f} s")
print(f"Throughput          : {rps:.0f} req/s")
