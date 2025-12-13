from fastapi import FastAPI

from contextlib import asynccontextmanager
import asyncio
# from redis.asyncio import Redis
# from redis import Redis, ConnectionPool
import redis 

import logging
import sys

logging.basicConfig(
    stream=sys.stdout,    
    level=logging.INFO,
    format="%(levelname)s:     %(message)s"
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info(f"Application is starting up (lifespan)")
    app.state.redis = redis.asyncio.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    # health check redis
    await app.state.redis.ping()
    asyncio.create_task(cache_invalidator())
    yield
    logging.info(f"Application is shutting down (lifespan)")
    
app             = FastAPI(lifespan=lifespan)
REDIS_HOST      = "redis"
REDIS_PORT      = 6379


@app.get("/")
async def main():
    return { "message": "this is root route."}

@app.get("/publish")
async def publish():
    redis   = app.state.redis
    channel = "cache_invalidate"
    message = "Hello, Redis!"
    await redis.publish(channel, message)
    print(f"Published message to {channel}")
    return { "message": "message published."}

cache = {}

async def cache_invalidator() -> None:
    redis   = app.state.redis
    ps      = redis.pubsub()
    await ps.subscribe("cache_invalidate")  
    async for msg in ps.listen():
        if msg['type'] == 'message':
            logging.info(f"🚀 [MESSAGE RECEIVED] {msg}")
            
