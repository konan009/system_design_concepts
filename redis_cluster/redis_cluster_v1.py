import asyncio
from redis.asyncio.cluster import RedisCluster
from redis.cluster import ClusterNode
import time
from multiprocessing import Process
from typing import AsyncGenerator

HOST            = '127.0.0.1'
TOTAL_NUMBER    = 20_000

async def fetch_key(rc : RedisCluster, key : str) -> None:
    return await rc.get(key)

# this is definitely not the best way of production implementation of using redis client, but for now
# i default onto this way. 
async def redis_client()-> AsyncGenerator[RedisCluster, None]:
    nodes           = [
        ClusterNode(HOST, 7001),
        ClusterNode(HOST, 7002),
        ClusterNode(HOST, 7003)
    ]
    rc = RedisCluster(
        startup_nodes=nodes
    )
    await rc.initialize()
    try:
        yield rc 
    finally:
        await rc.aclose() 

ranges          = [
    (0      ,5460),
    (5461   ,10922),
    (10923  ,16383),
]
node_numbers    = {}

for i in range(len(ranges)):
    node_numbers[i] = []

tasks_1 = []
tasks_2 = []  

async def main() -> None:
    async for rc in redis_client():  
        for i in range(20):
            key = f"user_session:{i}"
            value = f"session_data_{i}"
            await rc.set(key, value)
            slot = rc.keyslot(key)

            for node_number,(low_bound,up_bound) in enumerate(ranges):
                if slot >= low_bound and  slot <= up_bound:
                    break
            node_numbers[node_number].append(key)
            
        for counter in range(TOTAL_NUMBER):
            key = node_numbers[counter % 3][0]
            tasks_1.append(fetch_key(rc, key))
        
        for counter in range(TOTAL_NUMBER):
            key = f"user_session:5" 
            tasks_2.append(fetch_key(rc, key))
            
        start   = time.time()
        _       = await asyncio.gather(*tasks_1)
        end     = time.time()
        print(f"Distributed Access Time     {end - start}")
        start   = time.time()
        _       = await asyncio.gather(*tasks_2)
        end     = time.time()
        print(f"Hot Key Time Duration       {end - start}")
        await rc.aclose()

asyncio.run(main())

async def worker1() -> None:
    async for rc in redis_client():  
        keys    = ['user_session:3', 'user_session:1', 'user_session:0']
        tasks   = [fetch_key(rc, keys[i % 3]) for i in range(TOTAL_NUMBER//2)]
        await asyncio.gather(*tasks)

async def worker2() -> None:
    async for rc in redis_client(): 
        keys    = 'user_session:1'
        tasks   = [fetch_key(rc, keys) for _ in range(TOTAL_NUMBER//2)]
        await asyncio.gather(*tasks)
        
def start_async_process_1() -> None:
    asyncio.run(worker1())

def start_async_process_2()-> None:
    asyncio.run(worker2())
    
print("")
start = time.time()
p1 = Process(target=start_async_process_1)
p2 = Process(target=start_async_process_1)
p1.start()
p2.start()
p1.join()
p2.join()
end = time.time()
print(f"Distributed Time Duration   {end - start}")


start = time.time()
p1 = Process(target=start_async_process_2)
p2 = Process(target=start_async_process_2)
p1.start()
p2.start()
p1.join()
p2.join()
end = time.time()
print(f"Hot Key Time Duration       {end - start}")