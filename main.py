from fastapi import FastAPI, HTTPException
from celery import Celery
import redis
import uuid

from pydantic import BaseModel

app = FastAPI()
database = {}  # Manual db

celery = Celery("main", broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
redis_client = redis.StrictRedis(host='localhost', port=6379, db=1)

class User(BaseModel):
    N: int


def is_prime(num):
    if num <= 1:
        return False
    for i in range(2, int(num ** 0.5) + 1):
        if num % i == 0:
            return False
    return True


@celery.task
def generate_primes(n):
    print("LEVEL2")
    primes = []
    num = 1
    while len(primes) < n:
        if is_prime(num):
            primes.append(num)
        num += 1
    return primes


@app.post("/generate_primes")
async def generate_primes_number(i: User):
    request_id = str(uuid.uuid4())
    print(request_id)
    redis_client.hset(request_id, "status", "pending")
    generate_primes_task.delay(request_id, i.N)
    return {"request_id": request_id, "status": "pending"}


@celery.task
def generate_primes_task(request_id, n):
    print("LEVEL")
    result = generate_primes.apply_async(args=[n])
    print(result)
    update_status.apply_async(args=[request_id, result.id])


@celery.task
def update_status(request_id, result_id):
    result = celery.AsyncResult(result_id)
    if result.ready():
        redis_client.hset(request_id, "status", "processed")
        redis_client.hset(request_id, "result", result.get())
    else:
        update_status.apply_async(args=[request_id, result_id], countdown=1)


@app.get("/check_status")
async def check_status(request_id: str):
    status = redis_client.hget(request_id, "status")
    result = redis_client.hget(request_id, "result")

    if status:
        return {"request_id":request_id, "status":status, "result":result}
    else:
        raise HTTPException(status_code=404, detail="invalid request")
