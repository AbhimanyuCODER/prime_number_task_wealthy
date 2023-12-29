from fastapi import FastAPI, HTTPException, BackgroundTasks
from celery import Celery
import redis
import uuid

app = FastAPI()
database = {}  # Taking manual db

app.config = {
    "CELERY_BROKER_URL": "redis://localhost:6379/0",
    "CELERY_RESULT_BACKEND": "redis://localhost:6379/0"
}

celery = Celery("app", broker=app.config["CELERY_BROKER_URL"])
celery.conf.update(app.config)
redis_client = redis.StrictRedis(host="localhost", port=6379, db=1)


def is_prime(num):
    if num <= 1:
        return False
    for i in range(2, int(num**0.5)+ 1):
        if num%i == 0:
            return False
    return True


@celery.task
def generate_primes(n):
    primes = []
    num = 1
    while len(primes) <= n:
        if is_prime(num):
            primes.append(num)
        num += 1
    return primes


#made this api to post the query to generate the firt n Prime

@app.post("/generate_primes")
async def generate_primes_number(n: int, background_tasks: BackgroundTasks):
    request_id = str(uuid.uuid4())
    redis_client.hset(request_id, "status", "pending")
    background_tasks.add_task(generate_primes_task, request_id, n)
    return {"request_id": request_id, "status": "pending"}


def generate_primes_task(request_id, n):
    result = generate_primes.apply_async(args=[n])
    update_status.apply_async(args=[request_id, result.id])


@celery.task
def update_status(request_id, result):
    redis_client.hset(request_id, "status", "processed")
    redis_client.hset(request_id, "result", result)


#made this api to get the status
@app.get("/check_status")
async def check_status(request_id: str):
    status = redis_client.hget(request_id, "status")
    result = redis_client.hget(request_id, "result")

    if status:
        return {"request_id": request_id, "status": status, "result": result}
    else:
        raise HTTPException(status_code=404, detail="Invalid request")
