# Flask app (app.py)
from flask import Flask, jsonify, request
from celery import Celery
import redis , uuid

app = Flask(__name__)
database={} # taking manual db
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
redis_client = redis.StrictRedis(host='localhost', port=6379, db=1)


def is_prime(num):
    if num <= 1:
        return False
    for i in range(2, int(num**0.5) + 1):
        if num %i == 0:
            return False
    return True

@celery.task
def generate_primes(n):
    c = n
    primes = []
    num = 2
    while len(primes) < c:
        if is_prime(num):
            primes.append(num)
        num += 1
    return primes

#made this api to post the query to generate the firt n Prime
@app.route('/generate_primes', methods=['POST'])
def generate_primes_number():
    data = request.get_json()
    n = data.get('n')
    request_id  = str(uuid.uuid4())
    redis_client.hset(request_id, 'status', 'pending')
    generate_primes.apply_async(args=[n], link=update_status.s(request_id))
    return jsonify({'request_id': request_id, 'status': 'pending'})

@celery.task
def update_status(request_id, result):
    redis_client.hset(request_id, 'status', 'processed')
    redis_client.hset(request_id, 'result', result)

#made this api to get the status
@app.route('/check_status', methods=['GET'])
def check_status():
    request_id = request.data.get('request_id')
    status = redis_client.hget(request_id, 'status')
    result = redis_client.hget(request_id, 'result')

    if status:
        return jsonify({'request_id': request_id, 'status': status, 'result': result})
    else:
        return jsonify({'error': 'Invalid request'})
