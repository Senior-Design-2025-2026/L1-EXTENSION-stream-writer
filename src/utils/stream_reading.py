from celery import Celery
import redis
import json

from src.setup.redis_client import r
from src.setup.task_queue import celery_client

def stream_temperature_reading(sensor_id:str, timestamp:str, temperature_c:float) -> str:
    entry = {
        "sensor_id": sensor_id,
        "temperature_c": json.dumps(temperature_c)
    }
    r.xadd("readings", entry, id=f"{timestamp}-{sensor_id}", maxlen=300)

    return "ON" if temperature_c is not None else "OFF"
