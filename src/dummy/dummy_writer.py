import time
import random
import redis

from src.dummy.virtualization import dummy_check_button_toggle
from src.setup.task_queue import celery_client
from src.setup.redis_client import r

def dummy_stream_record(sensor_id:str, timestamp:int):
    temp = random.uniform(25,35)
    entry = {
        "sensor_id":sensor_id,
        "temperature_c":f"{temp:.2f}",
    }

    # check for toggle
    status = dummy_check_button_toggle(sensor_id=sensor_id)

    # write if sensor is "ON"
    if status == "ON":
        r.xadd("readings", entry, id=f"{timestamp}-{sensor_id}", maxlen=300)

        celery_client.send_task(
            "insert_record", 
            kwargs={
                "sensor_id":sensor_id,
                "timestamp":timestamp,
                "temperature_c":temp
            }
        )

def dummy_writer(r, celery_client):
    # SETUP - start both sensors on
    r.set("virtual:1:status", "ON")
    r.set("virtual:2:status", "ON")

    while True:
        timestamp = int(time.time())

        dummy_stream_record(sensor_id="1", timestamp=timestamp)
        dummy_stream_record(sensor_id="2", timestamp=timestamp)
            
        time.sleep(1)
