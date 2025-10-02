from typing import Union

from src.setup.task_queue import celery_client

def add_reading_to_db(sensor_id:str, timestamp:Union[int, None], temperature_c:Union[float, None]):
    celery_client.send_task(
        "insert_record", 
        kwargs={
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature_c": temperature_c
        }
    )
