import redis
import json
from server import r

def stream_temperature_reading(sensor_id:str, timestamp:str, temperature_c:float) -> str:
    entry = {
        "sensor_id": sensor_id,
        "temperature_c": json.dumps(temperature_c)
    }
    r.xadd("readings", entry, id=f"{timestamp}-{sensor_id}", maxlen=300)

    return "ON" if temperature_c is not None else "OFF"
