import json

from src.setup.redis_client import r

def stream_reading(sensor_id, timestamp, temperature_c=None):
    entry = {
        "sensor_id": sensor_id,
        "temperature_c": json.dumps(temperature_c)
    }

    # redis XADD collision from user action toggle because we parameterize stream id.
    # we just ignore as we have already gotten the correct reading.
    try:
        r.xadd(f"readings:{sensor_id}", entry, id=timestamp, maxlen=300)
    except:
        return 

def get_temps(last_three):
    temps = []
    for id, entry in last_three:
        raw = entry.get("temperature_c")
        temperature_c = float(raw) if raw not in (None, "", "NaN", "null") else None
        if temperature_c:
            temps.append(temperature_c)
    return temps

def hits_thresh_low(temps, max_min_thresh):
    if len(temps) == 3:
        print("TEMPS", temps)
        print("maxMinThresh", max_min_thresh)
        return all(t < float(max_min_thresh) for t in temps)
    return False

def hits_thresh_high(temps, min_max_thresh):
    if len(temps) == 3:
        print("TEMPS", temps)
        print("minMaxThresh", min_max_thresh)
        return all(t > float(min_max_thresh) for t in temps)
    return False

