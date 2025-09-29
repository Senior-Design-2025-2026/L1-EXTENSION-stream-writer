from src.setup.redis_client import r

def dummy_check_button_toggle(sensor_id:str):
    wants_toggle = r.get(f"virtual:{sensor_id}:wants_toggle")
    curr = r.get(f"virtual:{sensor_id}:status")

    if wants_toggle == "true":
        new = "ON" if curr == "OFF" else "OFF"
        r.set(f"virtual:{sensor_id}:status", new)
        r.set(f"virtual:{sensor_id}:wants_toggle", "false")
        return new

    return curr
