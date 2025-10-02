from src.setup.redis_client import r

# =========================================================== 
#                   BUTTON TOGGLE
# ===========================================================
def check_button_toggle(sensor_id:str, curr_status_p:str) -> bool:
    # 1. detect toggles
    was_toggle_p = _was_physical_toggle(sensor_id=sensor_id, curr_status_p=curr_status_p)
    was_toggle_v = _was_virtual_toggle(sensor_id=sensor_id)

    # 2. resolve toggle
    # a) collision (both toggled)
    if was_toggle_p and was_toggle_v:
        perform_virtual_toggle = _handle_collision(sensor_id=sensor_id, curr_status_p=curr_status_p)

    # b) physical toggled
    elif was_toggle_p:
        _handle_physical_toggle(sensor_id=sensor_id, curr_status_p=curr_status_p)
        perform_virtual_toggle = False   

    # c) virtual toggled
    elif was_toggle_v:
        _handle_virtual_toggle(sensor_id=sensor_id)
        perform_virtual_toggle = True

    # d) no toggle
    else:
        perform_virtual_toggle = False

    # 3. update physical status in redis if there was a virtual toggle
    if perform_virtual_toggle and curr_status_p == "ON":
        r.set(f"physical:{sensor_id}:status", "OFF")
    elif perform_virtual_toggle and curr_status_p == "OFF":
        r.set(f"physical:{sensor_id}:status", "ON")

    return perform_virtual_toggle

# -- BUTTON TOGGLE HELPERS

def _was_physical_toggle(sensor_id:str, curr_status_p:str) -> bool:
    prev_status_p = r.get(f"physical:{sensor_id}:status")
    return True if prev_status_p != curr_status_p else False

def _was_virtual_toggle(sensor_id:str) -> bool:
    wants_toggle = r.get(f"virtual:{sensor_id}:wants_toggle")
    return True if wants_toggle == "true" else False

def _handle_collision(sensor_id:str, curr_status_p:str) -> bool:
    # no logic here collision conflict; just setting the status to the physical devices!
    r.set(f"physical:{sensor_id}:status", curr_status_p)
    r.set(f"virtual:{sensor_id}:status", curr_status_p)
    r.set(f"virtual:{sensor_id}:feedback", f"[REJECTED] Button {sensor_id} toggle")
    return False

def _handle_physical_toggle(sensor_id:str, curr_status_p:str) -> None:
    r.set(f"physical:{sensor_id}:status", curr_status_p)

    r.set(f"virtual:{sensor_id}:status", curr_status_p)
    r.set(f"virtual:{sensor_id}:feedback", f"[Physical] Button {sensor_id} toggled")

def _handle_virtual_toggle(sensor_id:str) -> None:
    curr = r.get(f"virtual:{sensor_id}:status")

    if curr is not None and curr in ("ON", "OFF"):

        if curr == "ON":
            new = "OFF"
        else: 
            new = "ON"

        r.set(f"virtual:{sensor_id}:status", new)
        r.set(f"virtual:{sensor_id}:feedback", f"[Virtual] Button {sensor_id} toggled")
        r.set(f"virtual:{sensor_id}:wants_toggle", "false")

    else:
        print("[UNEXPECTED]", "curr:", curr)

# =========================================================== 
#                        UNIT TOGGLE
# ===========================================================
def get_unit():
    unit = r.get("temperatureUnit")
    return unit
