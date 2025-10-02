import os
import redis
import time

from src.config import SOCK

# ==================================================
#                    REDIS STREAM 
# ==================================================
deadline = time.time() + 30
r = None
while time.time() < deadline:
    if os.path.exists(SOCK):
        try:
            r = redis.Redis(
                unix_socket_path=SOCK,
                decode_responses=True
            )
            r.ping()
            break
        except Exception:
            time.sleep(0.5)

    else:
        time.sleep(0.2)

if r is None:
    raise RuntimeError(f"Redis socket not ready at {SOCK}")

# status setup
r.set("physical:1:status", "OFF")
r.set("physical:2:status", "OFF")

r.set("virtual:1:status", "OFF")
r.set("virtual:1:wants_toggle", "false")

r.set("virtual:2:status", "OFF")
r.set("virtual:2:wants_toggle", "false")

r.set("temperatureUnit", "C")
