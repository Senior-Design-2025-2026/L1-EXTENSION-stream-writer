import os
import time
import random
import redis
from celery import Celery

SOCK = os.getenv("SOCK")

# ===================================================
#                CELERY TASK QUEUE
# ===================================================
celery_client = Celery(
    main=__name__,
    broker=f"redis+socket://{SOCK}",
)

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

# ==================================================
#       REDIS STREAM --> CELERY FOR DB WRITES 
# ==================================================
while True:
    t = int(time.time())
    temp_1 = random.uniform(15,45)
    entry_1 = {"sensor_id": "1", "temperature_c": f"{temp_1:.2f}"}

    temp_2 = random.uniform(15,45)
    entry_2 = {"sensor_id": "2", "temperature_c": f"{temp_2:.2f}"}

    r.xadd("readings", entry_1, id=f"{t}-1", maxlen=300)
    r.xadd("readings", entry_2, id=f"{t}-2", maxlen=300)
        
    print("SENDING READING 1", entry_1)
    celery_client.send_task(
        "insert_record", 
        kwargs={
            "sensor_id":1,
            "timestamp":t,
            "temperature_c":temp_1
        }
    )

    print("SENDING READING 2", entry_2)
    celery_client.send_task(
        "insert_record", 
        kwargs={
            "sensor_id":2,
            "timestamp":t,
            "temperature_c":temp_2
        }
    )

    time.sleep(1)
