import os
import time
import random
import redis
from celery import Celery
from flask import request, Flask

TEMPERATURE_PORT = os.getenv("TEMPERATURE_PORT")
SOCK             = os.getenv("SOCK")

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


# ===================================================
#                FLASK ENDPOINT
# ===================================================
# switching the way that we are doing the communication 
# pi does not have the processing power to handle 
# all of the containers (redis, postgres, dash app, celery)
# this is simply an endpoint for the embedded code to hit 
# to send data.

# methods:
# 1. receive data from the temperature sensor
# 2. send/receive current status of buttons         (TODO - pull from the redis stream)
app = Flask(__name__)

@app.route("/temperatureData", methods=["POST"])
def receive_from_pi():
    print("RECEIVED!", request.get_json())
    if request.method == "POST":
        data = request.get_json()

        # data is coming from the pi in a json like so:
        #
        #     {
        #         "sensor1Temperature": t1,
        #         "sensor2Temperature": t2
        #    }
        #
        t = int(time.time())
        temp_1 = data.get("sensor1Temperature")
        entry_1 = {"sensor_id": "1", "temperature_c": f"{temp_1:.2f}"}

        temp_2 = data.get("sensor2Temperature")
        entry_2 = {"sensor_id": "2", "temperature_c": f"{temp_2:.2f}"}

        # add to redis stream
        r.xadd("readings", entry_1, id=f"{t}-1", maxlen=300)
        r.xadd("readings", entry_2, id=f"{t}-2", maxlen=300)

        # add to postgres db
        celery_client.send_task(
            "insert_record", 
            kwargs={
                "sensor_id":1,
                "timestamp":t,
                "temperature_c":temp_1
            }
        )
        celery_client.send_task(
            "insert_record", 
            kwargs={
                "sensor_id":2,
                "timestamp":t,
                "temperature_c":temp_2
            }
        )

if __name__ == "__main__":
    print("STARTING FLASK SERVER:", TEMPERATURE_PORT)
    app.run(debug=True, port=TEMPERATURE_PORT)

