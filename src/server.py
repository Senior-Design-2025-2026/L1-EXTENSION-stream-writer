import os
import time
import redis
from celery import Celery
from flask import request, Flask, jsonify
from dummy.dummy_writer import dummy_writer

TEMPERATURE_PORT = os.getenv("TEMPERATURE_PORT")
HOST             = os.getenv("HOST")
SOCK             = os.getenv("SOCK")
MODE             = os.getenv("MODE")

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
# data is coming from the pi in a json like so:
#
#     {
#         "sensor1Temperature": t1,
#         "sensor2Temperature": t2
#    }
#
app = Flask(__name__)

@app.route("/")
def health_check():
    return jsonify({"status":"OK"}), 200

@app.route("/temperatureData", methods=["POST"])
def receive_from_pi():
    print("")
    print("RECEIVED!", request.get_json())
    print("")
    if request.method == "POST":
        data = request.get_json()

        timestamp = data.get("timestamp")
        temp_1    = data.get("sensor1Temperature")
        temp_2    = data.get("sensor2Temperature")

        print("")
        print("TIME", timestamp)
        print("TEMP 1", temp_1)
        print("TEMP 2", temp_2)
        print("")

        temp_1 = f"{temp_1:.2f}" if temp_1 is not None else ""
        temp_2 = f"{temp_2:.2f}" if temp_2 is not None else ""

        entry_1 = {"sensor_id": "1", "temperature_c": temp_1}
        entry_2 = {"sensor_id": "2", "temperature_c": temp_2}

        print("")
        print("ENTRY_1:", entry_1)
        print("ENTRY_2:", entry_2)
        print("")

        r.xadd("readings", entry_1, id=f"{timestamp}-1", maxlen=300)
        r.xadd("readings", entry_2, id=f"{timestamp}-2", maxlen=300)

        # add to postgres db
        celery_client.send_task(
            "insert_record", 
            kwargs={
                "sensor_id":1,
                "timestamp": timestamp,
                "temperature_c":temp_1
            }
        )

        celery_client.send_task(
            "insert_record", 
            kwargs={
                "sensor_id":2,
                "timestamp": timestamp,
                "temperature_c":temp_2
            }
        )

        res = jsonify(["C", True, True])
        print("RESPONSE:", res)
        print("")
        return res, 200

if __name__ == "__main__":
    if MODE == "testing":
        print("STARTING STREAM WRITER IN TESTING MODE (DUMMY WRITER)")
        dummy_writer(r=r)
    else:
        print("STARTING STREAM WRITER IN EMBEDDED MODE (SERVER)")
        app.run(
            debug=True,
            host="0.0.0.0",
            port=TEMPERATURE_PORT,
        )

