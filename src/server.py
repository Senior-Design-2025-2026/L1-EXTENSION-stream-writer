import os
import time
import redis
from celery import Celery
from flask import request, Flask, jsonify
from dummy.dummy_writer import dummy_writer
import json

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

def stream_reading(sensor_id, timestamp, temperature_c):
    entry = {
        "sensor_id": f"{sensor_id}",
        "temperature_c": json.dumps(temperature_c)
    }
    r.xadd("readings", entry, id=f"{timestamp}-{sensor_id}", maxlen=300)

def add_reading_to_db(sensor_id, timestamp, temperature_c):
    celery_client.send_task(
        "insert_record", 
        kwargs={
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature_c": temperature_c
        }
    )

def system_units():
    # TODO - figure something out here. way it is currently set up is chopped
    return "C"

def sensor_1_toggled():
    # TODO - figure something out here. way it is currently set up is chopped
    return False

def sensor_2_toggled():
    # TODO - figure something out here. way it is currently set up is chopped
    return False

def round_c(temperature_c):
    return None if temperature_c is None else round(float(temperature_c), 2)

@app.route("/temperatureData", methods=["POST"])
def handle_incoming_readings():
    try:
        if request.method == "POST":
            data      = request.get_json()
            timestamp = data.get("timestamp")

            temp_1 = round_c(data.get("sensor1Temperature"))
            stream_reading(sensor_id=1, timestamp=timestamp, temperature_c=temp_1)
            add_reading_to_db(sensor_id=1, timestamp=timestamp, temperature_c=temp_1)

            temp_2 = round_c(data.get("sensor2Temperature"))
            stream_reading(sensor_id=2, timestamp=timestamp, temperature_c=temp_2)
            add_reading_to_db(sensor_id=2, timestamp=timestamp, temperature_c=temp_2)

            response = jsonify(
                [
                    system_units(),
                    sensor_1_toggled(), 
                    sensor_2_toggled(),
                ]
            )
        else:
            response = jsonify(
                [
                    "C", 
                    False,
                    False
                ]
            )
    except Exception as e:
        response = jsonify(
            [
                "C", 
                False,
                False
            ]
        )
    return response, 200

if __name__ == "__main__":
    if MODE == "testing":
        dummy_writer(r=r, celery_client=celery_client)
    else:
        app.run(
            debug=True,
            host=HOST,
            port=TEMPERATURE_PORT,
        )
