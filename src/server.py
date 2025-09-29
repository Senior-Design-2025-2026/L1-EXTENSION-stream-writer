import os
import time
import redis
from celery import Celery
from flask import request, Flask, jsonify
from dummy.dummy_writer import dummy_writer
from real.virtualization import check_unit_toggle, check_button_toggle
from utils.unit_methods import system_units, round_c
from utils.db_methods import add_reading_to_db
from utils.stream_reading import stream_temperature_reading

# ===================================================
#              ENVIRONMENT VARIABLES
# ===================================================
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

# status setup
r.set("physical:1:status", "OFF")
r.set("physical:2:status", "OFF")

r.set("virtual:1:status", "OFF")
r.set("virtual:1:desired_status", "None")
r.set("virtual:1:wants_toggle", "false")

r.set("virtual:2:status", "OFF")
r.set("virtual:2:desired_status", "None")
r.set("virtual:2:wants_toggle", "false")

r.set("physical:unit", "c")
r.set("virtual:unit", "f")

# ===================================================
#       HTTP ENDPOINTS FOR EMBEDDED SYSTEM
r.set("physical:2:status", "OFF")

# ===================================================
#       HTTP ENDPOINTS FOR EMBEDDED SYSTEM
# ===================================================
app = Flask(__name__)

@app.route("/")
def health_check():
    return jsonify({"status":"OK"}), 200

@app.route("/temperatureData", methods=["POST"])
def handle_readings():
    try:
        if request.method == "POST" and MODE != "testing":
            # 1. retrieve message from device
            data      = request.get_json()
            timestamp = data.get("timestamp")

            # 2. get reading information
            timestamp = int(timestamp)
            temp_c_1 = round_c(data.get("sensor1Temperature"))
            temp_c_2 = round_c(data.get("sensor1Temperature"))

            # 3. add to redis stream; returns status of physical button
            curr_status_p_1:str = stream_temperature_reading(sensor_id="1", timestamp=timestamp, temperature_c=temp_c_1)
            curr_status_p_2:str = stream_temperature_reading(sensor_id="2", timestamp=timestamp, temperature_c=temp_c_2)

            # 4. add to database
            add_reading_to_db(sensor_id="1", timestamp=timestamp, temperature_c=temp_c_1)
            add_reading_to_db(sensor_id="2", timestamp=timestamp, temperature_c=temp_c_2)

            # 5. prepare response body
            perform_virtual_btn_toggle_1 = check_button_toggle(sensor_id="1", curr_status_p=curr_status_p_1)
            perform_virtual_btn_toggle_2 = check_button_toggle(sensor_id="2", curr_status_p=curr_status_p_2)
            perform_virtual_unit_toggle  = check_unit_toggle()

            # push the reading to the database
            add_reading_to_db(sensor_id="1", timestamp=timestamp, temperature_c=temp_1)
            add_reading_to_db(sensor_id="2", timestamp=timestamp, temperature_c=temp_2)

            response = jsonify(
                [
                    system_units(),
                    perform_virtual_btn_toggle_1, 
                    perform_virtual_btn_toggle_2,
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
        print("")
        print("[ERROR] Server")
        print("[ERROR]", e)
        print("[ERROR]", "returning defaults")
        response = jsonify(
            [
                "C", 
                False,
                False
            ]
        )
    return response, 200

# ===================================================
#                   ENTRY POINT
# ===================================================
if __name__ == "__main__":
    if MODE == "testing":
        dummy_writer(r=r, celery_client=celery_client)
    else:
        app.run(
            debug=True,
            host=HOST,
            port=TEMPERATURE_PORT,
        )
