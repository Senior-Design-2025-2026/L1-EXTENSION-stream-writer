from celery import Celery
import os
from flask import request, Flask, jsonify

from src.dummy.dummy_writer import dummy_writer
from src.real.virtualization import check_unit_toggle, check_button_toggle
from src.utils.unit_methods import system_units, round_c
from src.utils.db_methods import add_reading_to_db
from src.utils.stream_reading import stream_temperature_reading
from src.setup.redis_client import r
from src.setup.task_queue import celery_client
from src.config import TEMPERATURE_PORT, HOST, SOCK, MODE

# ===================================================
#       HTTP ENDPOINTS FOR EMBEDDED SYSTEM
# ===================================================
app = Flask(__name__)

TIMEOUT_S = 10

@app.route("/")
def health_check():
    return jsonify({"status":"OK"}), 200

@app.route("/turnOFF", methods=["GET"])
def turn_off():
    if request.method == "GET":
        r.set("systemStatus", "DISCONNECTED")
        return jsonify({"status":"OK"}), 200
    return jsonify({"status":"ERROR"}), 500

@app.route("/temperatureData", methods=["POST"])
def handle_readings():
    #   data comes in the form
    # 
    #   timestamp
    #   sensor[1/2]:
    #    - temp?
    #    - sensor[1/2]Unplugged?
    #
    # if this endpoint is reached then the system is turned on
    if request.method == "POST" and MODE != "testing":
        r.set("systemStatus", "CONNECTED", ex=TIMEOUT_S)                  

        data      = request.get_json()
        timestamp = int(data.get("timestamp"))

        print("")
        print("DATA", data)
        print("timestamp", timestamp)

        try:
            temp_c_1 = round_c(data.get("sensor1Temperature"))

            if temp_c_1 is not None:
                temp_c_1 = round_c(data.get("sensor1Temperature"))
                curr_status_p_1:str = stream_temperature_reading(sensor_id="1", timestamp=timestamp, temperature_c=temp_c_1)
                toggle_1 = check_button_toggle(sensor_id="1", curr_status_p=curr_status_p_1)
                # add_reading_to_db(sensor_id="1", timestamp=timestamp, temperature_c=temp_c_1)
            else:
                r.set(f"virtual:1:status", "UNPLUGGED")
                toggle_1 = False
        except Exception as e:
            print("EXCEPTION SENSOR 1", e)
            toggle_1 = False

        try:
            temp_c_2 = round_c(data.get("sensor2Temperature"))

            if temp_c_2 is not None:
                temp_c_2 = round_c(data.get("sensor2Temperature"))
                curr_status_p_2:str = stream_temperature_reading(sensor_id="2", timestamp=timestamp, temperature_c=temp_c_2)
                toggle_2 = check_button_toggle(sensor_id="2", curr_status_p=curr_status_p_2)
                # add_reading_to_db(sensor_id="2", timestamp=timestamp, temperature_c=temp_c_2)
            else:
                r.set(f"virtual:2:status", "UNPLUGGED")
                toggle_2 = False
        except Exception as e:
            print("EXCEPTION SENSOR 2", e)
            toggle_2 = False

        perform_virtual_unit_toggle  = check_unit_toggle()

        # check the maximum floor thresh and minimum top thresh to see if there is a hit
        # last_three = r.revxrange(f"readings:{sensor_id}", "+", "-", count=3)
        # json_df = r.get("users_df")
        # check_thresh(last_three=last_three, sensor_id=sensor_id, df=df)

        response = jsonify(
            [
                "C",
                toggle_1,
                toggle_2,
            ]
        )
    else:
        # no change to physical device
        response = jsonify(
            [
                "C", 
                False,
                False
            ]
        )
    return response

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
