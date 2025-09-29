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

@app.route("/")
def health_check():
    return jsonify({"status":"OK"}), 200

@app.route("/temperatureData", methods=["POST"])
def handle_readings():
    try:
        print("READING RECEIVED")
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
            
            print("READING RETURNED")

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
