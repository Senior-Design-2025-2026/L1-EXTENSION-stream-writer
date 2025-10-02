from flask import request, Flask, jsonify

from src.dummy.dummy_writer import dummy_writer
from src.real.virtualization import check_button_toggle, get_unit
from src.utils.stream_reading import hits_thresh_low, hits_thresh_high, stream_reading, get_temps
from src.setup.redis_client import r
from src.setup.task_queue import celery_client
from src.config import TEMPERATURE_PORT, HOST, MODE

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

@app.route("/nullData", methods=["POST"])
def stream_null():
    if request.method == "POST":
        data = request.get_json()
        timestamp = int(data.get("timestamp"))
        stream_reading(sensor_id="1", timestamp=timestamp)
        stream_reading(sensor_id="2", timestamp=timestamp)
        return jsonify({"status":"OK"}), 200
    return jsonify({"status":"ERROR"}), 500

@app.route("/temperatureData", methods=["POST"])
def handle_readings():
    if request.method == "POST" and MODE != "testing":
        r.set("systemStatus", "CONNECTED")

        data      = request.get_json()
        timestamp = int(data.get("timestamp"))              

        # process sensors
        ids = ["1", "2"]
        perform_toggle:list[bool] = []
        for id in ids:

            unplugged = data.get(f"sensor{id}Unplugged") 

            # sensor plugged in
            if not unplugged: 

                is_btn_on = data.get(f"sensor{id}Enabled")
                if is_btn_on:
                    curr_status_p = "ON"
                    temp = float(data.get(f"sensor{id}Temperature"))
                else:
                    curr_status_p = "OFF"
                    temp = None

                stream_reading(sensor_id=id, timestamp=timestamp, temperature_c=temp)

                r.set(f"sensor:{id}:unplugged", "false")

                toggle = check_button_toggle(sensor_id=id, curr_status_p=curr_status_p)
                perform_toggle.append(toggle)


                # threshold checks for automated mailing
                last_three = r.xrevrange(f"readings:{id}", "+", "-", count=3)
                temps = get_temps(last_three=last_three)

                # LOW TEMPERATURE READINGS
                timeout_l = r.get("timeout_l")                     # simple timer because this endpoint gets hit every 1 sec
                timeout_l = int(timeout_l) if timeout_l is not None else 0
                if (timeout_l <= 0):
                    max_min_thresh = r.get("maxMinThresh")
                    if hits_thresh_low(temps=temps, max_min_thresh=max_min_thresh):
                        users_df = r.get("users_df")
                        celery_client.send_task(
                            "email_min_thresh", 
                            kwargs={
                                "sensor_id": id,
                                "df": users_df,
                                "last_three_list": temps,
                            }
                        )
                        r.set("timeout_l", 10)
                else:
                    r.set("timeout_l", timeout_l-1)
                    
                # HOT TEMPERATURE READINGS
                timeout_h = r.get("timeout_h")
                timeout_h = int(timeout_h) if timeout_h is not None else 0
                if (timeout_h <= 0):
                    min_max_thresh = r.get("minMaxThresh")
                    if hits_thresh_high(temps=temps, min_max_thresh=min_max_thresh):
                        users_df = r.get("users_df")
                        celery_client.send_task(
                            "email_max_thresh", 
                            kwargs={
                                "sensor_id": id,
                                "df": users_df,
                                "last_three_list": temps,
                            }
                        )
                        r.set("timeout_h", 10)
                else:
                    r.set("timeout_h", timeout_h-1)

            # sensor unplugged 
            elif unplugged:
                stream_reading(sensor_id=id, timestamp=timestamp, temperature_c=None)    
                r.set(f"sensor:{id}:unplugged", "true")
                perform_toggle.append(False)

        unit = get_unit()

        response = jsonify(
            [
                unit,
                perform_toggle[0],
                perform_toggle[1],
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
    return response

# ===================================================
#                   ENTRY POINT
# ===================================================
if __name__ == "__main__":
    if MODE == "testing":
        r.set("systemStatus", "DUMMY DATA MODE")
        dummy_writer(r=r, celery_client=celery_client)
    else:
        app.run(
            debug=True,
            host=HOST,
            port=TEMPERATURE_PORT,
        )
