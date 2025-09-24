import os, time, random, redis

SOCK = os.getenv("SOCK", "6379")
print(f"*** UNIX SOCK {SOCK} ***")

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

while True:
    t = int(time.time())
    for i in ("1", "2"):
        entry = {"sensor_id": i, "temperature_c": f"{random.uniform(15, 45):.2f}"}
        r.xadd("readings", entry, id=f"{t}-{i}", maxlen=300)
        readings = r.xrange("readings", min="-", max="+", count=5)
    time.sleep(1)
