"""JemuranGuard v2 - python jemuran_guard.py [edge|receiver|detector]
edge     = HP A (dekat jemuran): sensing + edge processing + sirine lokal + buffer offline
receiver = HP B (HP utama): notifikasi + ACK + heartbeat/baterai + perintah (SNOOZE/SIRINE_OFF)
detector = Laptop: log telemetri (dasbor web: jemuran_dashboard.html)
"""
import sys, ssl, subprocess, json, time, math, os, threading
from collections import deque
import paho.mqtt.client as mqtt

BROKER, PORT = "broker.hivemq.com", 8883          # MQTT over TLS
PID = "jemuran_guard_kelompok_07"                  # ganti agar unik
T = {k: f"jemuranguard/{PID}/{k}" for k in ("telemetry", "alerts", "ack", "heartbeat", "cmd")}
LUX_DROP, RMS_TH = 80.0, 2.5
WIN, BASE_WIN, COOLDOWN = 20, 120, 60
ROLE = sys.argv[1] if len(sys.argv) > 1 else "edge"

def mk(cid):
    try: c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=cid)
    except AttributeError: c = mqtt.Client(client_id=cid)
    c.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
    return c

def batt():
    try:
        b = json.loads(subprocess.run(["termux-battery-status"], capture_output=True, text=True, timeout=8).stdout)
        return b.get("percentage")
    except Exception: return None

def heartbeat_loop(c, dev, extra=lambda: {}):
    while True:
        c.publish(T["heartbeat"], json.dumps({"device": dev, "battery": batt(), "ts": time.time(), **extra()}), qos=0)
        time.sleep(10)

if ROLE == "edge":
    st = {"siren": False, "snooze_until": 0, "sent": 0}
    buf = deque(maxlen=500)                       # buffer offline
    c = mk("jg-edge-A")

    def on_connect(cl, u, f, rc, *a): cl.subscribe(T["cmd"], qos=1); cl.subscribe(T["ack"], qos=1); print("[EDGE] online")
    def on_message(cl, u, m):
        d = json.loads(m.payload.decode())
        if m.topic == T["ack"] or d.get("cmd") == "SIRINE_OFF": st["siren"] = False
        if d.get("cmd") == "SNOOZE": st["snooze_until"] = time.time() + 600; st["siren"] = False
    c.on_connect, c.on_message = on_connect, on_message
    c.connect_async(BROKER, PORT, 60); c.loop_start()
    threading.Thread(target=heartbeat_loop, args=(c, "EDGE_A", lambda: {"siren": st["siren"], "buffered": len(buf)}), daemon=True).start()

    def siren():                                  # alarm lokal agar teman kos mendengar
        while True:
            if st["siren"]:
                os.system("termux-tts-speak 'Awas! Angkat jemuran! Mau hujan!'")
            time.sleep(4)
    threading.Thread(target=siren, daemon=True).start()

    # Pembaca sensor streaming: simpan nilai TERAKHIR (light sensor hanya kirim saat nilai berubah)
    latest = {"light": None, "accel": None}
    def stream_sensor():
        dec = json.JSONDecoder()
        while True:
            try:
                p = subprocess.Popen(["termux-sensor", "-s", "light,accelerometer", "-d", "200"],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                buf = ""
                for line in p.stdout:
                    buf += line
                    try: obj, _ = dec.raw_decode(buf.strip())
                    except ValueError: continue
                    buf = ""
                    for n, v in obj.items():
                        n = n.lower(); vals = v.get("values")
                        if not vals: continue
                        if "light" in n: latest["light"] = vals[0]
                        elif "accel" in n and "linear" not in n and "uncal" not in n: latest["accel"] = vals[:3]
                print("[EDGE] stream sensor berhenti:", p.stderr.read()[:200])
            except Exception as e:
                print("[EDGE] stream error:", e)
            time.sleep(2)
    threading.Thread(target=stream_sensor, daemon=True).start()

    def baca():
        if latest["accel"] is None:
            raise ValueError("accelerometer belum terbaca (cek Termux:API & izin sensor)")
        if latest["light"] is None:
            raise ValueError("light sensor belum ada data - tutup/buka sensor cahaya HP dengan tangan sekali")
        return latest["light"], latest["accel"]

    lux_h, dev_h, last_alert = deque(maxlen=BASE_WIN), deque(maxlen=WIN), 0
    while True:
        try:
            lux, (x, y, z) = baca()
            lux_h.append(lux); drop = max(lux_h) - lux                        # Processing 1
            dev_h.append(math.sqrt(x*x + y*y + z*z) - 9.81)
            rms = math.sqrt(sum(d*d for d in dev_h) / len(dev_h))              # Processing 2
            status = "JEMURAN_BAHAYA" if drop > LUX_DROP and rms > RMS_TH else "AMAN"   # Fusion
            p = json.dumps({"device_id": "EDGE_A", "light_lux": lux, "lux_drop": drop, "rms_swing": rms,
                            "status": status, "ts": time.time()})
            buf.append(p)
            if c.is_connected():
                while buf: c.publish(T["telemetry"], buf.popleft(), qos=1)    # kirim ulang yg tertahan
            if status == "JEMURAN_BAHAYA" and time.time() > st["snooze_until"]:
                if time.time() - last_alert > COOLDOWN:
                    last_alert = time.time(); st["siren"] = True
                    if c.is_connected(): c.publish(T["alerts"], p, qos=1)
            print(f"{status} | drop {drop:.1f} lx | RMS {rms:.2f} m/s2 | siren={st['siren']}")
            time.sleep(0.5)
        except Exception as e:
            print("sensor:", e); time.sleep(1)

elif ROLE == "receiver":
    S = {"d": {}, "last": 0.0, "online": False, "alert_ts": 0.0, "bahaya": False,
         "hb": {}, "lat": None, "alerts": 0, "conn": False}
    lock = threading.Lock()

    def run(cmd):                                   # non-blocking, callback MQTT tidak tertahan
        subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def notif(title, text, vib=3000):
        run(f"termux-vibrate -f -d {vib}")
        run(f"termux-notification --id jg --title '{title}' --content '{text}' --priority max --sound")

    c = mk("jg-recv-B")
    def on_connect(cl, u, f, rc, *a):
        S["conn"] = True
        cl.subscribe([(T["alerts"], 1), (T["telemetry"], 0), (T["heartbeat"], 0)])
    def on_disconnect(cl, u, rc, *a): S["conn"] = False

    def on_message(cl, u, m):
        try: d = json.loads(m.payload.decode())
        except ValueError: return
        k = m.topic.rsplit("/", 1)[-1]
        now = time.time()
        with lock:
            if k == "telemetry":
                S["d"], S["last"] = d, now
                S["lat"] = max(0, now - d.get("ts", now))
                if not S["online"]:
                    S["online"] = True; run("termux-toast 'Edge A online'")
                if d.get("status") == "AMAN" and S["bahaya"]:
                    S["bahaya"] = False
                    notif("Jemuran aman", "Kondisi sudah normal kembali", 300)
            elif k == "heartbeat":
                S["hb"][d.get("device", "?")] = d
            elif k == "alerts" and d.get("status") == "JEMURAN_BAHAYA":
                S["bahaya"] = True; S["alerts"] += 1; S["alert_ts"] = now
                notif("ANGKAT JEMURAN!", "Mendung tebal + jemuran bergoyang kencang!")
                cl.publish(T["ack"], json.dumps({"device": "RECEIVER_B", "alert_ts": d.get("ts"), "ts": now}), qos=1)

    c.on_connect, c.on_disconnect, c.on_message = on_connect, on_disconnect, on_message
    c.reconnect_delay_set(1, 5)                      # auto-reconnect cepat
    c.connect_async(BROKER, PORT, keepalive=15); c.loop_start()
    threading.Thread(target=heartbeat_loop, args=(c, "RECEIVER_B"), daemon=True).start()

    def bar(v, mx, w=24):
        n = int(min(1, v / mx) * w); return "#" * n + "." * (w - n)

    while True:                                      # tampilan live 4x per detik
        time.sleep(0.25)
        with lock:
            d, now = S["d"], time.time()
            if S["online"] and now - S["last"] > 6:  # watchdog Edge A
                S["online"] = False
                notif("Edge A OFFLINE", "HP di jemuran tidak mengirim data!", 1500)
            ha = S["hb"].get("EDGE_A", {})
            drop, rms = d.get("lux_drop", 0), d.get("rms_swing", 0)
            stt = d.get("status", "-")
            lat_txt = "-" if S["lat"] is None else "%.0f ms" % (S["lat"] * 1000)
            red, grn, rst = "\033[91m", "\033[92m", "\033[0m"
            col = red if stt == "JEMURAN_BAHAYA" else grn
            out = ["\033[H\033[J=== JEMURANGUARD RECEIVER (LIVE) ===",
                   f"Broker  : {'TERHUBUNG' if S['conn'] else 'TERPUTUS (reconnect...)'}",
                   f"Edge A  : {'ONLINE' if S['online'] else 'OFFLINE'}  | baterai A: {ha.get('battery','-')}%  | sirine: {'BUNYI' if ha.get('siren') else 'diam'}",
                   f"Status  : {col}{stt}{rst}",
                   f"Cahaya  : {d.get('light_lux',0):7.1f} lx",
                   f"LuxDrop : {drop:7.1f} lx  [{bar(drop, LUX_DROP)}] batas {LUX_DROP:.0f}",
                   f"RMS     : {rms:7.2f} m/s2 [{bar(rms, RMS_TH)}] batas {RMS_TH}",
                   f"Latensi : {lat_txt} (perkiraan)",
                   f"Alert   : {S['alerts']}x" + (f"  (terakhir {now - S['alert_ts']:.0f} dtk lalu)" if S["alerts"] else "")]
        print("\n".join(out), flush=True)

elif ROLE == "detector":
    c = mk("jg-detector")
    c.on_connect = lambda cl, u, f, rc, *a: cl.subscribe(f"jemuranguard/{PID}/#")
    c.on_message = lambda cl, u, m: print(m.topic.split("/")[-1], m.payload.decode())
    c.connect(BROKER, PORT, 60); c.loop_forever()
else:
    print("Gunakan: python jemuran_guard.py [edge|receiver|detector]")