"""
worker_node.py
Enhanced Worker Node with:
- Real ML: Anomaly Detection + Trend Forecast
- Menu-driven: Local or ngrok
- Auto IP detection
- All print() in terminal
Run: python worker_node.py
"""

import socket
import json
import threading
import requests
import time
import numpy as np
from collections import deque
from flask import Flask, request, jsonify, render_template_string
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression
import sys
import warnings

warnings.filterwarnings("ignore")

# ==============================
# CONFIG
# ==============================
WORKER_PORT = 5001
HEARTBEAT_INTERVAL = 2
ML_WINDOW = 50  # Rolling window for ML


# ==============================
# PRINT FIX (Terminal only)
# ==============================
def print_log(*args, **kwargs):
    msg = ' '.join(map(str, args))
    end = kwargs.get('end', '\n')
    flush = kwargs.get('flush', False)
    sys.stdout.write(msg + end)
    if flush: sys.stdout.flush()


import builtins

builtins.print = print_log


# ==============================
# ML PROCESSOR
# ==============================
class MLProcessor:
    def __init__(self):
        self.values = deque(maxlen=ML_WINDOW)
        self.timestamps = deque(maxlen=ML_WINDOW)
        self.iforest = IsolationForest(contamination=0.1, random_state=42)
        self.regressor = LinearRegression()
        self.trained = False

    def add(self, value, ts):
        self.values.append(value)
        self.timestamps.append(ts)

    def detect_anomalies(self):
        if len(self.values) < 10:
            return []
        X = np.array(self.values).reshape(-1, 1)
        scores = self.iforest.fit_predict(X)
        anomalies = [
            {'value': v, 'score': float(s), 'ts': self.timestamps[i]}
            for i, (v, s) in enumerate(zip(self.values, scores))
            if s == -1
        ]
        return anomalies

    def predict_trend(self):
        if len(self.values) < 5:
            return None, []
        X = np.array(self.timestamps).reshape(-1, 1)
        y = np.array(self.values)
        self.regressor.fit(X, y)
        slope = float(self.regressor.coef_[0])
        next_ts = np.array([[self.timestamps[-1] + i] for i in range(1, 6)])
        forecast = self.regressor.predict(next_ts).tolist()
        return slope, forecast

    def train(self):
        if len(self.values) >= 10:
            X = np.array(self.values).reshape(-1, 1)
            self.iforest.fit(X)
            self.trained = True


# ==============================
# WORKER NODE
# ==============================
class WorkerNode:
    def __init__(self, master_url, ngrok_url=None):
        self.master_url = master_url.rstrip('/')
        self.ip = socket.gethostbyname(socket.gethostname())
        self.ml = MLProcessor()
        self.processed = 0
        self.start_time = time.time()
        self.ngrok_url = ngrok_url
        self.register()
        threading.Thread(target=self.heartbeat, daemon=True).start()

    def register(self):
        payload = {'ip': self.ip, 'type': 'local'}
        if self.ngrok_url:
            payload.update({'type': 'ngrok', 'url': self.ngrok_url})
        for _ in range(5):
            try:
                requests.post(f"{self.master_url}/register_worker", json=payload, timeout=5)
                print(f"Registered → {payload['type']}")
                if self.ngrok_url:
                    print(f"  URL: {self.ngrok_url}")
                return
            except:
                time.sleep(2)
        print("Failed to register.")

    def heartbeat(self):
        while True:
            try:
                requests.post(f"{self.master_url}/heartbeat", json={'ip': self.ip}, timeout=1)
            except:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def process(self, data_batch):
        start = time.time()
        values = []
        for item in data_batch:
            val = item.get('value', 0)
            ts = item.get('timestamp', time.time())
            self.ml.add(val, ts)
            values.append(val)
            self.processed += 1

        self.ml.train()
        anomalies = self.ml.detect_anomalies()
        slope, forecast = self.ml.predict_trend()

        return {
            'processed_value': sum(values),
            'count': len(values),
            'average': np.mean(values),
            'ml_anomalies': anomalies,
            'trend_slope': slope,
            'forecast_next_5': forecast,
            'processing_time': time.time() - start,
            'worker_id': self.ip,
            'total_processed': self.processed
        }


# ==============================
# FLASK + SOCKET
# ==============================
app = Flask(__name__)
global worker


@app.route('/process', methods=['POST'])
def process_task():
    data = request.json
    result = worker.process(data['data'])
    result['task_id'] = data['task_id']
    return jsonify(result)


def handle_socket(client):
    try:
        data = b''
        while True:
            chunk = client.recv(4096)
            if not chunk: break
            data += chunk
            try:
                json.loads(data.decode()); break
            except:
                continue
        task = json.loads(data.decode())
        result = worker.process(task['data'])
        result['task_id'] = task['task_id']
        client.sendall(json.dumps(result).encode())
    except Exception as e:
        client.sendall(json.dumps({'error': str(e)}).encode())
    finally:
        client.close()


# ==============================
# WORKER UI (updated)
# ==============================
WORKER_HTML = """
<!DOCTYPE html>
<html><head><title>Worker {{ip}}</title>
<meta http-equiv="refresh" content="3">
<style>
  body{font-family:system-ui;background:#f0f4f8;padding:2rem;}
  .card{background:#fff;padding:1.5rem;border-radius:12px;box-shadow:0 4px 12px rgba(0,0,0,.08);max-width:600px;margin:auto;}
  h1{color:#2c3e50;margin:0 0 1rem;}
  .stat{font-size:1.1rem;margin:.4rem 0;color:#2c3e50;}
  .badge{padding:.2rem .5rem;border-radius:4px;font-size:.8rem;color:#fff;}
  .local{background:#3498db;} .ngrok{background:#9b59b6;}
</style></head>
<body>
<div class="card">
<h1>Worker {{ip}}</h1>
<div class="stat"><strong>Processed:</strong> {{processed}}</div>
<div class="stat"><strong>Uptime:</strong> {{uptime}} s</div>
<div class="stat"><strong>Master:</strong> {{master}}</div>
<div class="stat"><strong>Mode:</strong> <span class="badge {{mode}}">{{mode.upper()}}</span></div>
<div class="stat"><strong>Anomalies (window):</strong> {{anomalies}}</div>
<div class="stat"><strong>Trend Slope:</strong> {{slope}}</div>
</div>
</body></html>
"""


@app.route('/')
def worker_ui():
    if not worker:
        return "Worker not initialized", 500

    uptime = time.time() - worker.start_time
    mode = 'ngrok' if worker.ngrok_url else 'local'

    # Use worker's own ML state
    current_anomalies = len(worker.ml.detect_anomalies())
    slope, _ = worker.ml.predict_trend()
    slope_str = f"{slope:.4f}" if slope is not None else "N/A"

    return render_template_string(
        WORKER_HTML,
        ip=worker.ip,
        processed=worker.processed,
        uptime=round(uptime, 1),
        master=worker.master_url,
        mode=mode,
        anomalies=current_anomalies,
        slope=slope_str
    )


# ==============================
# MENU DRIVEN LAUNCHER
# ==============================
def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"


def menu():
    local_ip = get_local_ip()
    print("\n" + "=" * 60)
    print("   WORKER NODE - IoT Edge + ML")
    print("=" * 60)
    print("   [1] Local Worker (same network)")
    print("   [2] ngrok Worker (remote/internet)")
    print("   [3] Exit")
    print("=" * 60)
    while True:
        c = input("Choice (1-3): ").strip()
        if c in ['1', '2', '3']: return c, local_ip


def get_master_url(local_ip):
    default = f"http://{local_ip}:5000"
    url = input(f"Master URL [default: {default}]: ").strip()
    return url or default


def get_ngrok_url():
    print("\nNGROK SETUP:")
    print("   1. Open new terminal")
    print("   2. Run: ngrok http 5001")
    print("   3. Copy the HTTPS URL")
    url = input("\nPaste ngrok URL: ").strip()
    if url and not url.startswith("http"):
        url = "https://" + url
    return url


if __name__ == '__main__':
    choice, local_ip = menu()
    if choice == '3':
        print("Goodbye!")
        sys.exit(0)

    master_url = get_master_url(local_ip) if choice == '1' else input("Master URL (http/https): ").strip()
    ngrok_url = get_ngrok_url() if choice == '2' else None

    worker = WorkerNode(master_url, ngrok_url)

    if ngrok_url:
        print(f"\nStarting ngrok Worker → {ngrok_url}")
        app.run(host='0.0.0.0', port=WORKER_PORT, debug=False, use_reloader=False)
    else:
        print(f"\nStarting Local Worker → {master_url}")
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('0.0.0.0', WORKER_PORT))
        s.listen(5)
        print("Listening on port 5001...")
        while True:
            client, _ = s.accept()
            threading.Thread(target=handle_socket, args=(client,), daemon=True).start()
