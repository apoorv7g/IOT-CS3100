"""
master_node.py
Enhanced Master Node with:
- Real-time Dashboard (all print → logs)
- ML results from Workers
- Live Chart
- ngrok + Local Support
Run: python master_node.py
"""

import serial
import serial.tools.list_ports
import socket
import json
import threading
import time
from collections import deque
from flask import Flask, jsonify, request, render_template_string
import requests
import logging
from logging.handlers import QueueHandler
import queue
import sys
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings("ignore")

app = Flask(__name__)

# ==============================
# CONFIGURATION
# ==============================
WORKER_PORT = 5001
BATCH_SIZE = 10
HEARTBEAT_INTERVAL = 2
HEARTBEAT_TIMEOUT = 5

# Data storage
sensor_data = deque(maxlen=1000)
worker_nodes = []
processing_results = []
log_entries = deque(maxlen=200)  # NEW: For UI logs

# ==============================
# LOGGING: print() → Terminal + Dashboard
# ==============================
log_queue = queue.Queue()
log_handler = QueueHandler(log_queue)
logging.basicConfig(handlers=[log_handler], level=logging.INFO)
log = logging.getLogger()

def print_log(*args, **kwargs):
    msg = ' '.join(map(str, args))
    log.info(msg)
    end = kwargs.get('end', '\n')
    flush = kwargs.get('flush', False)
    sys.stdout.write(msg + end)
    if flush: sys.stdout.flush()

import builtins
builtins.print = print_log  # Replace print globally

# ==============================
# LOG COLLECTOR (for UI)
# ==============================
def collect_logs():
    while True:
        try:
            while True:
                record = log_queue.get_nowait()
                log_entries.append({
                    'msg': record.getMessage(),
                    'time': time.time()
                })
        except queue.Empty:
            pass
        time.sleep(0.1)

threading.Thread(target=collect_logs, daemon=True).start()

# ==============================
# ORIGINAL ARDUINO READER (unchanged)
# ==============================
class ArduinoReader:
    def __init__(self):
        self.arduino_ports = []
        self.running = True

    def detect_arduino_ports(self):
        ports = serial.tools.list_ports.comports()
        arduino_ports = []
        for port in ports:
            if 'Arduino' in port.description or 'CH340' in port.description or 'USB' in port.description:
                arduino_ports.append(port.device)
                print(f"Found Arduino on port: {port.device}")
        return arduino_ports

    def read_from_arduino(self, port, sensor_id):
        try:
            ser = serial.Serial(port, 9600, timeout=1)
            time.sleep(2)
            print(f"Reading from Arduino on {port} (Sensor ID: {sensor_id})")
            while self.running:
                try:
                    if ser.in_waiting > 0:
                        line = ser.readline().decode('utf-8').strip()
                        if line:
                            data = self.parse_sensor_data(line, sensor_id)
                            if data:
                                sensor_data.append(data)
                                print(f"[{sensor_id}] Received: {data}")
                except Exception as e:
                    print(f"Error reading from {port}: {e}")
                time.sleep(0.1)
        except Exception as e:
            print(f"Failed to open port {port}: {e}")

    def parse_sensor_data(self, line, sensor_id):
        try:
            data = json.loads(line)
            data['sensor_id'] = sensor_id
            data['timestamp'] = time.time()
            return data
        except:
            try:
                parts = line.split(',')
                if len(parts) >= 1:
                    return {
                        'sensor_id': sensor_id,
                        'value': float(parts[0]),
                        'timestamp': time.time()
                    }
            except:
                return None

    def start_reading(self):
        ports = self.detect_arduino_ports()
        if not ports:
            print("No Arduino devices found!")
            print("Available ports:")
            for port in serial.tools.list_ports.comports():
                print(f"  - {port.device}: {port.description}")
            return []
        threads = []
        for idx, port in enumerate(ports):
            sensor_id = f"arduino_{idx + 1}"
            thread = threading.Thread(
                target=self.read_from_arduino,
                args=(port, sensor_id),
                daemon=True
            )
            threads.append(thread)
            thread.start()
        return threads

# ==============================
# MASTER NODE (enhanced aggregate)
# ==============================
class MasterNode:
    def __init__(self):
        self.results = {}

    def get_online_workers(self):
        now = time.time()
        online = []
        for w in worker_nodes:
            last = w.get('last_heartbeat', 0)
            is_online = (now - last) <= HEARTBEAT_TIMEOUT
            w['online'] = is_online
            if is_online:
                online.append(w)
        return online

    def partition_workload(self, data_batch):
        online_workers = self.get_online_workers()
        if not online_workers:
            print("No online workers available for batch processing.")
            return []
        batch_size = max(1, len(data_batch) // len(online_workers))
        return [data_batch[i:i + batch_size] for i in range(0, len(data_batch), batch_size)]

    def send_to_worker_socket(self, worker_info, data, task_id):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((worker_info['ip'], WORKER_PORT))
            payload = json.dumps({'task_id': task_id, 'data': data})
            sock.sendall(payload.encode())
            result = sock.recv(8192).decode()
            self.results[task_id] = json.loads(result)
            sock.close()
            print(f"Task {task_id} completed by {worker_info['ip']} (local)")
        except Exception as e:
            print(f"Error with worker {worker_info['ip']}: {e}")
            self.results[task_id] = {'error': str(e)}

    def send_to_worker_http(self, worker_info, data, task_id):
        try:
            import requests
            payload = {'task_id': task_id, 'data': data}
            response = requests.post(f"{worker_info['url']}/process", json=payload, timeout=30)
            if response.status_code == 200:
                self.results[task_id] = response.json()
                print(f"Task {task_id} completed by {worker_info['url']} (ngrok)")
            else:
                self.results[task_id] = {'error': f'HTTP {response.status_code}'}
        except Exception as e:
            print(f"Error with worker {worker_info['url']}: {e}")
            self.results[task_id] = {'error': str(e)}

    def distribute_tasks(self, data_batch):
        online_workers = self.get_online_workers()
        if not online_workers:
            print("No online workers to distribute tasks.")
            return None

        partitions = self.partition_workload(data_batch)
        threads = []

        for idx, (worker, partition) in enumerate(zip(online_workers, partitions)):
            if partition:
                if worker['type'] == 'ngrok':
                    thread = threading.Thread(target=self.send_to_worker_http, args=(worker, partition, idx))
                else:
                    thread = threading.Thread(target=self.send_to_worker_socket, args=(worker, partition, idx))
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()

        return self.aggregate_results()

    def aggregate_results(self):
        if not self.results:
            return None

        total_sum = 0
        count = 0
        ml_anomalies = []
        trends = []

        for result in self.results.values():
            if 'error' not in result:
                total_sum += result.get('processed_value', 0)
                count += result.get('count', 0)
                ml_anomalies.extend(result.get('ml_anomalies', []))
                if 'trend_slope' in result:
                    trends.append({'worker': result['worker_id'], 'slope': result['trend_slope']})

        aggregated = {
            'average': total_sum / count if count > 0 else 0,
            'total_processed': count,
            'worker_count': len(self.results),
            'individual_results': self.results,
            'timestamp': time.time(),
            'ml_anomalies_count': len(ml_anomalies),
            'ml_anomalies': ml_anomalies[:10],
            'trends': trends
        }

        processing_results.append(aggregated)
        return aggregated


# Initialize components
arduino_reader = ArduinoReader()
master = MasterNode()


def auto_process_data():
    while True:
        online_count = len(master.get_online_workers())
        if len(sensor_data) >= BATCH_SIZE and online_count > 0:
            print(f"\n→ Processing batch of {len(sensor_data)} readings... (using {online_count} online workers)")
            data_batch = list(sensor_data)
            sensor_data.clear()
            result = master.distribute_tasks(data_batch)
            if result:
                print(f"Batch processed. Average: {result['average']:.2f}")
        time.sleep(2)


# ==============================
# FLASK ENDPOINTS (original + new)
# ==============================
@app.route('/register_worker', methods=['POST'])
def register_worker():
    data = request.json
    worker_ip = data.get('ip')
    worker_url = data.get('url', None)
    worker_type = data.get('type', 'local')

    worker_info = {
        'ip': worker_ip,
        'url': worker_url,
        'type': worker_type,
        'last_heartbeat': time.time(),
        'online': True
    }

    existing = [w for w in worker_nodes if w['ip'] == worker_ip]
    if existing:
        existing[0].update(worker_info)
        print(f"Worker updated: {worker_ip} ({worker_type})")
    else:
        worker_nodes.append(worker_info)
        print(f"Worker registered: {worker_ip} ({worker_type})")
        if worker_url:
            print(f"  URL: {worker_url}")

    return jsonify({
        'status': 'registered',
        'worker_count': len(worker_nodes),
        'type': worker_type
    }), 200


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = request.json
    worker_ip = data.get('ip')
    now = time.time()
    worker = next((w for w in worker_nodes if w['ip'] == worker_ip), None)
    if worker:
        worker['last_heartbeat'] = now
        worker['online'] = True
        print(f"Heartbeat received from {worker_ip}")
    else:
        print(f"Heartbeat from unknown worker: {worker_ip}")
    return jsonify({'status': 'ok'}), 200


@app.route('/status', methods=['GET'])
def status():
    now = time.time()
    for w in worker_nodes:
        w['online'] = (now - w.get('last_heartbeat', 0)) <= HEARTBEAT_TIMEOUT
    online_count = len([w for w in worker_nodes if w['online']])
    return jsonify({
        'sensor_data_count': len(sensor_data),
        'worker_count': len(worker_nodes),
        'online_worker_count': online_count,
        'workers': [{'ip': w['ip'], 'type': w['type'], 'url': w.get('url'), 'online': w['online']} for w in worker_nodes],
        'results_count': len(processing_results),
        'latest_result': processing_results[-1] if processing_results else None,
        'logs': list(log_entries)[-50:]  # NEW: Send logs to UI
    }), 200


@app.route('/results', methods=['GET'])
def get_results():
    return jsonify({
        'results': processing_results[-10:],
        'total': len(processing_results)
    }), 200


@app.route('/workers', methods=['GET'])
def list_workers():
    now = time.time()
    for w in worker_nodes:
        w['online'] = (now - w.get('last_heartbeat', 0)) <= HEARTBEAT_TIMEOUT
    return jsonify({
        'workers': worker_nodes,
        'count': len(worker_nodes)
    }), 200


# ==============================
# ENHANCED DASHBOARD (Chart + Logs + ML)
# ==============================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>IoT Edge Master + ML Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {font-family: system-ui, sans-serif; margin:0; background:#f4f6f9; color:#1a1a1a;}
        .container {max-width:1400px; margin:auto; padding:1rem;}
        .card {background:#fff; border-radius:12px; box-shadow:0 4px 12px rgba(0,0,0,.08); padding:1.5rem; margin-bottom:1.5rem;}
        h1, h2 {margin:0 0 1rem; color:#2c3e50;}
        .grid {display:grid; grid-template-columns:repeat(auto-fit, minmax(280px,1fr)); gap:1rem;}
        .stat {font-size:1.5rem; font-weight:bold; color:#2c3e50;}
        .badge {padding:.3rem .6rem; border-radius:6px; font-size:.8rem; color:#fff;}
        .online {background:#27ae60;} .offline {background:#e74c3c;} .local {background:#3498db;} .ngrok {background:#9b59b6;}
        table {width:100%; border-collapse:collapse; margin-top:.5rem;}
        th, td {padding:.6rem; text-align:left; border-bottom:1px solid #eee;}
        th {background:#f8f9fa;}
        .log {max-height:300px; overflow-y:auto; font-family:monospace; font-size:.9rem; background:#f8f9fa; padding:.5rem; border-radius:6px;}
        .log div {margin:.2rem 0;}
        .time {color:#7f8c8d; font-size:.8rem;}
    </style>
</head>
<body>
<div class="container">
    <h1>IoT Edge Master + ML Dashboard</h1>

    <div class="grid">
        <div class="card"><div class="stat" id="sensorCount">-</div><small>Sensor Queue</small></div>
        <div class="card"><div class="stat" id="totalWorkers">-</div><small>Total Workers</small></div>
        <div class="card"><div class="stat" id="onlineWorkers">-</div><small>Online</small></div>
        <div class="card"><div class="stat" id="latestAvg">-</div><small>Latest Avg</small></div>
    </div>

    <div class="card">
        <h2>Live Processing Trend</h2>
        <canvas id="chart"></canvas>
    </div>

    <div class="grid">
        <div class="card">
            <h2>Registered Workers</h2>
            <table id="workerTable">
                <thead><tr><th>IP</th><th>Type</th><th>URL</th><th>Status</th></tr></thead>
                <tbody></tbody>
            </table>
        </div>
        <div class="card">
            <h2>ML Anomalies (Latest)</h2>
            <div id="anomalies">-</div>
        </div>
    </div>

    <div class="card">
        <h2>System Logs (Real-time)</h2>
        <div class="log" id="logs"></div>
    </div>
</div>

<script>
let chart;
async function refresh(){
    const s = await fetch('/status').then(r=>r.json());
    document.getElementById('sensorCount').textContent = s.sensor_data_count;
    document.getElementById('totalWorkers').textContent = s.worker_count;
    document.getElementById('onlineWorkers').textContent = s.online_worker_count;
    document.getElementById('latestAvg').textContent = s.latest_result ? s.latest_result.average.toFixed(2) : '-';

    // Workers
    const wt = document.querySelector('#workerTable tbody');
    wt.innerHTML = '';
    s.workers.forEach(w => {
        const tr = document.createElement('tr');
        const statusBadge = w.online ? '<span class="badge online">ONLINE</span>' : '<span class="badge offline">OFFLINE</span>';
        const typeBadge = `<span class="badge ${w.type}">${w.type.toUpperCase()}</span>`;
        tr.innerHTML = `<td>${w.ip}</td><td>${typeBadge}</td><td>${w.url||'-'}</td><td>${statusBadge}</td>`;
        wt.appendChild(tr);
    });

    // ML Anomalies
    const anom = s.latest_result?.ml_anomalies || [];
    document.getElementById('anomalies').innerHTML = anom.length 
        ? anom.map(a => `<div>${a.value} (score: ${a.score?.toFixed(2)||'?'})</div>`).join('')
        : '<i>No anomalies detected</i>';

    // Logs
    const logDiv = document.getElementById('logs');
    s.logs.forEach(l => {
        const div = document.createElement('div');
        const t = new Date(l.time*1000).toLocaleTimeString();
        div.innerHTML = `<span class="time">${t}</span> ${l.msg}`;
        logDiv.appendChild(div);
    });
    logDiv.scrollTop = logDiv.scrollHeight;

    // Chart
    const res = await fetch('/results').then(r=>r.json());
    const labels = res.results.map(r => new Date(r.timestamp*1000).toLocaleTimeString());
    const avgs = res.results.map(r => r.average);
    if (!chart) {
        chart = new Chart(document.getElementById('chart'), {
            type: 'line',
            data: {labels, datasets: [{label: 'Average Value', data: avgs, borderColor: '#3498db', fill: false}]},
            options: {responsive: true, scales: {y: {beginAtZero: false}}}
        });
    } else {
        chart.data.labels = labels;
        chart.data.datasets[0].data = avgs;
        chart.update('quiet');
    }
}
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""

@app.route('/')
def dashboard():
    return render_template_string(HTML_TEMPLATE)


def start_flask():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)


if __name__ == '__main__':
    print("=" * 60)
    print("MASTER NODE - Distributed IoT Edge Computing (ngrok + UI + Heartbeat + ML)")
    print("=" * 60)

    print("\n1. Detecting Arduino devices...")
    arduino_threads = arduino_reader.start_reading()

    if not arduino_threads:
        print("\nWARNING: No Arduino detected!")
        print("Please connect Arduino boards and restart.")
        print("The system will continue waiting for worker nodes.")

    print("\n2. Starting auto-processing thread...")
    process_thread = threading.Thread(target=auto_process_data, daemon=True)
    process_thread.start()

    print("\n3. Starting API server on port 5000...")
    print("\nMaster Node ready!")
    print(f"- Sensor data buffer: {len(sensor_data)}")
    print(f"- Worker nodes: {len(worker_nodes)}")
    print("\nNGROK SETUP:")
    print("  1. Install ngrok: https://ngrok.com/download")
    print("  2. In another terminal, run: ngrok http 5000")
    print("  3. Copy the ngrok URL (e.g., https://abc123.ngrok.io)")
    print("  4. Give this URL to remote workers")
    print("\nWaiting for worker nodes to register...")
    print("- Local workers connect to: http://YOUR_LOCAL_IP:5000")
    print("- Remote workers connect to: https://YOUR_NGROK_URL")
    print("\n" + "=" * 60 + "\n")

    start_flask()