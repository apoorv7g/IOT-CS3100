"""
Master Node with ngrok Support - Accessible over Internet
Receives data from Arduino via Serial and distributes to Workers over Internet
Run this on the main laptop connected to Arduino boards
"""

import serial
import serial.tools.list_ports
import socket
import json
import threading
import time
from collections import deque
from flask import Flask, jsonify, request, render_template_string

app = Flask(__name__)

# Configuration
WORKER_PORT = 5001
BATCH_SIZE = 10  # Process every N sensor readings

# Heartbeat config
HEARTBEAT_INTERVAL = 2   # Worker sends every 2s
HEARTBEAT_TIMEOUT = 5    # Mark offline after 5s of no heartbeat

# Data storage
sensor_data = deque(maxlen=1000)
worker_nodes = []  # Now stores {'ip':..., 'url':..., 'type':..., 'last_heartbeat':..., 'online':...}
processing_results = []


class ArduinoReader:
    """Handles reading data from multiple Arduino boards via Serial"""

    def __init__(self):
        self.arduino_ports = []
        self.running = True

    def detect_arduino_ports(self):
        """Auto-detect Arduino COM ports"""
        ports = serial.tools.list_ports.comports()
        arduino_ports = []

        for port in ports:
            # Check for Arduino-like devices
            if 'Arduino' in port.description or 'CH340' in port.description or 'USB' in port.description:
                arduino_ports.append(port.device)
                print(f"Found Arduino on port: {port.device}")

        return arduino_ports

    def read_from_arduino(self, port, sensor_id):
        """Read data from a single Arduino"""
        try:
            ser = serial.Serial(port, 9600, timeout=1)
            time.sleep(2)  # Wait for Arduino to initialize

            print(f"Reading from Arduino on {port} (Sensor ID: {sensor_id})")

            while self.running:
                try:
                    if ser.in_waiting > 0:
                        line = ser.readline().decode('utf-8').strip()

                        if line:
                            # Parse Arduino data
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
        """Parse data from Arduino"""
        try:
            # Try parsing as JSON
            data = json.loads(line)
            data['sensor_id'] = sensor_id
            data['timestamp'] = time.time()
            return data
        except:
            # Try parsing comma-separated values
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
        """Start reading from all detected Arduinos"""
        ports = self.detect_arduino_ports()

        if not ports:
            print("No Arduino devices found!")
            print("Available ports:")
            for port in serial.tools.list_ports.comports():
                print(f"  - {port.device}: {port.description}")
            return []

        # Start a thread for each Arduino
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


class MasterNode:
    """Manages task distribution to worker nodes (local and remote via ngrok)"""

    def __init__(self):
        self.results = {}

    def get_online_workers(self):
        """Return list of workers that are currently online (based on heartbeat)"""
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
        """Divide data among available ONLINE workers"""
        online_workers = self.get_online_workers()
        if not online_workers:
            print("No online workers available for batch processing.")
            return []

        batch_size = max(1, len(data_batch) // len(online_workers))
        partitions = []

        for i in range(0, len(data_batch), batch_size):
            partitions.append(data_batch[i:i + batch_size])

        return partitions

    def send_to_worker_socket(self, worker_info, data, task_id):
        """Send data to worker via direct socket connection (for local workers)"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((worker_info['ip'], WORKER_PORT))

            payload = json.dumps({
                'task_id': task_id,
                'data': data
            })

            sock.sendall(payload.encode())

            # Receive result
            result = sock.recv(8192).decode()
            self.results[task_id] = json.loads(result)

            sock.close()
            print(f"Task {task_id} completed by {worker_info['ip']} (local)")

        except Exception as e:
            print(f"Error with worker {worker_info['ip']}: {e}")
            self.results[task_id] = {'error': str(e)}

    def send_to_worker_http(self, worker_info, data, task_id):
        """Send data to worker via HTTP (for ngrok workers)"""
        try:
            import requests

            payload = {
                'task_id': task_id,
                'data': data
            }

            response = requests.post(
                f"{worker_info['url']}/process",
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                self.results[task_id] = response.json()
                print(f"Task {task_id} completed by {worker_info['url']} (ngrok)")
            else:
                self.results[task_id] = {'error': f'HTTP {response.status_code}'}

        except Exception as e:
            print(f"Error with worker {worker_info['url']}: {e}")
            self.results[task_id] = {'error': str(e)}

    def distribute_tasks(self, data_batch):
        """Distribute tasks to all ONLINE workers (local and remote)"""
        online_workers = self.get_online_workers()
        if not online_workers:
            print("No online workers to distribute tasks.")
            return None

        partitions = self.partition_workload(data_batch)
        threads = []

        for idx, (worker, partition) in enumerate(zip(online_workers, partitions)):
            if partition:  # Only send if there's data
                # Choose communication method based on worker type
                if worker['type'] == 'ngrok':
                    thread = threading.Thread(
                        target=self.send_to_worker_http,
                        args=(worker, partition, idx)
                    )
                else:
                    thread = threading.Thread(
                        target=self.send_to_worker_socket,
                        args=(worker, partition, idx)
                    )
                threads.append(thread)
                thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        return self.aggregate_results()

    def aggregate_results(self):
        """Combine results from all workers"""
        if not self.results:
            return None

        total_sum = 0
        count = 0

        for result in self.results.values():
            if 'error' not in result:
                total_sum += result.get('processed_value', 0)
                count += result.get('count', 0)

        aggregated = {
            'average': total_sum / count if count > 0 else 0,
            'total_processed': count,
            'worker_count': len(self.results),
            'individual_results': self.results,
            'timestamp': time.time()
        }

        processing_results.append(aggregated)
        return aggregated


# Initialize components
arduino_reader = ArduinoReader()
master = MasterNode()


def auto_process_data():
    """Automatically process data when batch size is reached"""
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


# Flask API endpoints
@app.route('/register_worker', methods=['POST'])
def register_worker():
    """Workers register with master (supports both local and ngrok)"""
    data = request.json
    worker_ip = data.get('ip')
    worker_url = data.get('url', None)  # ngrok URL if provided
    worker_type = data.get('type', 'local')  # 'local' or 'ngrok'

    worker_info = {
        'ip': worker_ip,
        'url': worker_url,
        'type': worker_type,
        'last_heartbeat': time.time(),
        'online': True
    }

    # Check if worker already registered
    existing = [w for w in worker_nodes if w['ip'] == worker_ip]
    if existing:
        # Update existing worker
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
    """Receive heartbeat from worker"""
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
    """Get system status"""
    # Update online status
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
        'latest_result': processing_results[-1] if processing_results else None
    }), 200


@app.route('/results', methods=['GET'])
def get_results():
    """Get all processing results"""
    return jsonify({
        'results': processing_results[-10:],  # Last 10 results
        'total': len(processing_results)
    }), 200


@app.route('/workers', methods=['GET'])
def list_workers():
    """List all registered workers"""
    now = time.time()
    for w in worker_nodes:
        w['online'] = (now - w.get('last_heartbeat', 0)) <= HEARTBEAT_TIMEOUT
    return jsonify({
        'workers': worker_nodes,
        'count': len(worker_nodes)
    }), 200


# Dashboard UI
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>IoT Edge Master Dashboard</title>
    <meta http-equiv="refresh" content="5">
    <style>
        body {font-family: Arial, sans-serif; margin:2rem; background:#f7f9fc;}
        h1 {color:#2c3e50;}
        .card {background:#fff; padding:1.5rem; margin-bottom:1.5rem;
               border-radius:8px; box-shadow:0 2px 6px rgba(0,0,0,.1);}
        table {width:100%; border-collapse:collapse; margin-top:1rem;}
        th, td {padding:.6rem; text-align:left; border-bottom:1px solid #ddd;}
        th {background:#ecf0f1;}
        .badge {padding:.2rem .5rem; border-radius:4px; color:#fff; font-size:0.8rem;}
        .online {background:#27ae60;}
        .offline {background:#c0392b;}
        .local {background:#2980b9;}
        .ngrok {background:#9b59b6;}
        .stats {display:flex; gap:1rem; flex-wrap:wrap;}
        .stat {flex:1; min-width:150px;}
        .stat strong {display:block; font-size:1.5rem; color:#2c3e50;}
    </style>
</head>
<body>
    <h1>IoT Edge Master Dashboard</h1>

    <div class="card">
        <h2>System Status</h2>
        <div class="stats">
            <div class="stat"><strong id="sensorCount">-</strong>Sensor Queue</div>
            <div class="stat"><strong id="totalWorkers">-</strong>Total Workers</div>
            <div class="stat"><strong id="onlineWorkers">-</strong>Online Workers</div>
            <div class="stat"><strong id="latestAvg">-</strong>Latest Avg</div>
        </div>
    </div>

    <div class="card">
        <h2>Registered Workers</h2>
        <table id="workerTable">
            <thead><tr><th>IP</th><th>Type</th><th>URL</th><th>Status</th></tr></thead>
            <tbody></tbody>
        </table>
    </div>

    <div class="card">
        <h2>Recent Results (last 10)</h2>
        <table id="resultTable">
            <thead><tr>
                <th>Time</th><th>Avg</th><th>Processed</th><th>Workers Used</th>
            </tr></thead>
            <tbody></tbody>
        </table>
    </div>

<script>
function fmtTime(ts){
    const d = new Date(ts*1000);
    return d.toLocaleTimeString();
}
async function refresh(){
    const s = await fetch('/status').then(r=>r.json());
    document.getElementById('sensorCount').textContent = s.sensor_data_count;
    document.getElementById('totalWorkers').textContent = s.worker_count;
    document.getElementById('onlineWorkers').textContent = s.online_worker_count;
    document.getElementById('latestAvg').textContent =
        s.latest_result ? s.latest_result.average.toFixed(2) : '-';

    // workers
    const wt = document.querySelector('#workerTable tbody');
    wt.innerHTML = '';
    s.workers.forEach(w=>{
        const tr = document.createElement('tr');
        const statusBadge = w.online 
            ? '<span class="badge online">ONLINE</span>' 
            : '<span class="badge offline">OFFLINE</span>';
        const typeBadge = `<span class="badge ${w.type}">${w.type.toUpperCase()}</span>`;
        tr.innerHTML = `<td>${w.ip}</td>
                        <td>${typeBadge}</td>
                        <td>${w.url||'-'}</td>
                        <td>${statusBadge}</td>`;
        wt.appendChild(tr);
    });

    // results
    const rt = document.querySelector('#resultTable tbody');
    rt.innerHTML = '';
    (await fetch('/results').then(r=>r.json())).results.forEach(r=>{
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${fmtTime(r.timestamp)}</td>
                        <td>${r.average.toFixed(2)}</td>
                        <td>${r.total_processed}</td>
                        <td>${r.worker_count}</td>`;
        rt.appendChild(tr);
    });
}
refresh();   // initial load
setInterval(refresh, 5000);  // refresh every 10s
</script>
</body>
</html>
"""

@app.route('/')
def dashboard():
    return render_template_string(HTML_TEMPLATE)


def start_flask():
    """Start Flask API server"""
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)


if __name__ == '__main__':
    print("=" * 60)
    print("MASTER NODE - Distributed IoT Edge Computing (ngrok + UI + Heartbeat)")
    print("=" * 60)

    # Start Arduino readers
    print("\n1. Detecting Arduino devices...")
    arduino_threads = arduino_reader.start_reading()

    if not arduino_threads:
        print("\nWARNING: No Arduino detected!")
        print("Please connect Arduino boards and restart.")
        print("The system will continue waiting for worker nodes.")

    # Start auto-processing thread
    print("\n2. Starting auto-processing thread...")
    process_thread = threading.Thread(target=auto_process_data, daemon=True)
    process_thread.start()

    # Start Flask API
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