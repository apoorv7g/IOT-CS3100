"""
Worker Node with ngrok Support - Can connect over Internet
Processes data distributed by Master Node (local or remote via ngrok)
"""

import socket
import json
import threading
import requests
import time
import statistics
import numpy as np
from flask import Flask, request, jsonify, render_template_string

# ===== CONFIGURATION - CHANGE THESE =====
MASTER_URL = 'http://192.168.31.228:5000'  # Change to ngrok URL if master is remote
# Example: 'https://abc123.ngrok.io' if using ngrok

USE_NGROK = False  # Set to True if you want to expose this worker via ngrok
WORKER_PORT = 5001

# Heartbeat config
HEARTBEAT_INTERVAL = 2  # seconds
# ========================================

app = Flask(__name__)

WORKER_IP = socket.gethostbyname(socket.gethostname())
worker_instance = None


class WorkerNode:
    """Edge computing worker node (supports both socket and HTTP)"""

    def __init__(self):
        self.processed_count = 0
        self.total_processing_time = 0
        self.worker_url = None  # Will be set if using ngrok
        self.start_time = time.time()
        self.register_with_master()

        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()

    def _heartbeat_loop(self):
        """Send heartbeat to master every HEARTBEAT_INTERVAL seconds"""
        while True:
            try:
                requests.post(
                    f'{MASTER_URL}/heartbeat',
                    json={'ip': WORKER_IP},
                    timeout=1
                )
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def set_ngrok_url(self, url):
        """Set the ngrok URL for this worker"""
        self.worker_url = url
        print(f"Worker ngrok URL set: {url}")
        # Re-register with the new URL
        self.register_with_master()

    def register_with_master(self):
        """Register this worker with the master node"""
        max_retries = 5
        retry_delay = 3

        # Determine worker type
        worker_type = 'ngrok' if self.worker_url else 'local'

        for attempt in range(max_retries):
            try:
                payload = {
                    'ip': WORKER_IP,
                    'type': worker_type
                }

                # Add URL if using ngrok
                if self.worker_url:
                    payload['url'] = self.worker_url

                response = requests.post(
                    f'{MASTER_URL}/register_worker',
                    json=payload,
                    timeout=5
                )

                if response.status_code == 200:
                    print(f"Successfully registered with master at {MASTER_URL}")
                    print(f"  Worker IP: {WORKER_IP}")
                    print(f"  Worker Type: {worker_type}")
                    if self.worker_url:
                        print(f"  Worker URL: {self.worker_url}")
                    print(f"  Response: {response.json()}")
                    return True

            except requests.exceptions.ConnectionError:
                print(f"Cannot reach master at {MASTER_URL}")
                print(f"  Retrying in {retry_delay} seconds... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)

            except Exception as e:
                print(f"Registration error: {e}")
                time.sleep(retry_delay)

        print(f"\nFailed to register after {max_retries} attempts")
        print(f"  Make sure master node is running at {MASTER_URL}")
        return False

    def process_data(self, data_batch):
        """
        Main processing function - implement your edge computation here
        Examples: filtering, smoothing, feature extraction, anomaly detection
        """
        start_time = time.time()
        processed_values = []
        anomalies = []

        for item in data_batch:
            value = item.get('value', 0)
            sensor_id = item.get('sensor_id', 'unknown')

            # ===== EDGE COMPUTATION ALGORITHMS =====

            # 1. Signal Processing
            processed_value = self.apply_filter(value)

            # 2. Anomaly Detection
            if self.is_anomaly(value):
                anomalies.append({
                    'sensor_id': sensor_id,
                    'value': value,
                    'timestamp': item.get('timestamp')
                })

            # 3. Feature Extraction
            features = self.extract_features(value)

            processed_values.append(processed_value)
            self.processed_count += 1

        # Calculate statistics
        processing_time = time.time() - start_time
        self.total_processing_time += processing_time

        result = {
            'processed_value': sum(processed_values),
            'count': len(processed_values),
            'average': statistics.mean(processed_values) if processed_values else 0,
            'median': statistics.median(processed_values) if processed_values else 0,
            'std_dev': statistics.stdev(processed_values) if len(processed_values) > 1 else 0,
            'min': min(processed_values) if processed_values else 0,
            'max': max(processed_values) if processed_values else 0,
            'anomalies': len(anomalies),
            'anomaly_details': anomalies[:5],  # First 5 anomalies
            'processing_time': processing_time,
            'worker_id': WORKER_IP,
            'total_processed': self.processed_count
        }

        return result

    def apply_filter(self, value):
        """Apply signal processing filter"""
        smoothed = value * 0.7 + (value * 0.3)
        return smoothed

    def is_anomaly(self, value, threshold=800):
        """Detect anomalies in sensor data"""
        return value > threshold or value < 100

    def extract_features(self, value):
        """Extract features from sensor data"""
        features = {
            'value': value,
            'log_value': np.log(value + 1),
            'squared': value ** 2,
            'normalized': value / 100.0
        }
        return features

    def handle_client(self, client_socket):
        """Handle incoming task from master via socket"""
        try:
            # Receive data
            data = b''
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                try:
                    json.loads(data.decode())
                    break
                except:
                    continue

            task = json.loads(data.decode())

            task_id = task['task_id']
            data_batch = task['data']

            print(f"\n→ Processing task {task_id}: {len(data_batch)} items (socket)")

            # Process the data
            result = self.process_data(data_batch)
            result['task_id'] = task_id

            # Send result back to master
            response = json.dumps(result)
            client_socket.sendall(response.encode())

            print(f"Task {task_id} completed in {result['processing_time']:.3f}s")
            print(f"  Average value: {result['average']:.2f}")
            print(f"  Anomalies detected: {result['anomalies']}")

        except Exception as e:
            print(f"Error processing task: {e}")
            error_response = json.dumps({'error': str(e)})
            try:
                client_socket.sendall(error_response.encode())
            except:
                pass
        finally:
            client_socket.close()

    def start_listening(self):
        """Start listening for tasks from master via socket"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            server_socket.bind(('0.0.0.0', WORKER_PORT))
            server_socket.listen(5)

            print(f"\nWorker node listening on {WORKER_IP}:{WORKER_PORT}")
            print("  Waiting for tasks from master...\n")
            print("=" * 60 + "\n")

            while True:
                client_socket, address = server_socket.accept()

                # Handle each task in a separate thread
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,),
                    daemon=True
                )
                client_thread.start()

        except Exception as e:
            print(f"Error starting server: {e}")
        finally:
            server_socket.close()


# Flask routes for HTTP-based communication (ngrok)
@app.route('/process', methods=['POST'])
def process_task():
    """Process task received via HTTP (for ngrok workers)"""
    try:
        data = request.json
        task_id = data['task_id']
        data_batch = data['data']

        print(f"\n→ Processing task {task_id}: {len(data_batch)} items (HTTP)")

        # Process the data
        result = worker_instance.process_data(data_batch)
        result['task_id'] = task_id

        print(f"Task {task_id} completed in {result['processing_time']:.3f}s")
        print(f"  Average value: {result['average']:.2f}")
        print(f"  Anomalies detected: {result['anomalies']}")

        return jsonify(result), 200

    except Exception as e:
        print(f"Error processing task: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'worker_id': WORKER_IP,
        'processed_count': worker_instance.processed_count if worker_instance else 0
    }), 200


# Worker UI
WORKER_HTML = """
<!DOCTYPE html>
<html><head><title>Worker {{ip}}</title>
<meta http-equiv="refresh" content="3">
<style>
  body{font-family:Arial;background:#f0f4f8;padding:2rem;}
  .card{background:#fff;padding:1.5rem;border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,.1);max-width:500px;margin:auto;}
  h1{color:#2c3e50;}
  .stat{font-size:1.2rem;margin:.5rem 0;}
</style></head>
<body>
<div class="card">
<h1>Worker {{ip}}</h1>
<div class="stat"><strong>Processed:</strong> {{processed}}</div>
<div class="stat"><strong>Uptime:</strong> {{uptime}} s</div>
<div class="stat"><strong>Master:</strong> {{master}}</div>
<div class="stat"><strong>Mode:</strong> {{mode}}</div>
<div class="stat"><strong>Heartbeat:</strong> Every 15s</div>
</div>
</body></html>
"""

@app.route('/')
def worker_ui():
    uptime = time.time() - worker_instance.start_time
    mode = 'ngrok' if worker_instance.worker_url else 'local'
    return render_template_string(
        WORKER_HTML,
        ip=WORKER_IP,
        processed=worker_instance.processed_count,
        uptime=round(uptime, 1),
        master=MASTER_URL,
        mode=mode
    )


def keep_registered():
    """Re-register periodically to maintain connection"""
    while True:
        time.sleep(30)  # Re-register every 30 seconds
        try:
            worker_instance.register_with_master()
        except:
            pass


def start_flask_server():
    """Start Flask server for HTTP-based communication"""
    app.run(host='0.0.0.0', port=WORKER_PORT, debug=False, use_reloader=False)


if __name__ == '__main__':
    print("=" * 60)
    print("WORKER NODE - Distributed IoT Edge Computing (ngrok + UI + Heartbeat)")
    print("=" * 60)
    print(f"\nMaster Node URL: {MASTER_URL}")
    print(f"Worker IP: {WORKER_IP}")
    print(f"Worker Port: {WORKER_PORT}\n")

    worker_instance = WorkerNode()

    # Start keep-alive thread
    keepalive_thread = threading.Thread(target=keep_registered, daemon=True)
    keepalive_thread.start()

    if USE_NGROK:
        print("\nNGROK MODE ENABLED")
        print("Instructions:")
        print("1. Install ngrok: https://ngrok.com/download")
        print("2. In another terminal, run: ngrok http 5001")
        print("3. Copy the ngrok URL (e.g., https://xyz456.ngrok.io)")
        print("4. The worker will use HTTP communication\n")

        # Ask for ngrok URL
        ngrok_url = input("Enter your ngrok URL (or press Enter to skip): ").strip()
        if ngrok_url:
            worker_instance.set_ngrok_url(ngrok_url)

        print("\nStarting worker in HTTP mode (for ngrok)...\n")
        start_flask_server()
    else:
        print("\nStarting worker in socket mode (for local network)...\n")
        worker_instance.start_listening()