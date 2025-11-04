# distributed/master/dashboard.py
import os
import threading
from flask import Flask, render_template
from flask_socketio import SocketIO
import time
import json

# Ensure templates are found when running from project root
BASE_DIR = os.path.dirname(__file__)
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

app = Flask(__name__, template_folder=TEMPLATES_DIR, static_folder=None)
socketio = SocketIO(app, cors_allowed_origins="*")

# Shared global state that TaskManager will update
master_state = {
    "workers": [],  # list of worker identities (strings)
    "batches": {}  # batch_id -> {worker, status, result, attempts}
}


@app.route('/')
def index():
    return render_template("dashboard.html")


def start_flask(host="127.0.0.1", port=5000):
    """
    Start the Flask + SocketIO server. Run in a background thread.
    """
    # socketio.run is blocking; use it as the target of a thread
    socketio.run(app, host=host, port=port, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)


def update_dashboard():
    """
    Emit the current master_state to all connected clients.
    """
    try:
        # Convert workers to simple list of strings for JSON serialization
        state = {
            "workers": [w.decode() if isinstance(w, bytes) else str(w) for w in master_state.get("workers", [])],
            "batches": master_state.get("batches", {})
        }
        socketio.emit('update', state)
    except Exception:
        # Avoid crashes on emit problems
        pass


# Optional periodic updater (keeps clients in sync even if no events)
def start_auto_update(interval=1.0):
    def loop():
        while True:
            update_dashboard()
            time.sleep(interval)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
