# distributed/master/config.py

# ZeroMQ ports / addresses
ZMQ_MASTER_PORT = 5555
ZMQ_MASTER_BIND = f"tcp://*:{ZMQ_MASTER_PORT}"

# Dashboard
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 5000

# Batching & retry policy
BATCH_WINDOW_SEC = 10          # time-based batching window
INITIAL_RETRY_TIMEOUT = 2      # initial timeout (seconds)
RETRY_TIMEOUT_INCREMENT = 1    # increment on retry
RETRY_TIMEOUT_CAP = 5          # cap for timeout (seconds)
MAX_RETRIES_PER_WORKER = 3     # retry same worker up to this many times
TOTAL_REASSIGN_ATTEMPTS = 4    # after this many attempts reassign to other worker

# Polling
MASTER_POLL_TIMEOUT_MS = 1000  # poll interval in ms for recv
TASK_MONITOR_INTERVAL = 1      # seconds (monitor pending tasks)
