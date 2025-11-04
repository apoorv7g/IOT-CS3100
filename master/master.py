# distributed/master/master.py
import zmq
import threading
import time
import os
import sys

# # allow running master.py directly from its folder
# ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# if ROOT not in sys.path:
#     sys.path.insert(0, ROOT)

from task_manager import TaskManager
from dashboard import start_flask, update_dashboard, master_state
from data_provider import DataProvider
from common.utils import logging, safe_sleep
import config as config
from common.messages import parse_message


def main():
    context = zmq.Context()
    master_socket = context.socket(zmq.ROUTER)
    master_socket.bind(config.ZMQ_MASTER_BIND)
    logging.info(f"Master ROUTER bound to {config.ZMQ_MASTER_BIND}")

    # start dashboard
    flask_thread = threading.Thread(target=start_flask, args=(config.DASHBOARD_HOST, config.DASHBOARD_PORT),
                                    daemon=True)
    flask_thread.start()
    logging.info(f"Dashboard server thread started at http://{config.DASHBOARD_HOST}:{config.DASHBOARD_PORT}")

    # choose data provider mode: "simulate" or "arduino" (change as needed)
    # For demo/simulate:
    # data_provider = DataProvider(mode="simulate")
    # If using Arduino, replace with:
    # data_provider = DataProvider(mode="arduino", serial_port="/dev/ttyUSB0")
    data_provider = DataProvider(mode="arduino", serial_port="COM10")

    task_manager = TaskManager(master_socket, data_provider)
    # start time-based batching
    task_manager.start_batching(config.BATCH_WINDOW_SEC)

    poller = zmq.Poller()
    poller.register(master_socket, zmq.POLLIN)

    logging.info("Master entering main loop to receive messages from workers")
    try:
        while True:
            socks = dict(poller.poll(timeout=config.MASTER_POLL_TIMEOUT_MS))
            if master_socket in socks and socks[master_socket] == zmq.POLLIN:
                # ROUTER recv_multipart returns [identity, message]
                msg_parts = master_socket.recv_multipart()
                if len(msg_parts) >= 2:
                    worker_id = msg_parts[0]
                    msg = msg_parts[1]
                    # process in task manager
                    task_manager.process_incoming(worker_id, msg)
                    # update set of workers in dashboard state (task_manager.register_worker already does)
                else:
                    logging.warning("Malformed message from worker")
            else:
                # no message; continue loop
                pass
    except KeyboardInterrupt:
        logging.info("Master shutting down (KeyboardInterrupt)")
    finally:
        master_socket.close()
        context.term()


if __name__ == "__main__":
    main()
