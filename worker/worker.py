# distributed/worker/worker.py
import zmq
import time
import os
import sys
import json

# Ensure project root is importable when running worker.py directly
# ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# if ROOT not in sys.path:
#     sys.path.insert(0, ROOT)

from ml_model import MLModel
from common.messages import parse_message, create_ack
from common.utils import logging, safe_sleep

# Set MASTER_IP to the master machine address (tcp://<IP>:5555)
MASTER_IP = "tcp://127.0.0.1:5555"  # <-- replace with real master IP before running across machines


def main():
    context = zmq.Context()
    sock = context.socket(zmq.DEALER)
    # Optionally set an identity; otherwise ZMQ will assign one
    # sock.setsockopt(zmq.IDENTITY, b"worker-1")
    sock.connect(MASTER_IP)
    logging.info(f"Worker connected to master at {MASTER_IP}")

    ml = MLModel()
    processed = set()

    # Send READY announcement so master sees us
    ready_msg = json.dumps({"type": "READY"})
    sock.send(ready_msg.encode())

    while True:
        try:
            # DEALER.recv() returns raw bytes (message)
            msg = sock.recv()
            try:
                data = parse_message(msg.decode())
            except Exception:
                data = {}
            if not data:
                logging.warning("Received empty/invalid task message")
                continue

            if data.get("type") == "TASK":
                batch_id = data.get("batch_id")
                payload = data.get("payload") or {}
                if batch_id in processed:
                    logging.info(f"Batch {batch_id} already processed; sending cached ACK")
                    ack_msg = create_ack(batch_id, {"predictions": [0, 0, 0, 0, 0], "cached": True})
                else:
                    preds = ml.predict(payload)
                    ack_msg = create_ack(batch_id, {"predictions": preds})
                    processed.add(batch_id)
                    logging.info(f"Processed batch {batch_id} preds={preds}")
                # send ACK
                sock.send(ack_msg.encode())
            else:
                logging.info(f"Ignoring message type {data.get('type')}")
        except KeyboardInterrupt:
            logging.info("Worker interrupted by user")
            break
        except Exception as e:
            logging.warning(f"Worker loop error: {e}")
            safe_sleep(1)


if __name__ == "__main__":
    main()
