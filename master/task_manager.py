# distributed/master/task_manager.py
import threading
import time
from collections import deque
from common.messages import create_task, parse_message
from dashboard import master_state, update_dashboard
from common.utils import logging, safe_sleep
import zmq
import json
import config


class TaskManager:
    def __init__(self, master_socket, data_provider):
        """
        master_socket: zmq.ROUTER socket bound in master.py
        data_provider: DataProvider instance
        """
        self.master_socket = master_socket
        self.data_provider = data_provider

        self.pending_lock = threading.Lock()
        # pending_tasks: batch_id -> dict{ data, retries, assigned_worker, last_sent_ts, timeout }
        self.pending_tasks = {}

        # keep a deque of known worker identities (bytes) for round-robin assignment
        self.workers_lock = threading.Lock()
        self.worker_list = deque()  # store identities as bytes

        # index tracker for round robin (we use deque.rotate instead)
        self.monitor_thread = threading.Thread(target=self._monitor_pending, daemon=True)
        self.monitor_thread.start()

    def register_worker(self, worker_id):
        """
        Called when a worker communicates with master. Add to known worker list if not present.
        worker_id is bytes.
        """
        with self.workers_lock:
            if worker_id not in self.worker_list:
                self.worker_list.append(worker_id)
                logging.info(f"Registered worker {worker_id}")
                # reflect into dashboard state
                master_state["workers"] = list(self.worker_list)
                update_dashboard()

    def unregister_worker(self, worker_id):
        with self.workers_lock:
            try:
                if worker_id in self.worker_list:
                    self.worker_list.remove(worker_id)
                    master_state["workers"] = list(self.worker_list)
                    update_dashboard()
            except ValueError:
                pass

    def start_batching(self, batch_window_sec):
        """
        Start a background thread that creates time-based batches every batch_window_sec.
        """

        def batching_loop():
            while True:
                batch = self.data_provider.get_batch_for_window(batch_window_sec)
                if batch is None:
                    logging.warning("No data from provider for this window; sending empty batch")
                    batch = {"temperature": 0, "humidity": 0, "pressure": 0, "light": 0, "vibration": 0}
                task_msg = create_task(batch)
                task_obj = parse_message(task_msg)
                batch_id = task_obj.get("batch_id")
                with self.pending_lock:
                    self.pending_tasks[batch_id] = {
                        "data": batch,
                        "retries": 0,
                        "assigned_worker": None,
                        "last_sent_ts": None,
                        "timeout": config.INITIAL_RETRY_TIMEOUT,
                        "attempts": 0  # total attempts including reassigns
                    }
                    # add to dashboard state
                    master_state["batches"][batch_id] = {
                        "worker": None,
                        "status": "pending",
                        "result": None,
                        "attempts": 0
                    }
                    update_dashboard()

                logging.info(f"Created batch {batch_id} payload={batch}")
                # Immediately try to assign (non-blocking)
                self._assign_task(batch_id, task_msg)
                # Wait for next window
                safe_sleep(batch_window_sec)

        t = threading.Thread(target=batching_loop, daemon=True)
        t.start()

    def _get_next_worker(self, exclude_worker=None):
        """
        Return next available worker identity (bytes) in round-robin fashion.
        Optionally exclude a worker identity.
        Returns None if no workers available.
        """
        with self.workers_lock:
            if not self.worker_list:
                return None
            # rotate until a worker not equal to exclude_worker is found
            for _ in range(len(self.worker_list)):
                worker = self.worker_list[0]
                # rotate for fairness
                self.worker_list.rotate(-1)
                if exclude_worker is None or worker != exclude_worker:
                    return worker
            return None

    def _assign_task(self, batch_id, batch_msg, preferred_worker=None):
        """
        Low-level send of a task to a worker. Preferred_worker is bytes identity or None.
        """
        with self.workers_lock:
            if preferred_worker:
                worker = preferred_worker
            else:
                worker = self._get_next_worker()

        if worker is None:
            logging.warning(f"No workers available to assign batch {batch_id}")
            return False

        try:
            # ROUTER expects [identity, message]
            self.master_socket.send_multipart([worker, batch_msg.encode()])
            with self.pending_lock:
                p = self.pending_tasks.get(batch_id)
                if p is not None:
                    p["assigned_worker"] = worker
                    p["last_sent_ts"] = time.time()
                    p["attempts"] += 1
                    # store current timeout as used (could be adapted)
                # update dashboard
                master_state["batches"][batch_id]["worker"] = worker.decode() if isinstance(worker, bytes) else str(
                    worker)
                master_state["batches"][batch_id]["status"] = "pending"
                master_state["batches"][batch_id]["attempts"] = self.pending_tasks[batch_id]["attempts"]
                update_dashboard()
            logging.info(f"Assigned batch {batch_id} -> worker {worker}")
            return True
        except Exception as e:
            logging.warning(f"Failed to send batch {batch_id} to worker {worker}: {e}")
            return False

    def process_incoming(self, worker_id, msg_bytes):
        """
        Called by master main loop when a message is received from worker.
        worker_id: bytes identity
        msg_bytes: raw message bytes
        """
        # ensure worker is registered
        self.register_worker(worker_id)

        try:
            msg = msg_bytes.decode()
        except Exception:
            logging.warning("Received non-decodable message from worker")
            return

        data = parse_message(msg)
        if not data:
            # not a JSON message; ignore
            return

        mtype = data.get("type")
        if mtype == "ACK":
            batch_id = data.get("batch_id")
            result = data.get("result")
            with self.pending_lock:
                if batch_id in self.pending_tasks:
                    # mark completed
                    logging.info(f"ACK received for batch {batch_id} from {worker_id}")
                    # update dashboard
                    master_state["batches"][batch_id]["status"] = "ack"
                    master_state["batches"][batch_id]["result"] = result
                    update_dashboard()
                    # remove pending task
                    try:
                        del self.pending_tasks[batch_id]
                    except KeyError:
                        pass
                else:
                    logging.info(f"ACK for unknown or already-processed batch {batch_id}")
        elif data.get("type") == "READY":
            # Worker announced readiness; just register (done above)
            logging.info(f"Worker {worker_id} READY")
        else:
            # Unknown message type - ignore or log
            logging.info(f"Received message type={mtype} from {worker_id}")

    def _monitor_pending(self):
        """
        Background thread to monitor pending tasks for timeouts and perform retries/reassign.
        """
        while True:
            try:
                now = time.time()
                to_retry = []
                with self.pending_lock:
                    for batch_id, info in list(self.pending_tasks.items()):
                        last = info.get("last_sent_ts")
                        if last is None:
                            # not sent yet; skip
                            continue
                        timeout = info.get("timeout", config.INITIAL_RETRY_TIMEOUT)
                        if now - last >= timeout:
                            to_retry.append((batch_id, info.copy()))
                # process retries outside lock
                for batch_id, info in to_retry:
                    with self.pending_lock:
                        # verify task still pending (could have been acked)
                        if batch_id not in self.pending_tasks:
                            continue
                        info = self.pending_tasks[batch_id]
                        assigned = info.get("assigned_worker")
                        retries = info.get("retries", 0)
                        attempts = info.get("attempts", 0)

                        if retries < config.MAX_RETRIES_PER_WORKER:
                            # retry same worker: increment retry count and increase timeout
                            info["retries"] = retries + 1
                            info["timeout"] = min(
                                info.get("timeout", config.INITIAL_RETRY_TIMEOUT) + config.RETRY_TIMEOUT_INCREMENT,
                                config.RETRY_TIMEOUT_CAP)
                            logging.info(
                                f"Retrying batch {batch_id} to same worker {assigned} (retry #{info['retries']})")
                            # send again
                            # reconstruct message
                            task_msg = create_task(info["data"])
                            # Important: keep original batch_id (create_task creates new id) -> so we must not call create_task again
                            # Instead, send original payload as JSON with same id
                            payload_msg = json.dumps({
                                "batch_id": batch_id,
                                "type": "TASK",
                                "payload": info["data"]
                            })
                            self._assign_task(batch_id, payload_msg, preferred_worker=assigned)
                        else:
                            # exceeded retries for this worker -> reassign to different worker
                            logging.info(f"Max retries reached for batch {batch_id} on worker {assigned}. Reassigning.")
                            info["retries"] = 0
                            info["timeout"] = config.INITIAL_RETRY_TIMEOUT
                            # choose a different worker
                            new_worker = self._get_next_worker(exclude_worker=assigned)
                            if new_worker is None:
                                logging.warning(f"No other workers available to reassign batch {batch_id}")
                                # try same worker again later (we set last_sent_ts to now so it'll wait)
                                info["last_sent_ts"] = time.time()
                                continue
                            # send to new worker
                            payload_msg = json.dumps({
                                "batch_id": batch_id,
                                "type": "TASK",
                                "payload": info["data"]
                            })
                            info["attempts"] = attempts + 1
                            # update dashboard attempts count
                            master_state["batches"][batch_id]["attempts"] = info["attempts"]
                            update_dashboard()
                            self._assign_task(batch_id, payload_msg, preferred_worker=new_worker)
                safe_sleep(config.TASK_MONITOR_INTERVAL)
            except Exception as e:
                logging.warning(f"TaskManager monitor exception: {e}")
                safe_sleep(1)
