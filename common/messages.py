# distributed/common/messages.py
import uuid
import json


def create_task(batch_data):
    """
    Create a TASK message with a unique batch_id.
    payload: dict (sensor readings)
    """
    return json.dumps({
        "batch_id": str(uuid.uuid4()),
        "type": "TASK",
        "payload": batch_data
    })


def create_ack(batch_id, result):
    """
    Create an ACK message to return to master.
    result: dict
    """
    return json.dumps({
        "batch_id": batch_id,
        "type": "ACK",
        "result": result
    })


def parse_message(msg):
    """
    Parse a JSON string into a dict. Returns {} on failure.
    """
    try:
        return json.loads(msg)
    except Exception:
        return {}
