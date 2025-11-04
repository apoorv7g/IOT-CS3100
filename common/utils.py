# distributed/common/utils.py
import time
import logging
import os
import sys

# Ensure root path is in sys.path for simple imports when running files directly
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)

def timestamp():
    return time.strftime("%H:%M:%S")

# small helper for safe sleep
def safe_sleep(seconds):
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        pass
