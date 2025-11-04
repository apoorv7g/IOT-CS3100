# distributed/master/data_provider.py
import random
import time
import serial


class DataProvider:
    """
    Provides batch data. Mode can be 'simulate' or 'arduino'.
    If arduino mode is used, pass serial_port (e.g., '/dev/ttyUSB0' or 'COM3').
    """

    def __init__(self, mode="simulate", serial_port=None, baudrate=9600):
        self.mode = mode
        if self.mode == "arduino":
            if serial_port is None:
                raise ValueError("Serial port must be provided for Arduino mode")
            # Initialize serial connection (blocking reads with timeout)
            self.ser = serial.Serial(serial_port, baudrate, timeout=1)
            # Wait for Arduino reset
            time.sleep(2)
        else:
            self.ser = None

    def get_sample(self):
        """
        Return a single sample (dictionary with 5 sensor values).
        When used with time-based batching, master will call this repeatedly
        during the window or once per batch depending on design.
        """
        if self.mode == "simulate":
            return {
                "temperature": round(20 + random.random() * 10, 2),
                "humidity": round(30 + random.random() * 40, 2),
                "pressure": round(990 + random.random() * 30, 2),
                "light": round(random.random() * 100, 2),
                "vibration": round(random.random() * 5, 2)
            }
        elif self.mode == "arduino":
            try:
                line = self.ser.readline().decode().strip()
                if not line:
                    return None
                # Expect CSV from Arduino: temp,hum,press,light,vib
                parts = [p.strip() for p in line.split(",")]
                if len(parts) < 5:
                    return None
                return {
                    "temperature": float(parts[0]),
                    "humidity": float(parts[1]),
                    "pressure": float(parts[2]),
                    "light": float(parts[3]),
                    "vibration": float(parts[4])
                }
            except Exception:
                return None

    def get_batch_for_window(self, window_sec):
        """
        Build a batch collected over window_sec seconds.
        This function samples once per 0.5s internally (simple approach).
        Returns the last sample when nothing available.
        """
        end_t = time.time() + window_sec
        last_valid = None
        samples = []
        while time.time() < end_t:
            sample = self.get_sample()
            if sample:
                samples.append(sample)
                last_valid = sample
            time.sleep(0.5)
        # For simplicity, aggregate by taking mean of each field across samples
        if not samples and last_valid:
            return last_valid
        elif not samples:
            # No data at all
            return {"temperature": 0, "humidity": 0, "pressure": 0, "light": 0, "vibration": 0}
        else:
            keys = ["temperature", "humidity", "pressure", "light", "vibration"]
            agg = {}
            for k in keys:
                vals = [s[k] for s in samples if s and k in s]
                agg[k] = round(sum(vals) / len(vals), 3) if vals else 0
            return agg
