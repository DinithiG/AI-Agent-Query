import json
import os
import pandas as pd
from pathlib import Path

FIELD_MAPPINGS = {
    'temperature': ['temperature', 'temp', 'Temperature (C)', 'room_temperature'],
    'humidity': ['humidity', 'Humidity %', 'humid'],
    'co2': ['co2', 'CO2 (PPM)', 'co2_level', 'carbon_dioxide'],
    'timestamp': ['timestamp', 'time', 'datetime']
}

def normalize_fields(raw):
    normalized = {}
    for standard, variants in FIELD_MAPPINGS.items():
        for v in variants:
            if v in raw:
                normalized[standard] = raw[v]
                break
    return normalized

def load_sensor_data(data_dir: str = "./sensor-data") -> pd.DataFrame:
    all_data = []
    path = Path(data_dir)

    for file in path.glob("*.ndjson"):
        room_name = file.stem.replace("sensor_data_", "")  # e.g. Room 1
        with open(file, 'r') as f:
            for line in f:
                try:
                    raw = json.loads(line)
                    normalized = normalize_fields(raw)
                    normalized['room'] = room_name
                    all_data.append(normalized)
                except Exception:
                    continue

    df = pd.DataFrame(all_data)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df
