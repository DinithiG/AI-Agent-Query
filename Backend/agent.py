import openai
import os
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Any
from data_utils import load_sensor_data

# Load .env variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
DATA_DIR = os.getenv("DATA_DIR", "./sensor-data")
MODEL = os.getenv("MODEL_NAME", "gpt-4")

# Field mappings to normalize inconsistent field names
FIELD_MAPPINGS = {
    'temperature': ['temperature', 'temp', 'Temperature (C)', 'room_temperature'],
    'humidity': ['humidity', 'Humidity %', 'humid'],
    'co2': ['co2', 'CO2 (PPM)', 'co2_level', 'carbon_dioxide'],
    'timestamp': ['timestamp', 'time', 'datetime']
}

# Normalize a JSON line based on known mappings
def normalize_fields(raw: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {}
    for standard, variants in FIELD_MAPPINGS.items():
        for v in variants:
            if v in raw:
                normalized[standard] = raw[v]
                break
    return normalized

# Read all sensor files into one DataFrame with room name
def load_all_data():
    all_data = []
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".txt") or filename.endswith(".json"):
            room = os.path.splitext(filename)[0]
            with open(os.path.join(DATA_DIR, filename)) as f:
                for line in f:
                    try:
                        raw = json.loads(line)
                        data = normalize_fields(raw)
                        data['room'] = room
                        all_data.append(data)
                    except Exception as e:
                        continue
    df = pd.DataFrame(all_data)
    # Parse timestamps
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df

# Generate Python code using OpenAI to answer a query
async def handle_query(query: str):
    df = load_sensor_data(DATA_DIR)
    preview = df.head(5).to_dict(orient='records')

    # Prompt template
    prompt = f"""
You are a data analysis assistant. You will be given a user query and a pandas DataFrame called `df`
containing sensor data from different rooms. Each row contains:
- room: name of the room
- timestamp: when the reading was taken
- temperature, humidity, co2: sensor values (floats)

Sample data (first 5 rows):
{json.dumps(preview, indent=2)}

User query:
\"\"\"{query}\"\"\"

Write Python code that:
- Analyzes the DataFrame to answer the question
- Outputs a dictionary with 2 keys:
    - 'summary': a natural language answer (1-2 sentences)
    - 'table': a list of dictionaries with rows (if applicable)
    - (optional) 'chartData': for plotting, with keys 'label' and 'value'

Only return the Python code. Do not explain.
"""

    try:
        # Ask GPT for code
        response = openai.ChatCompletion.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a Python data analyst."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )
        code = response['choices'][0]['message']['content']
        print("Generated code:\n", code)

        # Define safe exec environment
        local_env = {'df': df}
        exec(code, {}, local_env)
        result = local_env.get('result', {"summary": "No result returned."})
        return result

    except Exception as e:
        return {
            "summary": f"An error occurred: {str(e)}",
            "table": []
        }
