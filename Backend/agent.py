import openai
import os
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Any
from data_utils import load_sensor_data
import re
import sys
from io import StringIO

# Load .env variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
DATA_DIR = os.getenv("DATA_DIR", "./sensor-data")
MODEL = "gpt-3.5-turbo"
print(f"Using OpenAI model: {MODEL}")

# Field mappings to normalize inconsistent field names (updated with exact keys)
FIELD_MAPPINGS = {
    'temperature': ['temperature', 'temp', 'Temperature (C)', 'Temperature (°C)', 'room_temperature'],
    'humidity': ['humidity', 'Humidity %', 'humid', 'Relative Humidity (%)'],
    'co2': ['co2', 'CO2 (PPM)', 'co2_level', 'carbon_dioxide', 'CO2 (ppm)'],
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
    # DEBUG: Print normalized fields for verification
    print("Normalized fields:", normalized)
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
                        print(f"Skipping line due to error: {e}")
                        continue
    df = pd.DataFrame(all_data)
    # Parse timestamps
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df

def safe_execute_code(code: str, df: pd.DataFrame) -> Dict[str, Any]:
    """
    Safely execute generated code and capture the result.
    Tries multiple ways to get the output.
    """
    # Clean up code block markers
    code = re.sub(r"^```(?:python)?\s*", "", code, flags=re.MULTILINE)
    code = re.sub(r"```\s*$", "", code, flags=re.MULTILINE)
    code = code.strip()
    
    # Create execution environment
    local_env = {
        'df': df,
        'pd': pd,
        'json': json,
        'datetime': datetime
    }
    
    # Capture stdout to get any print statements
    old_stdout = sys.stdout
    captured_output = StringIO()
    
    try:
        sys.stdout = captured_output
        
        # Execute the code
        exec(code, {'__builtins__': __builtins__, 'pd': pd, 'json': json, 'datetime': datetime}, local_env)
        
        # Try to find the result in multiple ways
        result = None
        
        # Method 1: Look for 'output' variable
        if 'output' in local_env:
            result = local_env['output']
        
        # Method 2: Look for 'result' variable
        elif 'result' in local_env:
            result = local_env['result']
        
        # Method 3: Look for the last expression that looks like a result
        else:
            # Try to find variables that look like results
            possible_results = []
            for var_name, var_value in local_env.items():
                if (isinstance(var_value, dict) and 
                    ('summary' in var_value or 'table' in var_value)):
                    possible_results.append(var_value)
            
            if possible_results:
                result = possible_results[-1]  # Take the last one
            else:
                # Method 4: Try to execute the last line if it looks like a variable name
                lines = code.strip().split('\n')
                if lines:
                    last_line = lines[-1].strip()
                    if (last_line and 
                        not last_line.startswith('#') and 
                        '=' not in last_line and 
                        last_line in local_env):
                        result = local_env[last_line]
        
        # If we still don't have a result, try to construct one from available variables
        if result is None:
            summary_vars = [v for k, v in local_env.items() if 'summary' in k.lower() and isinstance(v, str)]
            table_vars = [v for k, v in local_env.items() if 'table' in k.lower() or k.endswith('_df') or isinstance(v, (list, pd.DataFrame))]
            
            result = {}
            
            if summary_vars:
                result['summary'] = summary_vars[0]
            
            if table_vars:
                table_data = table_vars[0]
                if isinstance(table_data, pd.DataFrame):
                    result['table'] = table_data.to_dict(orient='records')
                elif isinstance(table_data, list):
                    result['table'] = table_data
            
            if not result:
                # Last resort: use captured output
                output_text = captured_output.getvalue()
                if output_text.strip():
                    result = {'summary': output_text.strip(), 'table': []}
                else:
                    result = {'summary': 'Code executed but no clear result was returned.', 'table': []}
        
        # Ensure result has the expected structure
        if not isinstance(result, dict):
            result = {'summary': str(result), 'table': []}
        
        if 'summary' not in result:
            result['summary'] = 'Analysis completed.'
        
        if 'table' not in result:
            result['table'] = []
            
        return result
        
    except Exception as e:
        return {
            "summary": f"An error occurred during execution: {str(e)}",
            "table": []
        }
    finally:
        sys.stdout = old_stdout

# Generate Python code using OpenAI to answer a query
async def handle_query(query: str):
    df = load_sensor_data(DATA_DIR)

    preview_df = df.head(5).copy()
    # Convert timestamp column to string to avoid JSON serialization error
    if 'timestamp' in preview_df.columns:
        preview_df['timestamp'] = preview_df['timestamp'].astype(str)

    preview = preview_df.to_dict(orient='records')

    # Enhanced prompt template with better instructions
    prompt = f"""
You are a data analysis assistant. You will be given a user query and a pandas DataFrame called `df`
containing sensor data from different rooms. Each row contains:
- room: name of the room
- timestamp: when the reading was taken (datetime object)
- temperature, humidity, co2: sensor values (floats)

Sample data (first 5 rows):
{json.dumps(preview, indent=2)}

User query:
\"\"\"{query}\"\"\"

Write Python code that:
1. Analyzes the DataFrame to answer the question
2. Creates a result dictionary with these keys:
   - 'summary': a natural language answer (1-2 sentences)
   - 'table': a list of dictionaries with the data rows (if applicable)
   - (optional) 'chartData': for plotting, with keys 'label' and 'value'

IMPORTANT INSTRUCTIONS:
- For day-of-week analysis, use proper day ordering (Monday=0 to Sunday=6)
- Always assign your final result to a variable called 'output'
- End your code with just: output
- Use df['timestamp'].dt.dayofweek for day of week (Monday=0, Sunday=6)
- Use df['timestamp'].dt.hour for hour of day
- When grouping by time periods, sort the results properly

Example structure:
```python
# Your analysis code here
result_df = df.groupby('some_column').mean()
output = {{
    'summary': 'Your analysis summary here',
    'table': result_df.reset_index().to_dict(orient='records')
}}
output
```

Only return the Python code. Do not include explanations or markdown.
"""

    try:
        # Ask GPT for code
        response = openai.ChatCompletion.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a Python data analyst. Always end your code by assigning the result to 'output' and then just write 'output' on the last line."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )
        code = response['choices'][0]['message']['content']
        print("Generated code:\n", code)

        # Execute the code safely
        result = safe_execute_code(code, df)
        
        # Post-process the result to handle day ordering if needed
        if 'table' in result and isinstance(result['table'], list) and result['table']:
            # Check if this looks like day-of-week data that needs reordering
            if any('day' in str(key).lower() for row in result['table'] for key in row.keys()):
                result = fix_day_ordering(result)
        
        return result

    except Exception as e:
        return {
            "summary": f"An error occurred: {str(e)}",
            "table": []
        }

def fix_day_ordering(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fix day ordering in results if they contain day-of-week data.
    """
    if 'table' not in result or not result['table']:
        return result
    
    # Define proper day order
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Check if we have day names in the data
    table = result['table']
    day_column = None
    
    for row in table:
        for key, value in row.items():
            if isinstance(value, str) and value in day_order:
                day_column = key
                break
        if day_column:
            break
    
    if day_column:
        # Sort by proper day order
        day_to_num = {day: i for i, day in enumerate(day_order)}
        table.sort(key=lambda x: day_to_num.get(x[day_column], 999))
        result['table'] = table
    
    return result