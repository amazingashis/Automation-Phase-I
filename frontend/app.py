# Simple Flask Backend for AI Schema Inference Workflow
# This provides API endpoints to connect the frontend with the existing Python workflow

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import json
import pandas as pd
import tempfile
import sys

# Add the agents directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'agents'))

app = Flask(__name__)
CORS(app)

# Import your existing agents
try:
    from agent_header_check import check_header_and_infer
    from import_schema_generator import generate_import_script
except ImportError as e:
    print(f"Warning: Could not import agents: {e}")

# LLM function (same as main.py)
def lmstudio_llm(prompt):
    import requests
    url = "http://127.0.0.1:1234/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    data = {
        "model": "google/gemma-3n-e4b",
        "messages": [
            {"role": "system", "content": "You are a helpful AI assistant for data engineering."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 200,
        "temperature": 0
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]

@app.route('/')
def serve_frontend():
    return send_from_directory('frontend', 'index.html')

@app.route('/<path:filename>')
def serve_static(filename):
    return send_from_directory('frontend', filename)

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400
    
    # Save uploaded file temporarily
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, file.filename)
    file.save(temp_path)
    
    # Get basic file info
    df = pd.read_csv(temp_path, nrows=5)
    
    return jsonify({
        'success': True,
        'filename': file.filename,
        'temp_path': temp_path,
        'rows_sample': len(df),
        'columns': len(df.columns)
    })

@app.route('/api/check-headers', methods=['POST'])
def check_headers():
    data = request.get_json()
    file_path = data.get('file_path')
    
    if not file_path or not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 400
    
    try:
        # Read first few rows to check if file has headers
        df_with_header = pd.read_csv(file_path, nrows=5)
        df_no_header = pd.read_csv(file_path, nrows=5, header=None)
        
        # Simple heuristic: check if first row looks like headers
        first_row = df_no_header.iloc[0].tolist()
        has_headers = all(isinstance(val, str) and not str(val).replace('.', '').isdigit() for val in first_row if pd.notna(val))
        
        if has_headers:
            # Extract existing headers and generate schema directly
            headers = list(df_with_header.columns)
            schema = {
                "type": "struct",
                "fields": []
            }
            
            for col in headers:
                # Infer data type from sample data
                sample_values = df_with_header[col].dropna().head(10).tolist()
                dtype = infer_data_type_from_samples(sample_values)
                nullable = df_with_header[col].isnull().any()
                
                schema["fields"].append({
                    "name": col,
                    "type": dtype,
                    "nullable": bool(nullable),
                    "metadata": {}
                })
        
        return jsonify({
            'success': True,
            'has_headers': has_headers,
            'headers': headers if has_headers else None,
            'schema': schema if has_headers else None
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def infer_data_type_from_samples(sample_values):
    """Infer data type from sample values"""
    if not sample_values:
        return 'string'
    
    # Check if all values are integers
    try:
        for val in sample_values:
            if pd.notna(val):
                int(val)
        return 'integer'
    except (ValueError, TypeError):
        pass
    
    # Check if all values are floats
    try:
        for val in sample_values:
            if pd.notna(val):
                float(val)
        return 'float'
    except (ValueError, TypeError):
        pass
    
    # Check if values look like dates
    sample_str = str(sample_values[0]) if sample_values else ''
    if any(pattern in sample_str.lower() for pattern in ['date', '/', '-', 'time']):
        return 'date'
    
    return 'string'

@app.route('/api/analyze-headers', methods=['POST'])
def analyze_headers():
    data = request.get_json()
    file_path = data.get('file_path')
    data_type = data.get('data_type', '')
    
    if not file_path or not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 400
    
    try:
        # Use your existing header check agent
        result = check_header_and_infer(file_path, llm=lmstudio_llm)
        
        if isinstance(result, tuple):
            schema, header_inferred = result
        else:
            schema = result
            header_inferred = True
        
        headers = [field['name'] for field in schema['fields']]
        
        return jsonify({
            'success': True,
            'headers': headers,
            'header_inferred': header_inferred,
            'schema': schema
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/generate-script', methods=['POST'])
def generate_script():
    data = request.get_json()
    file_path = data.get('file_path')
    schema = data.get('schema')
    
    if not file_path or not schema:
        return jsonify({'error': 'Missing file_path or schema'}), 400
    
    try:
        # Save schema to temporary file
        temp_dir = tempfile.gettempdir()
        schema_path = os.path.join(temp_dir, 'temp_schema.json')
        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=4)
        
        # Generate script using your existing agent
        script = generate_import_script(file_path, schema_path)
        
        return jsonify({
            'success': True,
            'script': script
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'message': 'AI Schema Inference API is running'
    })

if __name__ == '__main__':
    print("Starting AI Schema Inference Frontend...")
    print("Open your browser to: http://localhost:5000")
    app.run(debug=True, port=5000)
