from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
from concurrent.futures import ThreadPoolExecutor
import json
import os
import logging
import sys

# Add the src directory to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Now we can import modules from the src directory
from src.main import run_consumer

# Initialize Flask app and SocketIO for real-time logging
app = Flask(__name__)
socketio = SocketIO(app)

# Define global variables for logging and execution
executor = ThreadPoolExecutor(max_workers=5)

# Load connections configurations
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.abspath(os.path.join(script_dir, '.'))

connections_config_path = os.path.join(base_dir, 'configurations', 'connections.json')

with open(connections_config_path, 'r') as f:
    connections_config = json.load(f)

# Determine the directory containing connections.json
connections_dir = os.path.dirname(connections_config_path)

# Resolve SSL certificate paths to absolute paths relative to connections.json
ssl_config = connections_config.get('ssl_config', {})
for key in ['ca', 'cert', 'key']:
    if key in ssl_config:
        ssl_path = ssl_config[key]
        # If the path is relative, make it absolute based on connections_dir
        if not os.path.isabs(ssl_path):
            ssl_config[key] = os.path.abspath(os.path.join(connections_dir, ssl_path))
        # Optionally, verify that the file exists
        if not os.path.exists(ssl_config[key]):
            raise FileNotFoundError(f"SSL certificate file not found: {ssl_config[key]}")
        else:
            app.logger.info(f"Resolved SSL path for {key}: {ssl_config[key]}")
connections_config['ssl_config'] = ssl_config

@app.route('/')
def index():
    """ Render dashboard home """
    return render_template('index.html')

@app.route('/start_consumers', methods=['POST'])
def start_consumers():
    """ Start Kafka consumers based on user-provided topic configurations """

    try:
        # Retrieve form data
        topic_name = request.form.get('topic_name')
        group_id = request.form.get('group_id')
        num_messages = int(request.form.get('num_messages', 0))
        reset_offsets = request.form.get('reset_offsets', 'false').lower() == 'true'  

        # Parse starting offsets
        starting_offsets = {}
        for key, value in request.form.items():
            if 'starting_offsets' in key and 'partition' in key:
                partition_index = key.split('[')[1].split(']')[0]
                partition = int(value)
                offset = int(request.form[f'starting_offsets[{partition_index}][offset]'])
                starting_offsets[partition] = offset

        # Parse offset increments
        offset_increment = {}
        for key, value in request.form.items():
            if 'offset_increment' in key and 'partition' in key:
                partition_index = key.split('[')[1].split(']')[0]
                partition = int(value)
                increment = int(request.form[f'offset_increment[{partition_index}][increment]'])
                offset_increment[partition] = increment

        # Get Kafka connection settings
        bootstrap_servers = connections_config.get('bootstrap_servers')
        ssl_config = connections_config.get('ssl_config')
        schema_registry_url = connections_config.get('schema_registry_url')
        azure_storage_account_name = connections_config.get('azure_storage_account_name')
        azure_storage_account_key = connections_config.get('azure_storage_account_key')
        azure_filesystem_name = connections_config.get('azure_filesystem_name')

        # Submit the consumer task to the thread pool and get the Future object
        future = executor.submit(
            run_consumer,
            topic=topic_name,
            group_id=group_id,
            num_messages=num_messages,
            bootstrap_servers=bootstrap_servers,
            ssl_config=ssl_config,
            schema_registry_url=schema_registry_url,
            azure_storage_account_name=azure_storage_account_name,
            azure_storage_account_key=azure_storage_account_key,
            azure_filesystem_name=azure_filesystem_name,
            reset_offsets=reset_offsets,
            starting_offsets=starting_offsets if starting_offsets else None,
            offset_increment=offset_increment if offset_increment else None,
        )

        return {"status": "success", "message": f"Consumer for topic '{topic_name}' started successfully"}
        
    except Exception as e:
        app.logger.error(f"General error in /start_consumers: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)