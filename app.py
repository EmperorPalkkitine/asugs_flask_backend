from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
import boto3
import os
import time
import csv
import math

app = Flask(__name__)
CORS(app)

# MySQL Database connection
db = mysql.connector.connect(
    host='gridscout-db.cjkusa2a836b.us-east-2.rds.amazonaws.com',
    user='APP_TO_SQL_USER',
    password='password',
    database='APP_TO_SQL',
)

cursor = db.cursor()

# AWS S3 Configuration
AWS_ACCESS_KEY = "AKIA6IY36CBZOBK5V3HO"
AWS_SECRET_KEY = "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"
AWS_REGION = "us-east-2"
BUCKET_NAME = "gridscout"
DSS_FILE_KEY = "Trial2_Functional_Circuit.py"  # Key to the OpenDSS .py file in the bucket
CSV_FILE_KEY = "IEEE37_BusXY.csv" # Key to bus coord CSV file in s3 bucket
new_dss_file_key = f"Trial2_Functional_Circuit_{int(time.time())}.py"

# S3 Client Initialization
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

@app.route('/')
def home():
    return jsonify({"message": "Flask app is running"})

# Retrieve component data from MySQL
@app.route('/get_data/<component_id>', methods=['GET'])
def get_data(component_id):
    try:
        component_type = request.args.get('component_type')

        if not component_type or not component_id:
            return jsonify({"error": "Component type and ID are required"}), 400

        table_mapping = {
            'Transformer': {
                'table': 'transformers',
                'columns': ['Conn1', 'Kv1', 'Kva1', 'R1', 'Conn2', 'Kv2', 'Kva2', 'R2']
            },
            'Capacitor Bank': {
                'table': 'capacitor_banks',
                'columns': ['Phases', 'Kv', 'Kvar', 'Bus1']
            },
            'Generator': {
                'table': 'generators',
                'columns': ['Phases', 'Kv', 'Kw', 'Kvar', 'Bus1']
            },
            'Load': {
                'table': 'loads',
                'columns': ['Phases', 'Kv', 'Kw', 'Kvar', 'Bus1']
            }
        }

        component_info = table_mapping.get(component_type)
        if not component_info:
            return jsonify({"error": "Invalid component type"}), 400

        table_name = component_info['table']
        required_columns = component_info['columns']
        columns_str = ', '.join(required_columns)
        query = f"SELECT {columns_str} FROM {table_name} WHERE Equipment_ID = %s"

        try:
            cursor.execute(query, (component_id,))
            result = cursor.fetchone()
        except mysql.connector.Error as err:
            return jsonify({"error": f"MySQL query error: {str(err)}"}), 500

        if not result:
            return jsonify({"error": "Component not found"}), 404

        data = dict(zip(required_columns, result))
        return jsonify(data), 200

    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


# Load bus coordinates from the CSV file in S3
def load_bus_coordinates_from_s3():
    local_csv_file = "/tmp/bus_coords.csv"
    
    try:
        s3_client.download_file(BUCKET_NAME, CSV_FILE_KEY, local_csv_file)
    except boto3.exceptions.S3DownloadError as e:
        return jsonify({"error": f"S3 download error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"Unexpected error while downloading file from S3: {str(e)}"}), 500

    bus_coords = {}
    with open(local_csv_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            bus_coords[row["Bus"]] = (float(row["X"]), float(row["Y"]))

    return bus_coords

# Find the closest bus to a given geolocation
def find_closest_bus(bus_coords, target_location):
    target_x, target_y = target_location
    closest_bus = None
    min_distance = float("inf")

    for bus, (x, y) in bus_coords.items():
        distance = math.sqrt((x - target_x) ** 2 + (y - target_y) ** 2)
        if distance < min_distance:
            min_distance = distance
            closest_bus = bus

    return closest_bus

# Update parameters in a specific line
def update_line_parameter(line, key, value):
    if f"{key}=" in line:
        parts = line.split()
        for i, part in enumerate(parts):
            if part.startswith(f"{key}="):
                parts[i] = f"{key}={value}"
        line = " ".join(parts)
    return line

# Modify OpenDSS file based on geolocation and parameters
@app.route('/modify_component', methods=['POST'])
def modify_component():
    try:
        data = request.json
        component_type = data.get("component_type")
        geolocation = data.get("geolocation")
        parameters = data.get("parameters")
        component_id = data.get("component_id")

        if not component_type or not geolocation or not parameters:
            return jsonify({"error": "Invalid payload"}), 400
        
        if not geolocation or len(geolocation) != 2:
            return jsonify({"error": "Invalid geolocation. Expected a tuple (x, y)."}), 400

        # Load bus coordinates from S3
        bus_coords = load_bus_coordinates_from_s3()
        closest_bus = find_closest_bus(bus_coords, geolocation)
        if not closest_bus:
            return jsonify({"error": f"No matching bus found for the geolocation {geolocation}"}), 404

        local_file = "/tmp/temp_dss_file.py"
        s3_client.download_file(BUCKET_NAME, DSS_FILE_KEY, local_file)

        with open(local_file, "r") as file:
            lines = file.readlines()

        updated_lines = []
        in_component = False

        for line in lines:
            if f"New {component_type.capitalize()}" in line and f"Bus1={closest_bus}" in line:
                in_component = True

            if in_component:
                if "New" in line and not line.startswith(f"New {component_type.capitalize()}"):
                    in_component = False

            if in_component:
                if f"New {component_type.capitalize()}." in line:
                    component_name = line.split('.')[1].strip()
                    updated_name = component_id
                    line = line.replace(component_name, updated_name)
                    
                for key, value in parameters.items():
                    if key in line:
                        line = update_line_parameter(line, key, value)

            updated_lines.append(line)

        with open(local_file, "w") as file:
            file.writelines(updated_lines)

        s3_client.upload_file(local_file, BUCKET_NAME, new_dss_file_key)

        return jsonify({"message": "Component updated successfully.", "new_file": new_dss_file_key}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
