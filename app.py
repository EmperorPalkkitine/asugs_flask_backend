from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
import boto3
import os
import time
import csv
import math
from datetime import datetime

app = Flask(__name__)
CORS(app)

# MySQL Database connection
db = mysql.connector.connect(
    host='gridscout-db.cjkusa2a836b.us-east-2.rds.amazonaws.com',
    user='base_user',
    password='ASUstudent',
    database='Gridscout_main',
)

cursor = db.cursor()

# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-2')
BUCKET_NAME = "gridscout"
DSS_FILE_KEY = "DSS_Tassel.py"  # Key to the OpenDSS .py file in the bucket
CSV_FILE_KEY = "IEEE37_BusXY.csv" # Key to bus coord CSV file in s3 bucket
readable_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
new_dss_file_key = f"DSS_Tassel_{readable_timestamp}.py"

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

@app.route('/process_work_order/<work_order_number>', methods=['GET'])
def process_work_order(work_order_number):
    try:
        # Query work order table to get old and new component IDs and circuit table
        query = """
            SELECT Circuit_ID, Schematic_Component_ID, Bus_1, Bus_2
            FROM Tracking_Table
            WHERE Tracking_ID = %s
        """
        cursor.execute(query, (work_order_number,))
        result = cursor.fetchone()

        if not result:
            return jsonify({"error": "Work order not found"}), 404

        Circuit_ID, Schematic_ID, Bus_1, Bus_2 = result
        print(f"Circuit: {Circuit_ID}, Schematic ID: {Schematic_ID}, Bus 1: {Bus_1}, Bus 2: {Bus_2}")

        # Check if old component ID exists in the circuit table
        check_query = "SELECT * FROM OpenDSS WHERE Circuit_ID = %s AND Schematic_Component_ID = %s"
        cursor.execute(check_query, (Circuit_ID, Schematic_ID,))
        old_component_exists = cursor.fetchone() is not None

        # Determine the action in the application
        if old_component_exists:
            action = "replace_component"
        else:
            action = "add_component"
        
        # Return response for the Flutter app
        return jsonify({  
            "work_order_number": work_order_number,
            "Circuit_ID": Circuit_ID,
            "Schematic_ID": Schematic_ID,
            "Bus_1": Bus_1,
            "Bus_2": Bus_2,
            "action": action,
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Retrieve component data from MySQL
@app.route('/get_data/<component_id>', methods=['GET'])
def get_data(component_id):
    try:
        component_type = request.args.get('component_type')

        if not component_type or not component_id:
            return jsonify({"error": "Component type and ID are required"}), 400

        table_mapping = {
            'Transformer': {
                'table': 'Transformer',
                'columns': ['Phases', 'Windings', 'Xhl', 'Conn1', 'kV1', 'kVA1', 'Conn2', 'kV2', 'kVA2']
            },
            'Capacitor': {
                'table': 'Capacitor',
                'columns': ['Name', 'Phases', 'kV', 'kVAR', 'Bus1']
            },
            'Generator': {
                'table': 'Generator',
                'columns': ['Name', 'Phases', 'kV', 'kW', 'kvar', 'Bus1']
            },
            'Fuse' : {
                'table': 'Fuse',
                'columns': ['Name', 'MonitoredObj', 'MonitoredTerm', 'Status']
            },
            'Reactor' : {
                'table': 'Reactor',
                'columns': ['Name', 'Bus1', 'Bus2', 'Phases', 'R', 'X']
            },
            # can add more component tables for mapping here
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
        reader = csv.reader(csvfile)  # Use csv.reader instead of csv.DictReader
        for row in reader:
            if len(row) >= 3:  # Check if there are enough columns
                bus_coords[row[0]] = (float(row[1]), float(row[2]))  # Assuming columns are Bus, X, Y

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
    if f"{key.casefold()}=" in line:
        parts = line.split()
        for i, part in enumerate(parts):
            if part.startswith(f"{key.casefold()}="):
                parts[i] = f"{key.lower()}={value}"
        line = " ".join(parts)
        print(f"Updated line: {line}")
    return line

# Modify OpenDSS file based on geolocation and parameters

@app.route('/modify_component', methods=['POST'])
def modify_component():
    try:
        data = request.json
        print(f"Received data: {data}")

        parameters = data.get("parameters")
        component_type = data.get("component_type")
        component_id = data.get("component_id")

        # Validate required fields
        if not parameters or not component_type or not component_id:
            return jsonify({"error": "Missing parameters, component_type, or component_id"}), 400

        # Download the Python file from S3
        local_file = "/tmp/temp_python_file.py"
        try:
            s3_client.download_file(BUCKET_NAME, DSS_FILE_KEY, local_file)
        except Exception as e:
            return jsonify({"error": f"Failed to download file from S3: {str(e)}"}), 500

        # Read the existing file content
        with open(local_file, "r") as file:
            lines = file.readlines()

        # Construct a single "Edit" command
        param_string = " ".join([f"{key}={value}" for key, value in parameters.items()])
        edit_command = f'\n\ndss.text("Edit {component_type}.{component_id} {param_string}")\n'

        # Append the single "Edit" command to the file
        with open(local_file, "a") as file:
            file.write(edit_command)
            print(f"Added line: {edit_command.strip()}")

        # Upload the modified file back to S3
        try:
            s3_client.upload_file(local_file, BUCKET_NAME, new_dss_file_key)
        except Exception as e:
            return jsonify({"error": f"Failed to upload file to S3: {str(e)}"}), 500

        return jsonify({"message": "Python file updated successfully.", "new_file": new_dss_file_key}), 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)

''' @app.route('/modify_component', methods=['POST'])
 def modify_component():
    try:
        data = request.json
        print(f"Received data: {data}")
        component_type = data.get("component_type")
        geolocation = data.get("geolocation")
        parameters = data.get("parameters")
        component_id = data.get("component_id")
        name = data.get("name")

        print(f"Component Type: {component_type}")
        print(f"Component ID: {component_id}")
        print(f"Geolocation: {geolocation}")
        print(f"Parameters: {parameters}")
        print(f"Component Name: {name}")

        if not component_type or not geolocation or not parameters or not component_id or not name:
            return jsonify({"error": "Invalid payload"}), 400

        if not geolocation or len(geolocation) != 2:
            return jsonify({"error": "Invalid geolocation. Expected a tuple (x, y)."}), 400

        # Load bus coordinates from S3
        #bus_coords = load_bus_coordinates_from_s3()
        #closest_bus = find_closest_bus(bus_coords, geolocation)
        #print(f"Closest bus found: {closest_bus}")
        #if not closest_bus:
         #   return jsonify({"error": f"No matching bus found for the geolocation {geolocation}"}), 404

        local_file = "/tmp/temp_dss_file.py"
        s3_client.download_file(BUCKET_NAME, DSS_FILE_KEY, local_file)

        with open(local_file, "r") as file:
            lines = file.readlines()
            print(f"Initial lines from DSS file: {lines[:21]}")

        updated_lines = []
        in_component = False
        component_identified = False
        bus_found = False
        component_updated = False  # Track if the component name has been updated
        component_lines_to_update = []  # Store lines to be updated

        for i, line in enumerate(lines):
            print(f"Processing line {i}: {line.strip()}")

            # Start processing the specific component
            if f"New {component_type.capitalize()}" in line:
                in_component = True
                print(f"Component found: {line.strip()}")
                component_start_index = i
                current_component_name = line.split()[1]
                print(f"Component name identified: {current_component_name}")
            if in_component and current_component_name == name:
                print(f"Processing line: {line.strip()} within component: {current_component_name}")
                component_identified = True

            #if in_component:
             #   print(f"Processing line within component block: {line.strip()}")
              #  if f"bus={closest_bus}" in line:
               #     bus_found = True
                #    print(f"Bus found: {line.strip()}")

                #if bus_found and not component_updated:
                    # Update component name and add to updated lines
                 #   updated_line = lines[component_start_index].replace(
                  #      current_component_name.split('.')[-1], component_id
                   # )
                    #print(f"Updated component name at line {component_start_index}: {updated_line.strip()}")
                    #updated_lines[component_start_index] = updated_line
                    #print(f"Component lines to update: {updated_lines}")

                 # Update parameters in the line if any match
            if component_type == "Transformer":
                parts = line.split()
                for i, part in enumerate(parts):
                    if "=" in part:
                     key, value = part.split("=")
                     key_lower = key.lower()

                     for param_key, param_value in parameters.items():
                         base_param = param_key[:-1].lower()
                         winding = param_key[-1]
                         print(f"Base param = {base_param.lower()} and winding = {winding}")

                         if f"wdg={winding}" in line and key_lower == base_param:
                            parts[i] = f"{key}={param_value}"

            updated_param_line = " ".join(parts)
            updated_lines.append(updated_param_line + "\n")
            print(f"Updated parameters: {updated_param_line}")

            # Exit the block when encountering a blank line
            if in_component and line.strip() == "":
                print(f"Exiting component block at line {i}: {line.strip()}")
                in_component = False
                bus_found = False
                component_updated = False
            
            if in_component and not bus_found:
                updated_lines.append(line)
                print(f"Updated lines: {updated_lines}")

            # Append unchanged lines or lines outside of the component block
            if not in_component:
                updated_lines.append(line)
                print(f"Updated lines: {updated_lines}")

        # Append the modified component block to the lines after all changes
        updated_lines.extend(component_lines_to_update)

        print(f"Updated lines verification:\n{updated_lines[:22]}")

        # Write the updated lines back to the local file
        with open(local_file, "w") as file:
            file.writelines(updated_lines)
            print(f"Final updated lines written to file: {updated_lines[:22]}")

        # Upload the updated file to S3
        new_dss_file_key = f"DSS_Tassel_{readable_timestamp}.py"
        print(f"Uploading updated file to S3: {new_dss_file_key}")
        s3_client.upload_file(local_file, BUCKET_NAME, new_dss_file_key)
        print(f"File successfully uploaded to S3: {new_dss_file_key}")

        return jsonify({"message": "Component updated successfully.", "new_file": new_dss_file_key}), 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500'''

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
