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
DSS_FILE_KEY = "Tassel.py"  # Key to the OpenDSS .py file in the bucket
CSV_FILE_KEY = "IEEE37_BusXY.csv" # Key to bus coord CSV file in s3 bucket
readable_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
new_dss_file_key = f"Tassel{readable_timestamp}.py"

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
@app.route('/get_data/<equipment_id>', methods=['GET'])
def get_data(equipment_id):
    try:
        component_type = request.args.get('component_type')
        print(f"Received component_type: {component_type}, equipment_id: {equipment_id}")  # Debug print

        if not component_type or not equipment_id:
            return jsonify({"error": "Component type and ID are required"}), 400

        table_mapping = {
            'Transformer': {
                'table': 'Transformer',
                'columns': ['Phases', 'Windings', 'Xhl', 'Conn1', 'kV1', 'kVA1', 'Conn2', 'kV2', 'kVA2']
            },
            'Capacitor': {
                'table': 'Capacitor',
                'columns': ['Phases', 'kVAR', 'kV']
            },
            'Generator': {
                'table': 'Generator',
                'columns': ['Phases', 'kV', 'kW', 'kvar','Model']
            },
            'Fuse' : {
                'table': 'Fuse',
                'columns': ['MonitoredObj', 'MonitoredTerm', 'Status']
            },
            'Reactor' : {
                'table': 'Reactor',
                'columns': ['Phases', 'X', 'kV', 'kVAR']
            },
            # can add more component tables for mapping here
        }

        component_info = table_mapping.get(component_type)
        if not component_info:
            return jsonify({"error": "Invalid component type"}), 400

        table_name = component_info['table']
        required_columns = component_info['columns']
        columns_str = ', '.join(required_columns)
        
        # Ensure query is defined before it is used
        query = f"SELECT {columns_str} FROM {table_name} WHERE Equipment_ID = %s"
        cursor.execute(query, (equipment_id,))
        print(f"Executing query: {query}, with equipment_id: {equipment_id}")  # Debug print
        
        # Make sure any previous results are cleared
        cursor.fetchall()  # Clear any remaining results
        
        result = cursor.fetchone()

        if not result:
            return jsonify({"error": "Component not found"}), 404

        data = dict(zip(required_columns, result))
        return jsonify(data), 200

    except Exception as e:
        print(f"Error: {str(e)}")  # Debug print
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


#Modify component method using py dss commands and update "Instance_Tracker" table in mySQL database
@app.route('/modify_component', methods=['POST'])
def modify_component():
    try:
        data = request.json
        print(f"Received data: {data}")

        parameters = data.get("parameters")
        component_type = data.get('component_type')
        component_id = data.get('component_id')
        equipment_id = data.get('equipment_id')
        serial_num = data.get('serial_number')
        geo_loc = data.get('geolocation')
        user_id = data.get('user_id')
        tracking_id = data.get('work_order_id')
        notes = data.get('notes')

        if not parameters:
            return jsonify({"error": "Missing parameters"}), 400
        
         # Download Python file from S3
        local_file = "/tmp/temp_python_file.py"
        s3_client.download_file(BUCKET_NAME, DSS_FILE_KEY, local_file)

        # Read the existing file content
        with open(local_file, "r") as file:
            lines = file.readlines()

        # Prepare lists for connections, voltages, and kVA if the component is a transformer
        if component_type.lower() == "transformer":
            conns = [parameters["Conn1"], parameters["Conn2"]]
            kvs = [parameters["kV1"], parameters["kV2"]]
            kvas = [parameters["kVA1"], parameters["kVA2"]]
            edit_command = f'dss.text("Edit Transformer.{component_id} Windings={parameters["Windings"]} Phases={parameters["Phases"]} Xhl={parameters["Xhl"]} Conns={conns} kVs={kvs} kVAs={kvas}")\n\n'
            print(f"Generated Edit command for Transformer: {edit_command}")
        
        elif component_type.lower() == "capacitor":
            edit_command = f'dss.text("Edit Capacitor.{component_id} Bus1={parameters["Bus1"]} Phases={parameters["Phases"]} kvar={parameters["kVAR"]} kV={parameters["kV"]}")\n\n'
            print(f"Generated Edit command for Capacitor: {edit_command}")

        elif component_type.lower() == "generator":
            edit_command = f'dss.text("Edit Generator.{component_id} Bus1={parameters["Bus1"]} Phases={parameters["Phases"]} kV={parameters["kV"]} kW={parameters["kW"]} kvar={parameters["kvar"]} Model={parameters["Model"]}")\n\n'
            print(f"Generated Edit command for Generator: {edit_command}")

        elif component_type.lower() == "fuse":
            edit_command = f'dss.text("Edit Fuse.{component_id} MonitoredObj={parameters["MonitoredObj"]} RatedCurrent={parameters["RatedCurrent"]}")\n\n'
            print(f"Generated Edit command for Fuse: {edit_command}")
        
        elif component_type.lower() == "reactor":
            edit_command = f'dss.text("Edit Reactor.{component_id} Bus1={parameters["Bus1"]} Phases={parameters["Phases"]} kV={parameters["kV"]} kvar={parameters["kVAR"]}")\n\n'
            print(f"Generated Edit command for Reactor: {edit_command}")
        
        else: 
            # Error detection for unsupported component type
            print(f'Component Type: {component_type} not found')
            return jsonify({"error": f"Unsupported component type: {component_type}"}), 400

        # Find where "Save Circuit" is in the file and insert before it
        for i, line in enumerate(lines):
            if '# Save' in line:  # Look for Save Circuit line
                lines.insert(i, edit_command)  # Insert edit command before it
                break
        else:
            # If "Save Circuit" isn't found, just append at the end
            lines.append(edit_command)

        # Write updated content back to the file
        with open(local_file, "w") as file:
            file.writelines(lines)

        # Upload the modified file back to S3
        try:
            s3_client.upload_file(local_file, BUCKET_NAME, new_dss_file_key)
        except Exception as e:
            return jsonify({"error": f"Failed to upload file to S3: {str(e)}"}), 500

        insert_query = """
            INSERT INTO Instance_Tracker 
            (Equipment_ID, Serial_Number, Geo_Loc, User_ID, Tracking_ID, Notes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (
            equipment_id,
            serial_num,
            geo_loc,
            user_id,
            tracking_id,
            notes
        )

        cursor = db.cursor()
        cursor.execute(insert_query, values)
        db.commit()
        cursor.close()

        return jsonify({'message': "Component modified and instance tracked successfully", "new_file": new_dss_file_key}), 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)

# add component method using py dss commands
@app.route('/add_component', methods=['POST'])
def add_component():
    try:
        data = request.json
        print(f"Received data: {data}")

        parameters = data.get("parameters")
        component_type = data.get('component_type')
        component_id = data.get('component_id')
        equipment_id = data.get('equipment_id')
        serial_num = data.get('serial_number')
        geo_loc = data.get('geolocation')
        user_id = data.get('user_id')
        tracking_id = data.get('work_order_id')
        notes = data.get('notes')

        if not parameters:
            return jsonify({"error": "Missing parameters"}), 400
        
         # Download Python file from S3
        local_file = "/tmp/temp_python_file.py"
        s3_client.download_file(BUCKET_NAME, DSS_FILE_KEY, local_file)

        # Read the existing file content
        with open(local_file, "r") as file:
            lines = file.readlines()

        # Prepare lists for connections, voltages, and kVA if the component is a transformer
        if component_type.lower() == "transformer":
            buses = [parameters["Bus1"], parameters["Bus2"]]
            conns = [parameters["Conn1"], parameters["Conn2"]]
            kvs = [parameters["kV1"], parameters["kV2"]]
            kvas = [parameters["kVA1"], parameters["kVA2"]]
            new_command = f'dss.text("New Transformer.{component_id} Windings={parameters["Windings"]} Phases={parameters["Phases"]} Xhl={parameters["Xhl"]} buses={buses} Conns={conns} kVs={kvs} kVAs={kvas}")\n\n'
            print(f"Generated New command for Transformer: {new_command}")

        elif component_type.lower() == "capacitor":
            new_command = f'dss.text("New Capacitor.{component_id} Bus1={parameters["Bus1"]} Phases={parameters["Phases"]} kvar={parameters["kVAR"]} kV={parameters["kV"]}")\n\n'
            print(f"Generated New command for Capacitor: {new_command}")

        elif component_type.lower() == "generator":
            new_command = f'dss.text("New Generator.{component_id} Bus1={parameters["Bus1"]} Phases={parameters["Phases"]} kV={parameters["kV"]} kW={parameters["kW"]} kvar={parameters["kvar"]} Model={parameters["Model"]}")\n\n'
            print(f"Generated New command for Generator: {new_command}")

        elif component_type.lower() == "fuse":
            new_command = f'dss.text("New Fuse.{component_id} MonitoredObj={parameters["MonitoredObj"]} RatedCurrent={parameters["RatedCurrent"]}")\n\n'
            print(f"Generated New command for Fuse: {new_command}")
        
        elif component_type.lower() == "reactor":
            new_command = f'dss.text("New Reactor.{component_id} Bus1={parameters["Bus1"]} Phases={parameters["Phases"]} kV={parameters["kV"]} kvar={parameters["kVAR"]}")\n\n'
            print(f"Generated New command for Reactor: {new_command}")
        
        else: 
            # Error detection for unsupported component type
            print(f'Component Type: {component_type} not found')

        # Find where "Save Circuit" is in the file and insert before it
        for i, line in enumerate(lines):
            if '# Save' in line:  # Look for Save Circuit line
                lines.insert(i, new_command)  # Insert edit command before it
                break
        else:
            # If "Save Circuit" isn't found, just append at the end
            lines.append(new_command)

        # Write updated content back to the file
        with open(local_file, "w") as file:
            file.writelines(lines)

        # Upload the modified file back to S3
        try:
            s3_client.upload_file(local_file, BUCKET_NAME, new_dss_file_key)
        except Exception as e:
            return jsonify({"error": f"Failed to upload file to S3: {str(e)}"}), 500
        
        insert_query = """
            INSERT INTO Instance_Tracker 
            (Equipment_ID, Serial_Number, Geo_Loc, User_ID, Tracking_ID, Notes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (
            equipment_id,
            serial_num,
            geo_loc,
            user_id,
            tracking_id,
            notes
        )

        cursor = db.cursor()
        cursor.execute(insert_query, values)
        db.commit()
        cursor.close()

        return jsonify({'message': "Component modified and instance tracked successfully", "new_file": new_dss_file_key}), 200
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
