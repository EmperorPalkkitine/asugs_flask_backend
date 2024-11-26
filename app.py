from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
import boto3
import os
import time

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
DSS_FILE_KEY = "Trial2_Functional_Circuit.py" # Key to the OpenDSS .py file in the bucket
new_dss_file_key = f"Trial2_Functional_Circuit_{int(time.time())}.py"

# S3 Client Initialization
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# Write data to MySQL table
#@app.route('/send-data', methods=['POST'])
#def send_data():
#    try:
#        data = request.json
#        component_id = data.get('component_id')
#        component_type = data.get('component_type')
#        electrical_specifications = data.get('electrical_specifications')
#        connection_points = data.get('connection_points')
#        geolocation = data.get('geolocation')
#        installation_date = data.get('installation_date')
#        operation_status = data.get('operation_status')
#        der = data.get('der')
#
        # SQL query to insert data into the table
#        insert_query = """
#            INSERT INTO ParameterTests 
#            (component_id, component_type, electrical_specifications, connection_points, geolocation, installation_date, operation_status, der)
#            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#        """
#        cursor.execute(insert_query, (
#            component_id, component_type, electrical_specifications,
#            connection_points, geolocation, installation_date,
#            operation_status, der
#        ))
#        db.commit()
#
#        return jsonify({"message": "Data inserted successfully"}), 201
#    except mysql.connector.Error as err:
#        print(f"Database Error: {err}")
#        return jsonify({"error": str(err)}), 500
#    except Exception as e:
#        print(f"Unexpected error: {e}")
#        return jsonify({"error": "An unexpected error occurred"}), 500


# Get data from MySQL table
@app.route('/get_data/<component_id>', methods=['GET'])
def get_data(component_id):
    try:
        # Retrieve component type and ID from query parameters
        component_type = request.args.get('component_type')

        print(f"Component Type: {component_type}")

        if not component_type or not component_id:
            return jsonify({"error": "Component type and ID are required"}), 400

        # Map component types to their respective table names and required columns
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

        component_info = table_mapping.get(component_type.lower())
        if not component_info:
            return jsonify({"error": "Invalid component type"}), 400

        table_name = component_info['table']
        required_columns = component_info['columns']

        # Construct SQL query to fetch only the required columns
        columns_str = ', '.join(required_columns)
        query = f"SELECT {columns_str} FROM {table_name} WHERE Equipment_ID = %s"
        cursor.execute(query, (component_id,))
        result = cursor.fetchone()

        if not result:
            return jsonify({"error": "Component not found"}), 404

        # Map results to a dictionary
        data = dict(zip(required_columns, result))
        return jsonify(data), 200

    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500


# Modify the OpenDSS .py file based on the given input
@app.route('/modify_component', methods=['POST'])
def modify_component():
    """
    API to modify components in the OpenDSS .py file based on user input.
    Example payload:
    {
        "component_type": "line",
        "component_name": "L1",
        "parameters": {
            "length": 1.2,
            "linecode": "updated_code"
        }
    }
    """
    data = request.json
    component_type = data.get("component_type")
    component_name = data.get("component_name")
    parameters = data.get("parameters")

    if not component_type or not component_name or not parameters:
        return jsonify({"error": "Invalid payload"}), 400

    # Download the .py file from S3
    local_file = "/tmp/temp_dss_file.py"
    s3_client.download_file(BUCKET_NAME, DSS_FILE_KEY, local_file)

    # Modify the file
    with open(local_file, "r") as file:
        lines = file.readlines()

    updated_lines = modify_dss_file(lines, component_type, component_name, parameters)

    # Write the updated content back to the file
    with open(local_file, "w") as file:
        file.writelines(updated_lines)

    # Upload the updated file back to S3
    s3_client.upload_file(local_file, BUCKET_NAME, new_dss_file_key)

    return jsonify({"message": "Component updated successfully."}), 200


def modify_dss_file(lines, component_type, component_name, parameters):
    """
    Modify the OpenDSS .py file lines to update the specified component.
    """
    updated_lines = []
    in_component = False

    for line in lines:
        if f"{component_type.capitalize()}.{component_name}" in line:
            in_component = True

        if in_component:
            # Stop updating when we encounter an unrelated line
            if "New" in line and f"{component_type.capitalize()}.{component_name}" not in line:
                in_component = False

        if in_component:
            # Update parameters for the specified component
            for key, value in parameters.items():
                if key in line:
                    line = update_line_parameter(line, key, value)
        
        updated_lines.append(line)

    return updated_lines


def update_line_parameter(line, key, value):
    """
    Update a specific parameter in a line.
    """
    parts = line.split()
    for i, part in enumerate(parts):
        if key in part:
            parts[i] = f"{key}={value}"
    return " ".join(parts) + "\n"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
