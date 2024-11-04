from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector

app = Flask(__name__)
CORS(app)

# MySQL Database connection
db = mysql.connector.connect(
    host= 'gridscout-db.cjkusa2a836b.us-east-2.rds.amazonaws.com',       # e.g., localhost
    user='APP_TO_SQL_USER',       # e.g., root
    password='password',
    database='APP_TO_SQL',
)

cursor = db.cursor()

#write data to table
@app.route('/send-data', methods=['POST'])
def send_data():
    try:
        data = request.json
        component_id = data.get('component_id')
        component_type = data.get('component_type')
        electrical_specifications = data.get('electrical_specifications')
        connection_points = data.get('connection_points')
        geolocation = data.get('geolocation')
        installation_date = data.get('installation_date')
        operation_status = data.get('operation_status')
        der = data.get('der')        

        # SQL query to insert data into the User table
        insert_query = """
            INSERT INTO ParameterTests 
            (component_id, component_type, electrical_specifications, connection_points, geolocation, installation_date, operation_status, der)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            component_id, component_type, electrical_specifications,
            connection_points, geolocation, installation_date,
            operation_status, der
        ))
        db.commit()

        return jsonify({"message": "Data inserted successfully"}), 201
    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500

#get data from table   
@app.route('/get-data/<string:component_id>', methods=['GET'])
def get_data_by_component_id(component_id):
    try:
        # SQL query to fetch data based on component_id
        query = """
            SELECT component_id, component_type, electrical_specifications, connection_points,
                   geolocation, installation_date, operation_status, der
            FROM ParameterTests
            WHERE component_id = %s
        """
        cursor.execute(query, (component_id,))
        result = cursor.fetchone()

        if result:
            # Map the result to a dictionary
            data = {
                'component_id': result[0],
                'component_type': result[1],
                'electrical_specifications': result[2],
                'connection_points': result[3],
                'geolocation': result[4],
                'installation_date': result[5],
                'operation_status': result[6],
                'der': result[7],
            }
            return jsonify(data), 200
        else:
            return jsonify({"error": "Component not found"}), 404

    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
