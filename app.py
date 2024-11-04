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
@app.route('/get-data/User/<string:identifier>', methods=['GET'])
def get_user_data(identifier):
    try:
        if identifier.lower() == 'null':
            # SQL query to fetch all users with NULL email
            query = "SELECT id, email, name FROM User WHERE id IS NULL"
            cursor.execute(query)
            results = cursor.fetchall()  # Fetch all records

            if results:
                users = [{'id': row[0], 'email': row[1], 'name': row[2]} for row in results]
                return jsonify(users), 200
            else:
                return jsonify({"error": "No users found with null id"}), 404
        else:
            # SQL query to fetch data based on user ID
            query = "SELECT id, email, name FROM User WHERE id = %s"
            cursor.execute(query, (identifier,))
            result = cursor.fetchone()

            if result:
                user_data = {
                    'id': result[0],
                    'email': result[1],
                    'name': result[2]
                }
                print("Response Data: ", user_data)
                return jsonify(user_data), 200
            else:
                return jsonify({"error": "User not found"}), 404

    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
