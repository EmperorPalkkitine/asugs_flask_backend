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

@app.route('/send-data', methods=['POST'])
def send_data():
    try:
        data = request.json
        user_email = data.get('email')

        # SQL query to insert data into the table
        insert_query = "INSERT INTO User (email) VALUES (%s)"
        cursor.execute(insert_query, (user_email,))
        db.commit()

        return jsonify({"message": "Data inserted successfully!"}), 201
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500
    
@app.route('/get-data/<int:user_id>', methods=['GET'])
def get_data(user_id):
    try:
        # SQL query to fetch data based on user_id
        cursor.execute("SELECT email, name, phone FROM User WHERE id = %s", (user_id,))
        result = cursor.fetchone()  # Fetch one record

        if result:
            user_data = {
                'email': result[0],
                'name': result[1],
                'phone': result[2]
            }
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
