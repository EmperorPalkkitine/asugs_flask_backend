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
        insert_query = "INSERT INTO users (email) VALUES (%s)"
        cursor.execute(insert_query, (user_email,))
        db.commit()

        return jsonify({"message": "Data inserted successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
