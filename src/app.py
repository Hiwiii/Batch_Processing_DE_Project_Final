from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

# Database connection parameters
DB_HOST = "127.0.0.1"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow"
DB_PORT = "5432"  

# Function to get data from PostgreSQL
def get_aggregated_data():
    try:
        # Connect to the database
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )

        cursor = connection.cursor()

        # Execute a query to fetch the aggregated data
        query = "SELECT * FROM public.aggregated_ecommerce_data"
        cursor.execute(query)
        rows = cursor.fetchall()

        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]

        # Convert the data into a list of dictionaries
        data = [dict(zip(column_names, row)) for row in rows]

        cursor.close()
        connection.close()

        return data

    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

# Define a route to get the aggregated data
@app.route('/api/aggregated_data', methods=['GET'])
def aggregated_data():
    data = get_aggregated_data()
    return jsonify(data)

# Main block to run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
