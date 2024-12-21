from flask import Flask, jsonify, request
from pyhive import hive

app = Flask(__name__)

HIVE_HOST = 'localhost' # on cluster use hiveserver2
HIVE_PORT = 10000
HIVE_USER = 'root'
HIVE_DATABASE = 'default'

def get_hive_connection():
    return hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, database=HIVE_DATABASE)

@app.route('/api/query', methods=['GET'])
def query_hive_api():
    # Default query or custom query passed as a parameter
    query = request.args.get('query', "SELECT * FROM users LIMIT 10")

    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        cursor.execute(query)

        # Fetch results
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
