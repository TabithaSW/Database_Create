from flask import Flask, render_template, request, jsonify
from your_dbms_file import connect  # Import your DBMS implementation

app = Flask(__name__)
db_filename = "my_database.json"  # Default database file
connection = connect(db_filename)  # Connect to your DBMS


@app.route('/')
def index():
    """
    Renders the main HTML page.
    """
    return render_template('index.html')


@app.route('/execute', methods=['POST'])
def execute_query():
    """
    Executes an SQL query submitted by the user.
    """
    query = request.form.get('query')
    try:
        result = connection.execute(query)
        return jsonify({'success': True, 'result': list(result) if result else "Query executed successfully"})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/close', methods=['POST'])
def close_db():
    """
    Exports the database and closes the connection.
    """
    try:
        connection.close()
        return jsonify({'success': True, 'message': 'Database exported and connection closed successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


if __name__ == '__main__':
    app.run(debug=True)
