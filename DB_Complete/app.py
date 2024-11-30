from flask import Flask, render_template, request, jsonify
from Database_Create import connect  # Import your DBMS implementation
import os
print("Templates folder:", os.path.join(os.path.dirname(__file__), "Database_Create/DB_Complete/templates"))


app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)

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
    Executes an SQL query or multiple SQL statements submitted by the user.
    """
    query = request.form.get('query')

    try:
        # Split the input query into individual SQL statements
        statements = query.split(';')  # Split by semicolon
        results = []

        for statement in statements:
            statement = statement.strip()  # Remove extra whitespace
            if statement:  # Skip empty statements
                result = connection.execute(statement + ';')  # Add back the semicolon
                if result:  # Append the result if any
                    results.append(list(result))

        return jsonify({'success': True, 'result': results if results else "All statements executed successfully. Run SELECT to view contents."})

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
