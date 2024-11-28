from Test_DB_System.py import connect

def main():
    # Specify the SQL file containing commands
    sql_file = "test.sql"

    # Connect to the in-memory database
    conn = connect(":memory:")

    try:
        # Read the SQL commands from the file
        with open(sql_file, "r") as file:
            sql_commands = file.read()

        # Split the commands by semicolon and strip whitespace
        commands = [cmd.strip() for cmd in sql_commands.split(";") if cmd.strip()]

        # Execute each command
        for command in commands:
            print(f"Executing: {command}")
            try:
                result = conn.execute(command + ";")
                if result:
                    print(f"Result: {list(result)}\n")
                else:
                    print("Command executed successfully.\n")
            except Exception as e:
                print(f"Error executing command: {e}\n")

    finally:
        # Close the connection
        conn.close()

if __name__ == "__main__":
    main()
