
import string
import re
import operator
from operator import itemgetter
import copy
from copy import deepcopy
import csv
import os
from datetime import datetime


class Utility_Functions(object):
    def __init__(self):
        """
        Functions that will be used for tokenizing SQL statements - pulling words and values.
        """

    @staticmethod
    def pop_and_check(tokens, same_as):
        """
        Removes and verifies that the next token matches the expected value.
        """
        item = tokens.pop(0)
        assert item == same_as, "{} != {}".format(item, same_as)

    @staticmethod
    def collect_characters(query, allowed_characters):
        """
        Extracts a substring containing only allowed characters.
        """
        letters = []
        for letter in query:
            if letter not in allowed_characters:
                break
            letters.append(letter)
        return "".join(letters)

    @staticmethod
    def remove_leading_whitespace(query, tokens):
        """
        Strips leading whitespace from the query.
        """
        whitespace = Utility_Functions.collect_characters(query, string.whitespace)
        return query[len(whitespace):]

    @staticmethod
    def remove_word(query, tokens):
        """
        Extracts a word (e.g., table name or column name) from the query.
        """
        word = Utility_Functions.collect_characters(query,
                                string.ascii_letters + "_" + string.digits + "." + "*")
        if word == "NULL":
            tokens.append(None)
        elif "." in word:
            tokens.append(word)
        elif "*" in word:
            tokens.append(word)
        else:
            tokens.append(word.split(".")[0])
        return query[len(word):]

    @staticmethod
    def remove_text(query, tokens):
        """
        Extracts text enclosed in single quotes.
        """
        assert query[0] == "'"
        query = query[1:]
        end_quote_index = query.find("'")
        text = query[:end_quote_index]
        tokens.append(text)
        query = query[end_quote_index + 1:]
        return query

    @staticmethod
    def remove_integer(query, tokens):
        """
        Extracts an integer value from the query.
        """
        int_str = Utility_Functions.collect_characters(query, string.digits)
        tokens.append(int_str)
        return query[len(int_str):]

    @staticmethod
    def remove_number(query, tokens):
        """
        Extracts a numeric value (integer or float) from the query.
        """
        query = Utility_Functions.remove_integer(query, tokens)
        if query[0] == ".":
            whole_str = tokens.pop()
            query = query[1:]
            query = Utility_Functions.remove_integer(query, tokens)
            frac_str = tokens.pop()
            float_str = whole_str + "." + frac_str
            tokens.append(float(float_str))
        else:
            int_str = tokens.pop()
            tokens.append(int(int_str))
        return query

    @staticmethod
    def tokenize(query):
        """
        Splits the SQL query into individual tokens for parsing and execution.
        Calls the utility functions.
        """
        tokens = []
        print("ORIGINAL QUERY INPUT",query)
        while query:
            # print("Query:{}".format(query)) # Should decrease until query is empty.
            # print("Tokens: ", tokens) # SHould increase until query is empty.
            # Example: CREATE TABLE student (name TEXT, grade REAL, id INTEGER);
            # Tokens:  ['CREATE', 'TABLE', 'student', '(', 'name', 'TEXT', ',', 'grade', 'REAL', ',', 'id', 'INTEGER', ')']
            old_query = query

            if query[0] in string.whitespace:
                query = Utility_Functions.remove_leading_whitespace(query, tokens)
                continue

            if query[0] in (string.ascii_letters + "_"):
                query = Utility_Functions.remove_word(query, tokens)
                continue

            if query[0] in "(),;*:":
                tokens.append(query[0])
                query = query[1:]
                continue

            # FOR DELETE OPERATOR
            if query[0] == ">" or query[0] == "<":
                tokens.append(query[0])
                query = query[1:]
                continue

            if query[0] == "=" or query[0] == "!":
                tokens.append(query[0])
                query = query[1:]
                continue

            # NEED TO COVER: >, <, =, !=, IS NOT, IS.

            if query[0] == "'":
                query = Utility_Functions.remove_text(query, tokens)
                continue

            if query[0] in (string.ascii_letters + "."):
                tokens.append(query[0])
                print(query[0])

            if query[0] in string.digits:
                query = Utility_Functions.remove_number(query, tokens)
                continue

            if len(query) == len(old_query):
                print(query[0])
                raise AssertionError("Query didn't get shorter.")
        print("FINAL TOKENS: ", tokens)
        return tokens
    

_ALL_DATABASES = {} # A dictionary that tracks all database instances by their filenames.

def connect(filename, timeout=None, isolation_level=None):
    """
    Creates a Connection instance for the specified database file.
    """
    return Connection(filename)

class Connection(Utility_Functions):
    """
    Represents a connection to a database, allowing SQL statements to be executed and transactions to be managed.
    
    Attributes:
    counter: A class-level counter that uniquely identifies each connection.
    database: The Database object associated with the connection.
    id: A unique identifier for the connection, assigned incrementally using the counter.
    copy: A deep copy of the database, used for transaction management during BEGIN statements.
    auto_commit: A flag indicating whether changes are committed automatically (default is True).

    """
    counter = 0 # A class-level counter to uniquely identify each connection.

    def __init__(self, filename):
        """
        Initializes a connection, associating it with a database. Creates a new database if the filename is not already in _ALL_DATABASES.
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
            print(f"Registered databases: {_ALL_DATABASES.keys()}")
        else:
            _ALL_DATABASES[filename] = filename
            print(f"Registered databases: {_ALL_DATABASES.keys()}")

        # need a way for each connection to know it's identity.
        Connection.counter += 1
        self.id = Connection.counter
        print("Connection ID Check",self.id)
        self.copy = None  # For Transactions

        # For no begin statement/ no transactions statements.
        self.auto_commit = True

    def execute(self, statement):
        """
        Takes a SQL statement, processe/tokenizes it, submits commands to the database class.
        Supports commands like CREATE, INSERT, SELECT, DELETE, UPDATE, DROP, and transaction management (BEGIN, COMMIT, ROLLBACK).
        """

        def create_table(tokens):
            """
            - Handles the CREATE TABLE SQL statement.
            - Parses table names and columns, and either creates a new table or ensures it exists if IF NOT EXISTS is specified.
            """
            Utility_Functions.pop_and_check(tokens, "CREATE")
            Utility_Functions.pop_and_check(tokens, "TABLE")

            # IF NOT EXISTS: (Do not raise an exception if it exists, just do nothing.)
            if tokens[0] == "IF":
                Utility_Functions.pop_and_check(tokens, "IF")
                Utility_Functions.pop_and_check(tokens, "NOT")
                Utility_Functions.pop_and_check(tokens, "EXISTS")
                table_name = tokens.pop(0)
                Utility_Functions.pop_and_check(tokens, "(")
                column_name_type_pairs = []
                while True:
                    column_name = tokens.pop(0)
                    column_type = tokens.pop(0)
                    assert column_type in {"TEXT", "INTEGER", "REAL"}
                    column_name_type_pairs.append((column_name, column_type))
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
                print("THIS IS WHERE WE WOULD RUN CREATE NEW TABLE, HERE IS TABLE NAME",table_name)
                # self.database.if_exists(table_name, column_name_type_pairs)

            # IF NOT EXISTS clause not included, raise exception for table that exists already.
            else:
                table_name = tokens.pop(0)
                Utility_Functions.pop_and_check(tokens, "(")
                column_name_type_pairs = []
                while True:
                    column_name = tokens.pop(0)
                    column_type = tokens.pop(0)
                    assert column_type in {"TEXT", "INTEGER", "REAL"}
                    column_name_type_pairs.append((column_name, column_type))
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
                print("THIS IS WHERE WE WOULD RUN CREATE NEW TABLE, HERE IS TABLE NAME",table_name)
                # self.database.create_new_table(table_name, column_name_type_pairs)

        def drop(tokens):
            """
            Handles the DROP TABLE SQL statement.
            Deletes a table if it exists or raises an error if it does not.
            """
            # DROP TABLE IF EXISTS students;
            Utility_Functions.pop_and_check(tokens, "DROP")
            Utility_Functions.pop_and_check(tokens, "TABLE")
            if tokens[0] == "IF":
                Utility_Functions.pop_and_check(tokens, "IF")
                Utility_Functions.pop_and_check(tokens, "EXISTS")
                table_name = tokens.pop(0)
            # No if exists statement.
            else:
                table_name = tokens.pop(0)
            self.database.drop(table_name)

        def insert(tokens):
            """
            Handles the INSERT INTO SQL statement.
            Adds rows to a specified table, optionally specifying columns to insert into.
            """
            Utility_Functions.pop_and_check(tokens, "INSERT")
            Utility_Functions.pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            insert_to_cols = []

            # Checking if insert statement has column names before values:
            if tokens[0] == "(":
                Utility_Functions.pop_and_check(tokens, "(")
                while True:
                    if tokens[0] == ",":
                        Utility_Functions.pop_and_check(tokens, ",")
                    elif tokens[0] == ")":
                        Utility_Functions.pop_and_check(tokens, ")")
                        break
                    else:
                        insert_to_cols.append(tokens.pop(0))

            # Gathering and inserting all rows:
            Utility_Functions.pop_and_check(tokens, "VALUES")
            
            while tokens:
                Utility_Functions.pop_and_check(tokens, "(")
                row_contents = []
                while True:
                    item = tokens.pop(0)
                    row_contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ","
                # Insert the row into the database
                print("THIS IS WHERE WE WOULD RUN INSERT INTO - TABLE NAME, ROW CONTENTS",table_name,row_contents)
                # self.database.insert_into(table_name, row_contents)
                
                # Check if there are more rows to insert
                if tokens and tokens[0] == ",":
                    Utility_Functions.pop_and_check(tokens, ",")
                else:
                    break

        def update(tokens):
            """
            Handles the UPDATE SQL statement.
            Modifies table rows based on conditions provided in a WHERE clause.
            """
            Utility_Functions.pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            Utility_Functions.pop_and_check(tokens, "SET")
            columns = []
            values = []
            while True:
                # Column to update:
                column_name = tokens.pop(0)
                # List of the columns to update.
                columns.append(column_name)
                # Operator, what are we setting the col to?
                Utility_Functions.pop_and_check(tokens, "=")
                val = tokens.pop(0)
                values.append(val)
                if len(tokens) == 0:
                    # Then the statement has no WHERE CLAUSE! Call update.
                    # def update(self, table_name, columns, values, where_column=None, where_vals=None):
                    self.database.update(table_name, columns, values)
                    break

                # If there are more tokens, meaning a WHERE CLAUSE,

                elif len(tokens) > 0:
                    where = tokens.pop(0)
                    # CHECK WHERE CLAUSE
                    assert where == "WHERE"
                    if where == "WHERE":
                        where_column = tokens.pop(0)
                        Utility_Functions.pop_and_check(tokens, "=")  # Operator hopefully is always =
                        where_vals = tokens.pop(0)
                        # Go to the database function for the update:
                        self.database.update(table_name, columns, values, where_column, where_vals)
                        break

        def delete(tokens):
            """
            Handles the DELETE SQL statement.
            Deletes rows from a table, either all rows or specific rows based on a WHERE clause.
            """
            #  ['DELETE', 'FROM', 'students', 'WHERE', 'id', '>', 4, ';']
            Utility_Functions.pop_and_check(tokens, "DELETE")
            Utility_Functions.pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            # IF NO WHERE CLAUSE
            if not tokens:
                self.database.del_all_rows(table_name)
            # IF IT CONTAINS WHERE CLAUSE:
            else:
                # WHERE column_name operator value.
                Utility_Functions.pop_and_check(tokens, "WHERE")
                # WHAT IS BEING DELETED:
                col_name = tokens.pop(0)
                operator = tokens.pop(0)
                constant = tokens.pop(0)
                # del_where(self,table_name,col_name,operator,constant):
                self.database.del_where(table_name, col_name=col_name, operator=operator, constant=constant)
            return

        def select(tokens):
            """
            Handles the SELECT SQL statement.
            Retrieves rows from a table, with optional filtering (WHERE) and ordering (ORDER BY).
            """
            Utility_Functions.pop_and_check(tokens, "SELECT")
            output_columns = []
            while True:
                col = tokens.pop(0)  # student.name or student.*
                # SELECT student.name FROM student ORDER BY student.piazza, grade;
                if "." in col:
                    # This is if the table has .something selected, we need after the dot in the token.
                    parts = col.split('.')
                    output_columns.append(parts[1])
                else:
                    output_columns.append(col)
                # print("OUTPUT COLUMNS SELECT:",output_columns)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','

            table_name = tokens.pop(0)

            # AFTER TABLE NAME, THERE MAY BE A WHERE CLAUSE!
            # EXAMPLE: SELECT * FROM student WHERE grade > 3.5 ORDER BY student.piazza, grade;

            if tokens[0] == "WHERE":
                order_by_columns = []
                # Remove WHERE word,
                Utility_Functions.pop_and_check(tokens, "WHERE")
                # Pull column, operator, and constant
                where_col = tokens.pop(0)

                # We're not doing the null checks for project 4! Pull operator >, <, =, !=
                operator = tokens.pop(0)
                # If tokens has ! and then =,
                if tokens[0] == "=":
                    Utility_Functions.pop_and_check(tokens, "=")
                    operator = "!="
                    # print(operator)
                where_value = tokens.pop(0)
                # Remove order and by:
                Utility_Functions.pop_and_check(tokens, "ORDER")
                Utility_Functions.pop_and_check(tokens, "BY")
                # All order by columns work normally!
                while True:
                    col = tokens.pop(0)
                    # IF STUDENT.PIAZZA type of col
                    if "." in col:
                        parts = col.split(".")
                        order_by_columns.append(parts[1])
                        if tokens[0] == ",":
                            Utility_Functions.pop_and_check(tokens, ",")
                    else:
                        order_by_columns.append(col)
                    if not tokens:
                        break

                return self.database.select_where(output_columns, table_name, order_by_columns, where_col,
                                                  where_value, operator)

            # If no where clause, business as usual:
            else:

                Utility_Functions.pop_and_check(tokens, "ORDER")
                Utility_Functions.pop_and_check(tokens, "BY")
                order_by_columns = []
                while True:
                    col = tokens.pop(0)
                    if "." in col:
                        parts = col.split(".")
                        order_by_columns.append(parts[1])
                    else:
                        order_by_columns.append(col)
                    # print("ORDER COLUMNS SELECT:",order_by_columns)
                    if not tokens:
                        break
                    Utility_Functions.pop_and_check(tokens, ",")
                print("SELECT FUNCTION IN DATABASE CLASS RUNS HERE - TABLE NAME, OUTPUT COLUMNS, ORDER BY COLUMNS",
                      table_name, output_columns, order_by_columns )
                """
                return self.database.select(
                    output_columns, table_name, order_by_columns)
                """

        tokens = Utility_Functions.tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE", "DROP", "BEGIN", "COMMIT", "ROLLBACK"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        # Check for transaction statements
        if tokens[0] == "BEGIN":
            Utility_Functions.pop_and_check(tokens, "BEGIN")
            self.auto_commit = False  # disable autocommit mode after BEGIN statement!!!
            # Check for transaction mode:
            if tokens[0] == "DEFERRED":
                #self.database.print_lock_status()
                # If not in auto mode, aquire lock:
                if not self.auto_commit:
                    self.database.shared_lock()
            elif tokens[0] == "IMMEDIATE":
                #self.database.print_lock_status() debugging
                # A reserved lock is acquired at the start of the transaction and
                # an exclusive lock is acquired when needed
                if not self.auto_commit:
                    self.database.reserved_lock()
            elif tokens[0] == "EXCLUSIVE":
                #self.database.print_lock_status()
                if not self.auto_commit:
                    self.database.exclusive_lock()
            else:
                Utility_Functions.pop_and_check(tokens, "TRANSACTION")
                if not self.auto_commit:
                    self.database.shared_lock()
                # Work on a copy of the database,
                # if a transaction is started
                self.copy = deepcopy(self.database)

        # Rollbacks
        elif tokens[0] == "ROLLBACK":
            # if there is a copy from the begin transation statement,
            if self.copy is not None:
                self.database.tables = self.copy.tables
                self.copy = None  # Delete the copy/transaction so all events rolled back.
                # Release all locks.
                self.database.unlock_shared()
                self.database.unlock_reserved()
                self.database.unlock_exclusive()
                self.auto_commit = True  # re-enable autocommit mode after ROLLBACK
                return

                # Commit the copy we made with the transactions for the connection onbject to the original DB
        elif tokens[0] == "COMMIT":
            # Now release all locks:
            self.database.unlock_shared()
            self.database.unlock_reserved()
            self.database.unlock_exclusive()
            if self.copy is not None:
                # Copy transactions to the new DB
                self.database = self.copy
                # toss the old copy
                self.copy = None
                self.auto_commit = True  # reanble after a commit!
                return

        # execute the query on the database
        else:
            if tokens[0] == "CREATE":
                create_table(tokens)
                return []
            elif tokens[0] == "INSERT":
                if not self.auto_commit:
                    print("INSERT LOCK WOULD BE HERE")
                    # self.database.reserved_lock()
                insert(tokens)
                return []
            elif tokens[0] == "UPDATE":
                if not self.auto_commit:
                    print("UPDATE LOCK WOULD BE HERE")
                    #self.database.reserved_lock()
                update(tokens)
                return []
            elif tokens[0] == "DELETE":
                if not self.auto_commit:
                    print("DELETE LOCK WOULD BE HERE")
                    # self.database.reserved_lock()
                delete(tokens)
            elif tokens[0] == "DROP":
                if not self.auto_commit:
                    print("DROP LOCK WOULD BE HERE")
                    # self.database.exclusive_lock()
                drop(tokens)
            else:  # tokens[0] == "SELECT"
                print("SELECT LOCK WOULD BE HERE")

                """
                if self.database.r_lock:
                    self.database.unlock_reserved()
                if self.database.e_lock:
                    self.database.unlock_exclusive()
                if not self.auto_commit:
                    self.database.shared_lock()
                """
                return select(tokens)

    def export_to_file(self, table_names, format="csv"):
        """
        Exporting the databases selected tables to a csv file.
        """

        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"exported_tables_{timestamp}.csv"
        file_path = os.path.join(desktop, file_name)

        try:
            with open(file_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                for table_name in table_names:
                    if table_name in self.database.tables:
                        table = self.database.tables[table_name]
                        print(f"Exporting table: {table_name}")  # Debugging
                        print(f"Columns: {table.column_names}")  # Debugging
                        print(f"Rows: {table.rows}")  # Debugging

                        writer.writerow([f"Table: {table_name}"])
                        writer.writerow(table.column_names)
                        for row in table.rows:
                            writer.writerow([row[col] for col in table.column_names])
                        writer.writerow([])  # Blank line between tables
                    else:
                        print(f"Table '{table_name}' does not exist in the database.")
            print(f"Tables {table_names} exported successfully to: {file_path}")
        except Exception as e:
            print(f"Error exporting tables: {e}")


    def close(self):
        """
        Placeholder method for closing a connection.
        """
        pass

Connection = connect("test_database")
query = "CREATE TABLE student (name TEXT, grade REAL, id INTEGER);"

# Test File:
file_path = input("Enter the path to the SQL file: ")
with open(file_path, 'r') as sql_file:
    commands = sql_file.read().split(';')  # Split commands by semicolons
    for command in commands:
        command = command.strip()
        if command:  # Skip empty commands
            print(f"Executing: {command}")
            result = Connection.execute(command + ';')  # Add back semicolon
            if result:
                print("Result:", list(result))
