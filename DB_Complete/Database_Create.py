"""
Creates a database management system that takes in SQL commands and has transaction locking support.
Four Main classes: Connection, Database, Tables, and Utility Functions

- Connection: Manages database connections and provides methods to execute SQL-like commands. It handles tokenizing the SQL statements and forwarding them to the appropriate functions.
- Database: Represents the database, holding tables and their data. It also manages lock states and provides methods to modify the database structure and content.
- Table: Represents a table with column definitions and rows. It contains methods to insert, update, delete, and retrieve rows.
- Utility_Functions: provides methods to tokenize SQL queries into manageable parts, identifying commands, operators, and values for further processing.


Supports three lock types:
- Shared Lock: Allows multiple reads but no writes.
- Reserved Lock: Ensures no new transactions start but allows completing ongoing ones.
- Exclusive Lock: Prevents all other operations on the database
"""
import string
import re
import operator
from operator import itemgetter
import copy
from copy import deepcopy
import csv
import os
from datetime import datetime
import json

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
        while query:
            # print("Query:{}".format(query))
            # print("Tokens: ", tokens)
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
        print("ORIGINAL QUERY INPUT",query)
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
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database
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
                self.database.if_exists(table_name, column_name_type_pairs)

            # If IF NOT EXISTS clause not included, raise exception for table that exists already.
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
                self.database.create_new_table(table_name, column_name_type_pairs)

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
                self.database.insert_into(table_name, row_contents)
                
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
            Retrieves rows from a table, with optional DISTINCT, filtering (WHERE), and ordering (ORDER BY).
            """
            Utility_Functions.pop_and_check(tokens, "SELECT")

            # Check for DISTINCT
            distinct = False
            if tokens[0] == "DISTINCT":
                Utility_Functions.pop_and_check(tokens, "DISTINCT")
                distinct = True

            output_columns = []

            # Parse output columns
            while True:
                col = tokens.pop(0)  # e.g., student.name or student.*
                if "." in col:
                    parts = col.split('.')
                    output_columns.append(parts[1])
                else:
                    output_columns.append(col)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','

            table_name = tokens.pop(0)

            # Initialize variables for optional clauses
            where_clause_present = False
            where_col, operator, where_value = None, None, None
            order_by_columns = []

            # Check for optional WHERE clause
            if tokens and tokens[0] == "WHERE":
                where_clause_present = True
                Utility_Functions.pop_and_check(tokens, "WHERE")
                where_col = tokens.pop(0)
                operator = tokens.pop(0)
                if tokens[0] == "=":
                    Utility_Functions.pop_and_check(tokens, "=")
                    operator = "!="
                where_value = tokens.pop(0)

            # Check for optional ORDER BY clause
            if tokens and tokens[0] == "ORDER":
                Utility_Functions.pop_and_check(tokens, "ORDER")
                Utility_Functions.pop_and_check(tokens, "BY")
                while tokens:
                    col = tokens.pop(0)
                    if "." in col:
                        parts = col.split(".")
                        order_by_columns.append(parts[1])
                    else:
                        order_by_columns.append(col)
                    if tokens and tokens[0] == ",":
                        Utility_Functions.pop_and_check(tokens, ",")
                    else:
                        break

            # Call appropriate database method based on the presence of WHERE clause
            if where_clause_present:
                print("SELECT WHERE CLAUSE PRESENT:",output_columns, table_name, order_by_columns, where_col, where_value, operator, distinct)
                return self.database.select_where(
                    output_columns, table_name, order_by_columns, where_col, where_value, operator, distinct
                )
            else:
                print("SELECT NO WHERE CLAUSE:",output_columns, table_name, order_by_columns, distinct)
                return self.database.select(output_columns, table_name, order_by_columns, distinct)


        """
        THIS IS WHERE WE TOKENIZE AND PULL THE INITIAL COMMAND: CREATE, SELECT, ETC....
        THEN WE CHOOSE WHICH LOCK AND CONNECTION FUNC TO SEND IT TO!
        """


        tokens = Utility_Functions.tokenize(statement)
        if isinstance(tokens[0],int):
            print("This is a connection based statement. What # Connection?",tokens[0])
            tokens.pop(0)
            Utility_Functions.pop_and_check(tokens=tokens,same_as=":") # If it starts with an integer based connection, after int, always semicolon.

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
                    self.database.reserved_lock()
                insert(tokens)
                return []
            elif tokens[0] == "UPDATE":
                if not self.auto_commit:
                    self.database.reserved_lock()
                update(tokens)
                return []
            elif tokens[0] == "DELETE":
                if not self.auto_commit:
                    self.database.reserved_lock()
                delete(tokens)
            elif tokens[0] == "DROP":
                if not self.auto_commit:
                    self.database.exclusive_lock()
                drop(tokens)
            else:  # tokens[0] == "SELECT"
                # print("SHOULD BE SELECT:",tokens[0])
                if self.database.r_lock:
                    self.database.unlock_reserved()
                if self.database.e_lock:
                    self.database.unlock_exclusive()
                if not self.auto_commit:
                    self.database.shared_lock()
                return select(tokens)
            

    def close(self):
        print("CLOSE ALL TEST", _ALL_DATABASES.keys())
        # write the database contents into disk by filename, used json here.
        temp_dict = {}
        with open(self.database.filename, "w") as file:
            for tb in self.database.tables:
                temp_tb = self.database.tables[tb]
                # extract rows, use json dump
                temp_dict[tb] = temp_tb.rows
            file.write(json.dumps(temp_dict))
            return

class Database(Utility_Functions):
    """
    Represents the database itself, containing tables and methods to perform various operations.
    
    Attributes:
    filename: The name of the database file.
    tables: A dictionary where table names map to Table objects.
    s_lock, r_lock, e_lock: Class-level attributes for managing shared, reserved, and exclusive locks.
    """
    #  Class-level variables for shared, reserved, and exclusive locks, used in transaction management.
    s_lock = False
    r_lock = False
    e_lock = False

    def __init__(self, filename):
        """
        Initializes a database object with a given filename and an empty table dictionary.
        """
        self.filename = filename
        self.tables = {}

    """
    Manage locking mechanisms to enforce transaction isolation and concurrency control.
    - Acquire different types of locks to enforce transaction isolation.
    - shared_lock(self), reserved_lock(self), exclusive_lock(self):
    - unlock_shared(self), unlock_reserved(self), unlock_exclusive(self):
    
    """
    def shared_lock(self):
        if self.r_lock or self.e_lock:
            raise Exception("Cannot acquire shared lock when reserved or exclusive lock is held.")
        self.s_lock = True

    def reserved_lock(self):
        if self.e_lock:
            raise Exception("Cannot acquire reserved lock when exclusive lock is held.")
        self.r_lock = True

    def exclusive_lock(self):
        if self.r_lock or self.e_lock:
            raise Exception("Cannot acquire exclusive lock when reserved or exclusive lock is held.")
        self.e_lock = True
    
    """"
    Release the respective locks.
    """

    def unlock_shared(self):
        self.s_lock = False

    def unlock_reserved(self):
        self.r_lock = False

    def unlock_exclusive(self):
        self.e_lock = False

    def print_lock_status(self):
        """
        Code for debugging the locking status upon commit.
        """
        print(f"Shared lock: {self.s_lock}")
        print(f"Reserved lock: {self.r_lock}")
        print(f"Exclusive lock: {self.e_lock}")

    # Cannot begin transaction with an open transaction.
    def check_zero_locks(self):
        if self.s_lock or self.r_lock or self.e_lock:
            raise Exception("Cannot begin transaction with an open transaction.")
        
    """
    Start of table management functions:
    - create_new_table(self, table_name, column_name_type_pairs):
    - if_exists(self, table_name, column_name_type_pairs):
    - drop(self, table_name):
    """

    def create_new_table(self, table_name, column_name_type_pairs):
        """
        Creates a new table in the database.
        Is called in the create_table function of the execute func in the connection class after CREATE TABLE statement tokenized/submitted.
        """
        if table_name in self.tables:
            raise Exception("Table Already In Database")
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        print(f"Create New Table Test - Tables in Database {self.filename}: {self.tables.keys()}")
        return []

    def if_exists(self, table_name, column_name_type_pairs):
        """
        Is called in the create_table function of the execute func in the connection class.
        For IF NOT EXISTS statements in Create/Drop functions of the execute 
        Ensures the table exists or creates it if it does not.
        """
        if table_name in self.tables:
            return
        else:
            self.tables[table_name] = Table(table_name, column_name_type_pairs)
    
    """
    Start of row management and query execution.
    - del_all_rows(self, table_name):
    - del_where(self, table_name, col_name, operator, constant):
    - view, insert_into, select, select_where, update
    """

    def del_all_rows(self, table_name):
        """
        Deletes all rows from the specified table. 
        Called in delete func of execute func of connect class.
        """
        self.tables[table_name].rows.clear()
        return

    def drop(self, table_name):
        """
        Removes a table from the database.
        """
        if self.tables[table_name] is not None:
            del self.tables[table_name]
        return

    def del_where(self, table_name, col_name, operator, constant):
        """
        Deletes rows matching a condition in the WHERE clause.
        Called in the delete func of the connection class - execute func.
        Specific to:
        - OPERATORS: >, <, =, !=, IS, IS NOT
        """
        # Loop through the list of all rows,
        for dict_ in self.tables[table_name].rows:
            # Each row is a dictionary, loop through the key/values.
            for key, value in dict_.items():
                # If the column specified by where exists,
                if key == col_name:
                    # Change
                    if value is not None:
                        # Check for operator instance:
                        if operator == ">":
                            if value > constant:
                                self.tables[table_name].rows.remove(dict_)
                        elif operator == "<":
                            if value < constant:
                                self.tables[table_name].rows.remove(dict_)
                        elif operator == "=":
                            if value == constant:
                                self.tables[table_name].rows.remove(dict_)
                        elif operator == "!=":
                            if value != constant:
                                self.tables[table_name].rows.remove(dict_)
                        else:
                            print("ISSUE HERE!")

        # Loop through the list of all rows,
        for dict_ in self.tables[table_name].rows:
            # Each row is a dictionary, loop through the key/values.
            for key, value in dict_.items():
                # If the column specified by where exists,
                if key == col_name:
                    # Change
                    if value is not None:
                        # Check for operator instance:
                        if operator == ">":
                            if value > constant:
                                self.tables[table_name].rows.remove(dict_)
                        elif operator == "<":
                            if value < constant:
                                self.tables[table_name].rows.remove(dict_)
                        elif operator == "=":
                            if value == constant:
                                self.tables[table_name].rows.remove(dict_)
                        elif operator == "!=":
                            if value != constant:
                                self.tables[table_name].rows.remove(dict_)
                        else:
                            print("ISSUE HERE!")


    def insert_into(self, table_name, row_contents):
        """
        Inserts a new row into the specified table.
        """
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents)
        return []

    def select(self, output_columns, table_name, order_by_columns, distinct=False):
        """
        Selects and orders rows based on specified columns, optionally removing duplicates.
        """
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns, distinct)

    def select_where(self, output_columns, table_name, order_by_columns, where_col, where_value, operator, distinct=False):
        """
        Selects rows that meet a condition and optionally orders them, with an option for DISTINCT.
        """
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_where_rows(output_columns, order_by_columns, where_col, where_value, operator, distinct)

    def update(self, table_name, columns, values, where_column=None, where_vals=None):
        """
        Updates rows in a table based on specified conditions.
        """
        assert table_name in self.tables
        table = self.tables[table_name]
        # Check table class for update func.
        return table.update_table(columns, values, where_column, where_vals)
    
    def view(self, table_name):
        """
        Retrieves and displays all rows from the specified table.
        """
        # print("TABLE NAME CHECK:",self.tables[table_name].name)
        # print("TABLE ROWS CHECK",self.tables[table_name].rows)
        # print("COLUMNS:", self.tables[table_name].column_names)
        if len(self.tables) != 0:
            return self.tables[table_name].rows
        else:
            print("ENTIRE DATABASE:", self.tables)
 

class Table(Utility_Functions):
    """
    Represents an individual table in the database, containing rows and column definitions.
    
    Attributes:
    name: The name of the table.
    column_names: A list of column names in the table.
    column_types: A list of column data types corresponding to the columns.
    rows: A list of rows (each row is a dictionary mapping column names to values).
    """
    def __init__(self, name, column_name_type_pairs):
        """"
        Initializes a table with a name, columns, and an empty row list.
        """
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents):
        """
        Adds a new row to the table.
        """
        # print("COLUMN NAMES in insert_new_row:",self.column_names)
        # print("ROW CONTENTS in insert_new_row:",row_contents)

        assert len(self.column_names) == len(row_contents)
        row = dict(zip(self.column_names, row_contents))
        self.rows.append(row)
        # print(f"Inserted row into {self.name}: {row_contents}")
        print(f"Current rows: {self.rows}")


    def update_table(self, columns, values, where_column, where_vals):
        """
        Updates rows in the table matching conditions.
        """
        # # ['UPDATE', 'student', 'SET', 'grade', '=', 3.0, 'WHERE', 'piazza', '=', 2, ';']

        # If update has no where clause,
        if where_column is None:
            # Each row is a dictionary
            for row in self.rows:
                for column in range(len(columns)):
                    # Columns is a list, so is values, so Cols[columns] is new key,
                    # Values[cols] is value.
                    row[columns[column]] = values[column]
        # If there is a where clause:
        else:
            # Pull columns to change:
            first_col = columns.pop(0)

            for row_dict in self.rows:
                for key, value in row_dict.items():
                    if key == where_column:
                        if value == where_vals:
                            row_dict[first_col] = values[0]
                            # If more than one column:
                            if len(columns) > 0:
                                second_col = columns.pop(0)
                                row_dict[second_col] = values[1]

    def select_rows(self, output_columns, order_by_columns, distinct=False):
        """
        Retrieve rows, with optional filtering, ordering, and DISTINCT.
        """
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns):
            return sorted(self.rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            seen = set()
            for row in rows:
                result = tuple(row[col] for col in output_columns)
                if distinct:
                    if result in seen:
                        continue
                    seen.add(result)
                yield result

        expanded_output_columns = expand_star_column(output_columns)
        check_columns_exist(expanded_output_columns)
        check_columns_exist(order_by_columns)
        sorted_rows = sort_rows(order_by_columns)
        print("TEST SELECT NO WHERE CLAUSE ROWS OUTPUT:",sorted_rows, expanded_output_columns)
        return generate_tuples(sorted_rows, expanded_output_columns)

    def select_where_rows(self, output_columns, order_by_columns, where_col, where_val, operator, distinct=False):
        """
        Retrieve rows, with optional filtering, ordering, and DISTINCT.
        """
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def generate_tuples(rows, output_columns):
            seen = set()
            for row in rows:
                result = tuple(row[col] for col in output_columns)
                if distinct:
                    if result in seen:
                        continue
                    seen.add(result)
                yield result

        expanded_output_columns = expand_star_column(output_columns)
        where_sort = []

        for dict_ in self.rows:
            for key, value in dict_.items():
                if key == where_col:
                    if value is not None:
                        if operator == ">" and value > where_val:
                            where_sort.append(dict_)
                        elif operator == "<" and value < where_val:
                            where_sort.append(dict_)
                        elif operator == "=" and value == where_val:
                            where_sort.append(dict_)
                        elif operator == "!=" and value != where_val:
                            where_sort.append(dict_)

        sorted_rows = sorted(where_sort, key=itemgetter(*order_by_columns))
        print("TEST SELECT WHERE CLAUSE ROWS OUTPUT:",sorted_rows, expanded_output_columns)
        return generate_tuples(sorted_rows, expanded_output_columns)

"""
Testing the database management system.
"""
def main():
    print("Welcome to the Lightweight DBMS Tester!")
    print("You can test SQL commands interactively or by running an SQL file.")
    print("Type 'exit' to quit the interactive mode.")
    print("CURRENT DATABASES:", _ALL_DATABASES)

    # Prompt for database filename
    db_filename = input("Enter the name of the database file (it will be created if it doesn't exist): ")
    connection = connect(db_filename)

    try:
        while True:
            # Choose between interactive mode and file submission
            mode = input("\nEnter '1' to type SQL commands, '2' to run commands from a file, or 'exit' to quit: ")

            if mode == '1':  # Interactive mode
                while True:
                    statement = input("Enter an SQL statement (or type 'exit' to quit): ")
                    if statement.lower() == 'exit':
                        break
                    try:
                        result = connection.execute(statement)
                        if result:
                            print("Result:", list(result))
                    except Exception as e:
                        print(f"Error: {e}")

            elif mode == '2':  # File submission mode
                file_path = input("Enter the path to the SQL file: ")
                try:
                    with open(file_path, 'r') as sql_file:
                        commands = sql_file.read().split(';')  # Split commands by semicolons
                        for command in commands:
                            command = command.strip()
                            if command:  # Skip empty commands
                                print(f"Executing: {command}")
                                result = connection.execute(command + ';')  # Add back semicolon
                                if result:
                                    print("Result:", list(result))
                except FileNotFoundError:
                    print("Error: File not found.")
                except Exception as e:
                    print(f"Error: {e}")

            elif mode.lower() == 'exit':  # Exit the program
                break

            else:
                print("Invalid choice. Please enter '1', '2', or 'exit'.")
    finally:
        # Ensure the close function is called when the program exits
        print("Closing the database connection...")
        try:
            connection.close()  # Close the connection and handle cleanup
            print(f"Database successfully closed: {db_filename}")
        except Exception as e:
            print(f"Error during close operation: {e}")

if __name__ == "__main__":
    main()
