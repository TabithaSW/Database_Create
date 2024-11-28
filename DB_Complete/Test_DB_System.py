

import string
import re
import operator
from operator import itemgetter
import copy
from copy import deepcopy

_ALL_DATABASES = {} # A dictionary that tracks all database instances by their filenames.

class Connection(object):
    counter = 0 # A class-level counter to uniquely identify each connection.

    def __init__(self, filename):
        """
        Initializes a connection, associating it with a database. Creates a new database if the filename is not already in _ALL_DATABASES.
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

        # you need a way for each connection to know it's identity.
        Connection.counter += 1
        self.id = Connection.counter
        self.copy = None  # For Transactions

        # For no begin statement/ no transactions statements.
        self.auto_commit = True

    def execute(self, statement):
        """
        Takes a SQL statement, processe/tokenizes it, submits commands to the database class.
        Supports commands like CREATE, INSERT, SELECT, DELETE, UPDATE, DROP, and transaction management (BEGIN, COMMIT, ROLLBACK).
        """

        # NEW CREATE INFO: CREATE TABLE IF NOT EXISTS students
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
                        # If closing bracket, insert?
                        Utility_Functions.pop_and_check(tokens, ")")
                        break
                    else:
                        # List of Columns to insert to:
                        insert_to_cols.append(tokens[0])
                        tokens.pop(0)

            # Gathering vals:
            Utility_Functions.pop_and_check(tokens, "VALUES")
            Utility_Functions.pop_and_check(tokens, "(")
            row_contents = []
            while True:
                item = tokens.pop(0)
                row_contents.append(item)
                comma_or_close = tokens.pop(0)

                if comma_or_close == ")":
                    # print(row_contents)
                    break
                assert comma_or_close == ','
            # Insert first row:
            self.database.insert_into(table_name, row_contents)

            # If the tokens list is not empty after first insert:
            if tokens:
                # print(tokens)
                Utility_Functions.pop_and_check(tokens, ",")
                Utility_Functions.pop_and_check(tokens, "(")
                row_next = []
                while len(tokens) > 0:
                    items = tokens.pop(0)
                    # print("ITEMS:",items)
                    row_next.append(items)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        # print("NEXT ROW",row_next)
                        break
                    assert comma_or_close == ','
                self.database.insert_into(table_name, row_next)

            # If the tokens list is not empty after first insert:
            if tokens:
                # print(tokens)
                Utility_Functions.pop_and_check(tokens, ",")
                Utility_Functions.pop_and_check(tokens, "(")
                row_next = []
                while len(tokens) > 0:
                    items = tokens.pop(0)
                    # print("ITEMS:",items)
                    row_next.append(items)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        # print("NEXT ROW",row_next)
                        break
                    assert comma_or_close == ','
                self.database.insert_into(table_name, row_next)

            # If the tokens list is not empty after first insert:
            if tokens:
                # print(tokens)
                Utility_Functions.pop_and_check(tokens, ",")
                Utility_Functions.pop_and_check(tokens, "(")
                row_next = []
                while len(tokens) > 0:
                    items = tokens.pop(0)
                    # print("ITEMS:",items)
                    row_next.append(items)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        # print("NEXT ROW",row_next)
                        break
                    assert comma_or_close == ','
                self.database.insert_into(table_name, row_next)

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
                return self.database.select(
                    output_columns, table_name, order_by_columns)

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
                if self.database.r_lock:
                    self.database.unlock_reserved()
                if self.database.e_lock:
                    self.database.unlock_exclusive()
                if not self.auto_commit:
                    self.database.shared_lock()
                return select(tokens)

    def close(self):
        """
        Placeholder method for closing a connection.
        """
        pass


def connect(filename, timeout=None, isolation_level=None):
    """
    Creates a Connection instance for the specified database file.
    """
    return Connection(filename)


class Database:
    """
    
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

    def create_new_table(self, table_name, column_name_type_pairs):
        """
        Creates a new table in the database.
        Is called in the create_table function of the execute func in the connection class after CREATE TABLE statement tokenized/submitted.
        """
        if table_name in self.tables:
            raise Exception("Table Already In Database")
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
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

    def select(self, output_columns, table_name, order_by_columns):
        """
        Selects and orders rows based on specified columns.
        """
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns)

    def select_where(self, output_columns, table_name, order_by_columns, where_col, where_value, operator):
        """
        Selects rows that meet a condition and optionally orders them.
        """
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_where_rows(output_columns, order_by_columns, where_col, where_value, operator)

    def update(self, table_name, columns, values, where_column=None, where_vals=None):
        """
        Updates rows in a table based on specified conditions.
        """
        assert table_name in self.tables
        table = self.tables[table_name]
        # Check table class for update func.
        return table.update_table(columns, values, where_column, where_vals)


class Table:
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

    def select_where_rows(self, output_columns, order_by_columns, where_col, where_val, operator):
        """
        Retrieve rows, with optional filtering and ordering.
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
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        # Check if * (all) or specific columns:
        expanded_output_columns = expand_star_column(output_columns)

        # Where checks:
        # SELECT * FROM student WHERE grade > 3.5 ORDER BY student.piazza, grade;
        where_sort = []

        for dict_ in self.rows:
            for key, value in dict_.items():
                # If the column specified by where exists,
                if key == where_col:
                    if value is not None:
                        # Check for operator instance:
                        if operator == ">":
                            if value > where_val:
                                # Extract the rows that fit the criteria.
                                where_sort.append(dict_)
                        elif operator == "<":
                            if value < where_val:
                                where_sort.append(dict_)
                        elif operator == "=":
                            if value == where_val:
                                where_sort.append(dict_)
                        elif operator == "!=":
                            if value != where_val:
                                where_sort.append(dict_)
                        else:
                            print("ISSUE")

        # Sort the where rows, extract the tuples, return.
        sorted_rows = sorted(where_sort, key=itemgetter(*order_by_columns))
        return generate_tuples(sorted_rows, expanded_output_columns)

    def select_rows(self, output_columns, order_by_columns):
        """
        Retrieve rows, with optional filtering and ordering.
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
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        expanded_output_columns = expand_star_column(output_columns)
        check_columns_exist(expanded_output_columns)
        check_columns_exist(order_by_columns)
        sorted_rows = sort_rows(order_by_columns)
        return generate_tuples(sorted_rows, expanded_output_columns)


"""
Utility Functions For Data Processing/Tokenizing the SQL Statements
"""
class Utility_Functions:
    def __init__(self):
        """
        Functions that will be used for tokenizing SQL statements - pulling words and values.
        """


    def pop_and_check(self,tokens, same_as):
        """
        Removes and verifies that the next token matches the expected value.
        """
        item = tokens.pop(0)
        assert item == same_as, "{} != {}".format(item, same_as)


    def collect_characters(self, query, allowed_characters):
        """
        Extracts a substring containing only allowed characters.
        """
        letters = []
        for letter in query:
            if letter not in allowed_characters:
                break
            letters.append(letter)
        return "".join(letters)


    def remove_leading_whitespace(self,query, tokens):
        """
        Strips leading whitespace from the query.
        """
        whitespace = self.collect_characters(query, string.whitespace)
        return query[len(whitespace):]


    def remove_word(self,query, tokens):
        """
        Extracts a word (e.g., table name or column name) from the query.
        """
        # for student.grades or just student.* or student whatever
        # # UPDATED TO ACCOUNT FOR DOTS IN THE STATEMENT, CASES LIKE student.grades access!
        word = self.collect_characters(query,
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


    def remove_text(self,query, tokens):
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


    def remove_integer(self,query, tokens):
        """
        Extracts an integer value from the query.
        """
        int_str = self.collect_characters(query, string.digits)
        tokens.append(int_str)
        return query[len(int_str):]


    def remove_number(self,query, tokens):
        """
        Extracts a numeric value (integer or float) from the query.
        """
        query = self.remove_integer(query, tokens)
        if query[0] == ".":
            whole_str = tokens.pop()
            query = query[1:]
            query = self.remove_integer(query, tokens)
            frac_str = tokens.pop()
            float_str = whole_str + "." + frac_str
            tokens.append(float(float_str))
        else:
            int_str = tokens.pop()
            tokens.append(int(int_str))
        return query


    def tokenize(self,query):
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
                query = self.remove_leading_whitespace(query, tokens)
                continue

            if query[0] in (string.ascii_letters + "_"):
                query = self.remove_word(query, tokens)
                continue

            if query[0] in "(),;*":
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
                query = self.remove_text(query, tokens)
                continue

            if query[0] in (string.ascii_letters + "."):
                tokens.append(query[0])
                print(query[0])

            if query[0] in string.digits:
                query = self.remove_number(query, tokens)
                continue

            if len(query) == len(old_query):
                print(query[0])
                raise AssertionError("Query didn't get shorter.")

        return tokens

