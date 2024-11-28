

import string
import re
import operator
from operator import itemgetter
import copy
from copy import deepcopy

_ALL_DATABASES = {}


class Connection(object):
    # Connection ID Counter Global Var:
    counter = 0

    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

        # Then each time a new connection object is made,
        # you can assign that object's ID to be equal to whatever the counter's value was
        # you need a way for each connection to know it's identity.
        Connection.counter += 1
        self.id = Connection.counter
        self.copy = None  # For Transactions

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """

        # NEW CREATE INFO: CREATE TABLE IF NOT EXISTS students
        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")

            # IF NOT EXISTS: (Do not raise an exception if it exists, just do nothing.)
            if tokens[0] == "IF":
                pop_and_check(tokens, "IF")
                pop_and_check(tokens, "NOT")
                pop_and_check(tokens, "EXISTS")
                table_name = tokens.pop(0)
                pop_and_check(tokens, "(")
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
                pop_and_check(tokens, "(")
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

        def rollback(tokens):
            print("ROLLBACK FUNC")

        def drop(tokens):
            """
            Drops table if requested, if the table exists.
            """
            # DROP TABLE IF EXISTS students;
            pop_and_check(tokens, "DROP")
            pop_and_check(tokens, "TABLE")
            if tokens[0] == "IF":
                pop_and_check(tokens, "IF")
                pop_and_check(tokens, "EXISTS")
                table_name = tokens.pop(0)
            # No if exists statement.
            else:
                table_name = tokens.pop(0)
            self.database.drop(table_name)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            insert_to_cols = []

            # Checking if insert statement has column names before values:
            if tokens[0] == "(":
                pop_and_check(tokens, "(")
                while True:
                    if tokens[0] == ",":
                        pop_and_check(tokens, ",")
                    elif tokens[0] == ")":
                        # If closing bracket, insert?
                        pop_and_check(tokens, ")")
                        break
                    else:
                        # List of Columns to insert to:
                        insert_to_cols.append(tokens[0])
                        tokens.pop(0)

            # Gathering vals:
            pop_and_check(tokens, "VALUES")
            pop_and_check(tokens, "(")
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
                pop_and_check(tokens, ",")
                pop_and_check(tokens, "(")
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
                pop_and_check(tokens, ",")
                pop_and_check(tokens, "(")
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
                pop_and_check(tokens, ",")
                pop_and_check(tokens, "(")
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
            # UPDATE student SET grade = 4.0 WHERE name = ’James’;
            # ['UPDATE', 'student', 'SET', 'grade', '=', 3.0, 'WHERE', 'piazza', '=', 2, ';']
            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "SET")
            columns = []
            values = []
            while True:
                # Column to update:
                column_name = tokens.pop(0)
                # List of the columns to update.
                columns.append(column_name)
                # Operator, what are we setting the col to?
                pop_and_check(tokens, "=")
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
                        pop_and_check(tokens, "=")  # Operator hopefully is always =
                        where_vals = tokens.pop(0)
                        # Go to the database function for the update:
                        self.database.update(table_name, columns, values, where_column, where_vals)
                        break

        def delete(tokens):
            #  ['DELETE', 'FROM', 'students', 'WHERE', 'id', '>', 4, ';']
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            # IF NO WHERE CLAUSE
            if not tokens:
                self.database.del_all_rows(table_name)
            # IF IT CONTAINS WHERE CLAUSE:
            else:
                # WHERE column_name operator value.
                pop_and_check(tokens, "WHERE")
                # WHAT IS BEING DELETED:
                col_name = tokens.pop(0)
                operator = tokens.pop(0)
                constant = tokens.pop(0)
                # del_where(self,table_name,col_name,operator,constant):
                self.database.del_where(table_name, col_name=col_name, operator=operator, constant=constant)
            return

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            pop_and_check(tokens, "SELECT")
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
                pop_and_check(tokens, "WHERE")
                # Pull column, operator, and constant
                where_col = tokens.pop(0)

                # We're not doing the null checks for project 4! Pull operator >, <, =, !=
                operator = tokens.pop(0)
                # If tokens has ! and then =,
                if tokens[0] == "=":
                    pop_and_check(tokens, "=")
                    operator = "!="
                    # print(operator)
                where_value = tokens.pop(0)
                # Remove order and by:
                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")
                # All order by columns work normally!
                while True:
                    col = tokens.pop(0)
                    # IF STUDENT.PIAZZA type of col
                    if "." in col:
                        parts = col.split(".")
                        order_by_columns.append(parts[1])
                        if tokens[0] == ",":
                            pop_and_check(tokens, ",")
                    else:
                        order_by_columns.append(col)
                    if not tokens:
                        break

                return self.database.select_where(output_columns, table_name, order_by_columns, where_col,
                                                  where_value, operator)

            # If no where clause, business as usual:
            else:

                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")
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
                    pop_and_check(tokens, ",")
                return self.database.select(
                    output_columns, table_name, order_by_columns)

        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE", "DROP", "BEGIN", "COMMIT", "ROLLBACK"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        # Check for transaction statements
        if tokens[0] == "BEGIN":
            pop_and_check(tokens, "BEGIN")
            # Check for transaction mode:
            if tokens[0] == "DEFFERED":
                print("D MODE CHECK")
            elif tokens[0] == "IMMEDIATE":
                print("I MODE CHECK")
            elif tokens[0] == "EXCLUSIVE":
                print("E MODE CHECK")
            else:
                pop_and_check(tokens, "TRANSACTION")
                # Work on a copy of the database,
                # if a transaction is started
                self.copy = deepcopy(self.database)

        # Rollbacks
        elif tokens[0] == "ROLLBACK":
            if self.copy is not None:
                self.copy = None  # Delete the copy/transaction so all events rolled back.
                return

                # Commit the copy we made with the transactions for the connection onbject to the original DB
        elif tokens[0] == "COMMIT":
            if self.copy is not None:
                # Copy transactions to the new DB
                self.database = self.copy
                # toss the old copy
                self.copy = None
                return

        # xecute the query on the database
        else:
            if tokens[0] == "CREATE":
                create_table(tokens)
                return []
            elif tokens[0] == "INSERT":
                insert(tokens)
                return []
            elif tokens[0] == "UPDATE":
                update(tokens)
                return []
            elif tokens[0] == "DELETE":
                delete(tokens)
            elif tokens[0] == "DROP":
                drop(tokens)
            else:  # tokens[0] == "SELECT"
                return select(tokens)
        assert not tokens

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename, timeout=None, isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}
        self.lock = None

    def create_new_table(self, table_name, column_name_type_pairs):
        if table_name in self.tables:
            raise Exception("Table Already In Database")
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def if_exists(self, table_name, column_name_type_pairs):
        """
        For IF NOT EXISTS statements in Create/Drop.
        """
        if table_name in self.tables:
            return
        else:
            self.tables[table_name] = Table(table_name, column_name_type_pairs)

    def del_all_rows(self, table_name):
        self.tables[table_name].rows.clear()
        return

    def drop(self, table_name):
        if self.tables[table_name] is not None:
            del self.tables[table_name]
        return

    def del_where(self, table_name, col_name, operator, constant):
        """
        OPERATORS: >, <, =, !=, IS, IS NOT

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

    def view(self, table_name):
        # print("TABLE NAME CHECK:",self.tables[table_name].name)
        # print("TABLE ROWS CHECK",self.tables[table_name].rows)
        # print("COLUMNS:", self.tables[table_name].column_names)
        if len(self.tables) != 0:
            return self.tables[table_name].rows
        else:
            print("ENTIRE DATABASE:", self.tables)

    def insert_into(self, table_name, row_contents):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents)
        return []

    def select(self, output_columns, table_name, order_by_columns):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns)

    def select_where(self, output_columns, table_name, order_by_columns, where_col, where_value, operator):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_where_rows(output_columns, order_by_columns, where_col, where_value, operator)

    def update(self, table_name, columns, values, where_column=None, where_vals=None):
        assert table_name in self.tables
        table = self.tables[table_name]
        # Check table class for update func.
        return table.update_table(columns, values, where_column, where_vals)


class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents):
        # print("COLUMN NAMES in insert_new_row:",self.column_names)
        # print("ROW CONTENTS in insert_new_row:",row_contents)

        assert len(self.column_names) == len(row_contents)
        row = dict(zip(self.column_names, row_contents))
        self.rows.append(row)

    def update_table(self, columns, values, where_column, where_vals):
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


def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    # for student.grades or just student.* or student whatever
    # # UPDATED TO ACCOUNT FOR DOTS IN THE STATEMENT, CASES LIKE student.grades access!
    word = collect_characters(query,
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


def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        # print("Query:{}".format(query))
        # print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
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
            query = remove_text(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "."):
            tokens.append(query[0])
            print(query[0])

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            print(query[0])
            raise AssertionError("Query didn't get shorter.")

    return tokens

