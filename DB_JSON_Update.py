
import os.path
import string
from operator import itemgetter
import copy
import json
import csv


_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        """
        """
        # check for file existing already,
        if os.path.exists(filename):
            #print("FILENAME TEST!",filename)
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

            # Pull content from file on disk and insert values/create table in db, use close to write it.
            with open(filename,"r") as file:
                db_ = json.load(file)
                #print(list(db_.keys())
                name = list(db_.keys())[0]

                # Values in a json dict will be rows/cols here,
                for i in list(db_.values())[0]:
                    cols = list(i.keys())
                    row_vals = list(i.values())
                    column_types = []
                    for b in row_vals:
                        if type(b) == int:
                            column_types.append("INTEGER")
                        elif type(b) == str:
                            column_types.append("TEXT")
                        else:
                            column_types.append("REAL")
                    # Extract db info, break loop.
                    break
                # Name and Type Row tuple pairs:
                name_type = []
                for i in range(len(cols)):
                    name_type.append((cols[i],column_types[i]))

                # Now create a table and close it after test, else run usual route.
                self.database.create_new_table(name,name_type)

                # Execute rest of test,
                for a in list(db_.values())[0]:
                    cols = list(a.keys())
                    vals = list(a.values())
                    sql_stat = "INSERT INTO {} VALUES ({});".format(name,str(vals)[1:-1])
                    self.execute(sql_stat)

        else:
            #print("ELSE FILENAME TEST", filename)
            # if the file isn't generated yet, run commands, then output file with close func at end of test.
            if filename in _ALL_DATABASES:
                self.database = _ALL_DATABASES[filename]
            else:
                self.database = Database(filename)
                _ALL_DATABASES[filename] = self.database
            self.database.filename = filename




    def executemany(self, statement, parameters):
        """
        1: CREATE TABLE students (name TEXT, grade REAL);
        Parameters: [('James', 3.5), ('Yaxin', 2.5)]
        1: INSERT INTO students VALUES (?, ?);

        """
        tokens = tokenize(statement)
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        def param_insert(tokens, parameters):
            # Issue with tuple parenthesis, going to use list brackets:
            param_list = []
            for i in parameters:
                param_list.append(list(i))

            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "VALUES")
            pop_and_check(tokens, "(")

            row_contents = []

            while len(param_list) > 0:
                row_contents.extend(param_list[0])
                param_list = param_list[1:]
                self.database.insert_into(table_name, row_contents)
                row_contents.clear()

        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            # CREATE TABLE students (name TEXT, grade REAL);

            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
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

        # SELECT * FROM students ORDER BY name DESC;
        # SELECT * FROM students ORDER BY name DESC, grade DESC;

        def select(tokens):
            # 1: SELECT * FROM students ORDER BY grade;
            pop_and_check(tokens, "SELECT")
            output_columns = []
            order_by_columns = []
            while True:
                col = tokens.pop(0)
                output_columns.append(col)

                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','

            table_name = tokens.pop(0)
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_col = tokens.pop(0)
            order_by_columns.append(order_by_col)

            # If DESC OR MORE ORDER BY COLS:
            if tokens:
                desc_or_comma = tokens.pop(0)
                if desc_or_comma == "DESC":
                    # If more than one descending:
                    if tokens:
                        pop_and_check(tokens, ",")
                        order_by_col = tokens.pop(0)
                        order_by_columns.append(order_by_col)
                        return self.database.select_desc(output_columns, table_name, order_by_columns)
                    # If only one descending:
                    else:
                        return self.database.select_desc(output_columns, table_name, order_by_columns)

                # If more order by columns:
                elif desc_or_comma == ",":
                    order_by_col2 = tokens.pop(0)
                    order_by_columns.append(order_by_col2)
                    return self.database.select(output_columns, table_name, order_by_columns)

            # If only one order by columns/regular select:
            return self.database.select(output_columns, table_name, order_by_columns)

        if tokens[0] == "CREATE":
            create_table(tokens)

        elif tokens[0] == "INSERT":
            param_insert(tokens, parameters)
            return []
        else:
            select(tokens)

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """

        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            # CREATE TABLE students (name TEXT, grade REAL);

            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
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

        def create_view(tokens):
            """
            Views are read-only named SELECT statements. They act like a table, but if any of the tables
            the underlying SELECT statements it draws from changes, the results returned are changed (on
            subsequent queries)
            """
            order_by_columns = []
            # CREATE VIEW stu_view AS SELECT name FROM students WHERE grade > 3.0 ORDER BY name;
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "VIEW")
            # Pull view name,
            view_name = tokens.pop(0)
            pop_and_check(tokens, "AS")
            pop_and_check(tokens, "SELECT")
            # Pull columns we need for view
            select_col = tokens.pop(0)
            pop_and_check(tokens, "FROM")
            # Extract from original table,
            table_name = tokens.pop(0)
            # Deal with WHERE clause,
            pop_and_check(tokens, "WHERE")
            where_col = tokens.pop(0)
            operator = tokens.pop(0)
            where_val = tokens.pop(0)
            # Deal with ORDER BY clause,
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_col = tokens.pop(0)
            order_by_columns.append(order_col)
            # Create view:
            self.database.create_view(view_name, select_col, table_name, where_col, operator, where_val,
                                      order_by_columns)

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

                # MIN/MAX CHECK!
                # SELECT MAX(piazza) FROM student ORDER BY piazza;
                order_by_columns = []
                if tokens[0] == "(":
                    pop_and_check(tokens, "(")
                    max_col = tokens.pop(0)
                    output_columns.append(max_col)
                    pop_and_check(tokens, ")")
                    pop_and_check(tokens, "FROM")
                    table_name = tokens.pop(0)
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
                    # def select_min_max(self,output_columns,table_name,order_by_columns)
                    # the MIN/MAX will be in output_columns here.
                    return self.database.select_min_max(output_columns, table_name, order_by_columns)

                    # NEW FUNC FOR MAX/MIN

                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','

            table_name = tokens.pop(0)
            # print("SELECT TABLE NAME CHECK:",table_name)

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
                # SELECT * FROM students ORDER BY name DESC, grade DESC;
                order_by_columns = []
                desc_cols = []
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

                    desc_or_comma = tokens.pop(0)
                    if desc_or_comma == "DESC":
                        # If more than one descending:
                        if tokens:
                            pop_and_check(tokens, ",")
                            order_by_col = tokens.pop(0)
                            order_by_columns.append(order_by_col)
                            return self.database.select_desc(output_columns, table_name, order_by_columns)
                        else:
                            # If only one descending:
                            return self.database.select_desc(output_columns, table_name, order_by_columns)

                # print(output_columns,table_name,order_by_columns)
                return self.database.select(output_columns, table_name, order_by_columns)

        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            if tokens[1] == "TABLE":
                create_table(tokens)
            elif tokens[1] == "VIEW":
                create_view(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
        else:  # tokens[0] == "SELECT"
            return select(tokens)
        assert not tokens

    def close(self):
        #print("CLOSE ALL TEST", _ALL_DATABASES)

        # write the database contents into disk by filename, used json here.
        temp_dict = {}
        with open(self.database.filename, "w") as file:
            for tb in self.database.tables:
                temp_tb = self.database.tables[tb]
                # extract rows, use json dump
                temp_dict[tb] = temp_tb.rows
            file.write(json.dumps(temp_dict))
            return


def connect(filename, timeout=None, isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    # self.database.create_view(view_name,select_col,table_name,where_col,operator,where_val,order_by_columns)
    def create_view(self, view_name, select_col, table_name, where_col, operator, where_val, order_by_columns):
        table = self.tables[table_name]
        # Make a copy of the initial table that can be used as a reference to allow changes in values,
        self.tables[view_name] = copy.deepcopy(table)
        table = self.tables[view_name]
        return table.create_tb_view(view_name, select_col, where_col, operator, where_val, order_by_columns)

    def del_all_rows(self, table_name):
        self.tables[table_name].rows.clear()
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
        return self.tables[table_name].rows

    def insert_into(self, table_name, row_contents):
        # print("INSERT_INTO ROW CONTENTS:", row_contents)
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents)
        return []

    def select(self, output_columns, table_name, order_by_columns):
        # print("DATABASE TABLE NAME CHECK:",table_name)
        # print("OUTPUT COLUMNS and ORDER BY:",output_columns,order_by_columns)
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns)

    def select_where(self, output_columns, table_name, order_by_columns, where_col, where_value, operator):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_where_rows(output_columns, order_by_columns, where_col, where_value, operator)

    def select_min_max(self, output_columns, table_name, order_by_columns):
        # SELECT MAX(piazza) FROM student ORDER BY piazza;
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_min_max_rows(output_columns, order_by_columns)

    # self.database.select_desc(output_columns,table_name,order_by_columns)
    def select_desc(self, output_columns, table_name, order_by_columns):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_desc(output_columns, order_by_columns)

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

    def create_tb_view(self, view_name, select_col, where_col, operator, where_val, order_by_columns):
        self.name = view_name
        # CREATE VIEW stu_view AS SELECT name FROM students WHERE grade > 3.0 ORDER BY name;

        # Only put the columns and rows in the view that are specified by select and where.
        new_rows = []
        for dict_ in self.rows:
            for key, value in dict_.items():
                if key == where_col:
                    if operator == ">":
                        if value > where_val:
                            new_rows.append(dict_)
                    elif operator == "<":
                        if value < where_val:
                            new_rows.append(dict_)
                    else:
                        print("ISSUE HERE!")

        # Only the rows we need are now pulled, delete the excess cols/rows

        for i in new_rows:
            for key, value in i.copy().items():
                if key != select_col:
                    del i[key]

        # Order By and replace rows in the view:
        sorted_rows = sorted(new_rows, key=itemgetter(*order_by_columns))
        self.rows = sorted_rows
        self.column_names = (select_col,)

    def update_table(self, columns, values, where_column, where_vals):

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
        # print("SORTED ROWS IN SELECT WHERE CHECK:",sorted_rows, "OUTPUT COLS",expanded_output_columns)
        return generate_tuples(sorted_rows, expanded_output_columns)

    def select_min_max_rows(self, output_columns, order_by_columns):
        # The min or max will be in the first part of output columns!
        # print("SELECT_MIN_MAX_ROWS OUTPUT COLS:",output_columns)
        # print("SELECT_MIN_MAX_ROWS ORDER BY COLS:",order_by_columns)
        # SELECT MAX(grade) FROM student ORDER BY grade;

        max_vals = []
        min_vals = []

        for dict_ in self.rows:
            for key, value in dict_.items():
                if key == output_columns[1]:
                    if value is not None:
                        if output_columns[0] == "MAX":
                            max_vals.append(value)
                        else:
                            min_vals.append(value)

        if len(max_vals) > 0:
            max_ = max(max_vals)
            my_tup = (max_,)
            # print(my_tup)
            yield my_tup
        else:
            min_ = min(min_vals)
            my_tup = (min_,)
            yield my_tup

    # table.select_desc(output_columns,order_by_columns)
    def select_desc(self, output_columns, order_by_columns):

        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def sort_rows(order_by_columns):
            # this was itemgetter(*order_by_columns) but the star is giving error?
            return sorted(self.rows, key=itemgetter(*order_by_columns), reverse=True)

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        expanded_output_columns = expand_star_column(output_columns)
        sorted_rows = sort_rows(order_by_columns)
        #print("DESCENDING ROWS:", sorted_rows)
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
            # this was itemgetter(*order_by_columns) but the star is giving error?
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

        if query[0] == "?":
            tokens.append(query[0])
            query = query[1:]
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

