

# Develop a simple DBMS in python: Handle CREATE, INSERT, SELECT statements
# Needs to match the SQLite input and output identically (for the subset of SQL used in the projects)

import re  # This is a Python 3 module, I checked the docs.
import string
import operator
import copy

class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        pass

    def execute(self,statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        # Create instance of DataBase Class:
        db = Database()

        # Only SELECT statements should return rows/table.
        # Tokens that should return an empty list:
        c = "CREATE"
        ins = "INSERT"

        # First we tokenize,
        tokens = tokenize(statement)
        # If it's a create statement,
        if c in tokens:
            db.create_table(tokens)
            return []
        # If it's an insert statement:
        elif ins in tokens:
            db.insert_vals(tokens)
            return []
        # If it is a select statement:
        else:
            # Table ID is right after FROM statement.
            # We have to pull here, so we can have the table inputted into the get_rows func.
            for i in range(len(tokens)):
                if tokens[i] == "FROM":
                    table_name = tokens[i + 1]
            table = db.get_table(table_name)
            # USE GET ROWS FO THE REST!
            return db.get_rows(tokens, table)

    # The "close" method will not be used for this project, so it does not need to be implemented for this project.
    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)



# ALL DATABASE CLASS CONTENT BELOW:

all_database = {}


class Database(object):
    def __init__(self):
        self.database = {}

    def create_table(self, tokens):
        # Temp storage:
        name_cols = []
        actual_table = {}
        d_type = []

        # I want to pull all the lowercase text from the tokens,
        # These will be the table and column names of the table. (for CREATE TABLE)
        for i in range(len(tokens)):
            if tokens[i].islower():
                # We also want the data type of the columns:
                d_type.append(tokens[i + 1])
                # Append column names:
                name_cols.append(tokens[i])
        # d_type will have a bracket because of the title, so we pop it off.
        d_type.pop(0)

        # Extract table name:
        title = name_cols[0]
        # Add columns to table:
        for i in name_cols[1:]:
            actual_table[i] = []

        # Insert table into database:
        # self.database[title] = [actual_table]

        # Using global variable instead to prevent overwriting.
        all_database[title] = [actual_table]

        return

    def insert_vals(self, tokens):
        # Temp storage:
        db = Database()
        my_list = []
        # In an INSERT INTO statement, the third token is the table name.
        table_name = tokens[2]
        table = db.get_table(table_name)
        keys = list(table.keys())

        # Extract values, I will turn them into a tuple in my return.
        # The INSERT INTO statement values begin after the table name and bracket, so we start at [5:]
        for i in tokens[5:]:
            if i != ")":
                if i != "," and i != ";":
                    my_list.append((i))

        # Inserting the values
        # The columns I believe are the same amoutn as the values inserted, so lets try this:
        for i in range(len(my_list)):
            table[keys[i]].append(my_list[i])

        return
        # return self.database[table_name]

    def get_table(self, table_id):
        # Since I used nested dictionaries, my table will first pull as a list.
        # table_list = self.database[table_id]

        # We are going to use a global variable instead to prevent overwriting.
        table_list = all_database[table_id]

        # Index at 0 to pull entire table.
        return table_list[0]

    def get_rows(self, tokens, table):
        # Temp storage
        cols = []
        col_to_sort = []
        all_rows = []

        # What are we selecting?

        for i in range(len(tokens)):
            # After SELECT are the column names we need.
            if tokens[i] == "SELECT":
                select_cols = tokens[i + 1:]

        # We need to slice, everything after the column names we dont want in the list.
        remove_from = select_cols.index("FROM")
        select_cols = select_cols[:remove_from]
        # Remove commas/special characters
        for i in select_cols:
            if i != ",":
                cols.append(i)

        # What are we ordering it by?
        for i in range(len(tokens)):
            if tokens[i] == "BY":
                order = tokens[i + 1:]
        for i in order:
            if i != ";" and i != "," and i != ")":
                col_to_sort.append(i)

        # Get all rows:
        keys = []
        for key, value in table.items():
            keys.append(key)
            all_rows.append(table[key])

        # Transpose list
        t_list = [tuple(i) for i in zip(*all_rows)]
        #print("Transposed List:", t_list)

        # Sorting transposed list by specified key:
        index = []
        for i in keys:
            # Keys appended in order of transposed rows, so:
            if i in col_to_sort:
                index.append(keys.index(i))

        for i in index:
            t_list = sorted(t_list, key=operator.itemgetter(i))

        # Return tuple
        return t_list


#  ALL TOKENIZE CONTENT BELOW:

def float_int_check(query, tokens):
    nums = []
    x = re.findall(r'-?\d+\.\d+', query)
    # If the number is a float,
    if x:
        if query[0] in x[0]:
            tokens.append(float(x[0]))
            query = query[len(x[0]):]
        else:
            # If the list wasn't empty but number is integer:
            nums = collect_characters(query, string.digits)
            tokens.append(int(nums))
            query = query[len(nums):]

    # If the number is an integer:
    if len(x) == 0:
        nums = collect_characters(query, string.digits)
        tokens.append(int(nums))
        query = query[len(nums):]
    return query


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
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def tokenize(query):
    tokens = []
    while query:
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] == "*":
            tokens.append(query[0])
            query = query[1:]
            continue

        # INTEGERS AND FLOATS
        if query[0] in string.digits or query[0] == "-":
            query = float_int_check(query, tokens)
            continue
        # NEGATIVE SIGNS
        if query[0] == "-":
            tokens.append(query[0])
            query = query[1:]
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens
