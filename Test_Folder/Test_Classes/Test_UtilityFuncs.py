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

            print("Tokens: ", tokens) # SHould increase until query is empty.
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

        return tokens

Utility_Functions.tokenize("1: SELECT names.name, grades.grade FROM names LEFT OUTER JOIN grades ON names.id = grades.id ORDER BY names.id;")