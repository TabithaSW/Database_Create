# Custom Database Management System
This project is a lightweight, individual statement or file based database management system that takes in SQL and generates custom databases with tables and views. The database can be generated with ease, modified in the flask application, and exported as a JSON file to the users PC.

**Custom database, tables, rows, columns, and views. Structured to allow properties similar, or mimicking, sqlite3 python module.**

<img src="DBMS_Logo.png" alt="DBMS" width="250" height="250">


- Database handles individual connections to the database for individual transactions, it can interpret BEGIN/COMMIT/ROLLBACK TRANSACTION statements.

- Database handles transactions modes for shared, reserved, and exclusive locks. These modes are DEFERRED, IMMEDIATE, and EXCLUSIVE.

# SQL Statement Support for:
- CREATE TABLE
- DROP TABLE
- INSERT INTO
- SELECT
- SELECT DISTINCT
- SELECT WITH WHERE
- SELECT WITH ORDER BY
- UPDATE
- DELETE
- BEGIN TRANSACTION
- COMMIT TRANSACTION
- ROLLBACK TRANSACTION
- Multi Insert Statements

# In Progress:
- Support for ASCENDING/DESCENDING statements.
- Support for INNER, LEFT, FULL OUTER, and RIGHT JOIN statements.
- Support for different file types being exported using custom file conversion commands.
- Support for NULLs.
- Updating the Flask application for users to create and query their own database online and export it to their computer.

