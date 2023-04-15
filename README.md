# Database_Create
Working on creating a database system that takes in SQL statements and generates custom database. Returns database as excel file with functions allowing conversion to XML or JSON if needed.

- Custom database, tables, rows, columns, and views. Structured to allow properties similar, or mimicking, sqlite3 python module.

- Database runs CREATE, UPDATE, SELECT, INSERT, DROP, DELETE, and more queries.

- Database handles individual connections to the database for individual transactions, it can interpret BEGIN/COMMIT/ROLLBACK TRANSACTION statements.

- Database handles transactions modes for shared, reserved, and exclusive locks. These modes are DEFERRED, IMMEDIATE, and EXCLUSIVE.

- Database handles Aggregate functionality for MAX/MIN, allowes sort by DESCENDING, inserting multiple values, parameterized queries, and DISTINCT values.
