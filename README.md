# Database_Create
Working on creating a database system that takes in SQL statements and generates custom database.
-Custom database, tables, rows, allows properties similar or mimicking sqlite3 python module.

Database runs CREATE, UPDATE, SELECT, INSERT, DROP, DELETE, and more queries.
Database handles individual connections to the database for individual transactions, it can interpret BEGIN/COMMIT/ROLLBACK TRANSACTION statements.
Database handles transactions modes for shared, reserved, and exlusive locks. These modes are DEFERRED, IMMEDIATE, and EXCLUSIVE.
