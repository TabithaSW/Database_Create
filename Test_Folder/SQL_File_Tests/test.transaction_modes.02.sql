1: CREATE TABLE students (name TEXT);
1: BEGIN TRANSACTION;
1: UPDATE students SET name = 'James';
2: BEGIN EXCLUSIVE TRANSACTION;
2: COMMIT TRANSACTION;
1: COMMIT TRANSACTION;
