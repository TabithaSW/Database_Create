CREATE TABLE student (name TEXT, grade REAL, piazza INTEGER);
CREATE TABLE tests (version INTEGER, total REAL);
INSERT INTO student VALUES ('James', 3.5, 1);
INSERT INTO tests VALUES (1, 100.0);
INSERT INTO student VALUES ('Yaxin', 4.0, 2);
INSERT INTO tests VALUES (2, 60.0);
INSERT INTO student VALUES ('Li', 3.2, 2);
SELECT * FROM student ORDER BY piazza, grade;
SELECT piazza, name FROM student ORDER BY grade;
SELECT * FROM tests ORDER BY version;