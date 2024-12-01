CREATE TABLE student (name TEXT, grade REAL, class INTEGER);
INSERT INTO student VALUES ('Liam', 4.0, 1), ('Dean', 4.0, 2);
INSERT INTO student VALUES ('Angel', 3.2, 2);
SELECT * FROM student ORDER BY grade, class;
