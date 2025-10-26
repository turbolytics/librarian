CREATE TABLE users
(
    id    SERIAL PRIMARY KEY,
    name  TEXT,
    email TEXT
);
INSERT INTO users (name, email)
VALUES ('Alice', 'alice@example.com'),
       ('Bob', 'bob@example.com'),
       ('Charlie', 'charlie@example.com'),
       ('David', 'david@example.com'),
       ('Eve', 'eve@example.com');