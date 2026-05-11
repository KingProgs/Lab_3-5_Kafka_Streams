CREATE SCHEMA IF NOT EXISTS db;

USE db;

DROP TABLE IF EXISTS customers;

CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR
);

INSERT INTO customers (customer_id, first_name, last_name, email)
VALUES
    (1, 'Rey', 'Skywalker', 'rey@rebelscum.org'),
    (2, 'Hermione', 'Granger', 'hermione@hogwarts.edu'),
    (3, 'Tony', 'Stark', 'tony@starkindustries.com');

SELECT * FROM customers;
