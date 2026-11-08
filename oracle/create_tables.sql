-- 01_create_tables.sql - the same 7 practice tables, in Oracle types.
-- INT -> NUMBER(10), STRING -> VARCHAR2, DECIMAL(10,2) -> NUMBER(10,2), DATE -> DATE
-- Note: an Oracle DATE also stores a time (here always 00:00:00).

CREATE TABLE departments (
    dept_id    NUMBER(10)    PRIMARY KEY,
    dept_name  VARCHAR2(50)  NOT NULL
);

CREATE TABLE employees (
    emp_id      NUMBER(10)    PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    dept_id     NUMBER(10),
    manager_id  NUMBER(10),
    salary      NUMBER(10,2),
    hire_date   DATE
);

CREATE TABLE customers (
    customer_id  NUMBER(10)    PRIMARY KEY,
    name         VARCHAR2(100) NOT NULL,
    city         VARCHAR2(50),
    signup_date  DATE
);

-- No primary key on purpose: this table has duplicate rows.
CREATE TABLE orders (
    order_id     NUMBER(10),
    customer_id  NUMBER(10),
    order_date   DATE,
    amount       NUMBER(10,2),
    status       VARCHAR2(20)
);

CREATE TABLE products (
    product_id    NUMBER(10)    PRIMARY KEY,
    product_name  VARCHAR2(100) NOT NULL,
    category      VARCHAR2(50),
    price         NUMBER(10,2)
);

CREATE TABLE order_items (
    order_id    NUMBER(10),
    product_id  NUMBER(10),
    quantity    NUMBER(10)
);

CREATE TABLE user_logins (
    user_id     NUMBER(10),
    login_date  DATE
);
