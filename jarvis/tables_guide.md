# Practice tables — quick reference

Same 7 tables in **BigQuery** (`shield.<table>`), **Oracle** and **Spark**.
Keep this open on the side: right-click this tab → **Split Right** (or press `Cmd+\`).

## How they connect

```
departments ──< employees           employees.dept_id    → departments.dept_id
                employees.manager_id → employees.emp_id   (manager is also an employee)

customers ──< orders ──< order_items >── products
```
`──<` = one to many

---

## departments (7 rows)
| column | type | note |
|---|---|---|
| dept_id | int | PK |
| dept_name | string | Engineering, Sales, Marketing, Finance, HR, Operations, **Legal (no employees)** |

## employees (30 rows)
| column | type | note |
|---|---|---|
| emp_id | int | PK |
| name | string | |
| dept_id | int | → departments. **1 NULL** |
| manager_id | int | → employees.emp_id. **2 NULL** (top bosses) |
| salary | decimal(10,2) | **ties** in the same dept |
| hire_date | date | |

## customers (40 rows)
| column | type | note |
|---|---|---|
| customer_id | int | PK. **33–40 never ordered** |
| name | string | |
| city | string | **1 NULL** |
| signup_date | date | |

## orders (108 rows)
| column | type | note |
|---|---|---|
| order_id | int | **no PK: 4 exact duplicate rows** |
| customer_id | int | → customers |
| order_date | date | |
| amount | decimal(10,2) | **6 NULL** |
| status | string | e.g. completed |

## products (20 rows)
| column | type | note |
|---|---|---|
| product_id | int | PK. **2 never sold** |
| product_name | string | |
| category | string | |
| price | decimal(10,2) | |

## order_items (201 rows)
| column | type | note |
|---|---|---|
| order_id | int | → orders |
| product_id | int | → products |
| quantity | int | |

## user_logins (240 rows)
| column | type | note |
|---|---|---|
| user_id | int | |
| login_date | date | streaks, gaps, **same-day duplicates** |

---

## Type names in each tool
| generic | BigQuery | Oracle | Spark |
|---|---|---|---|
| int | INT64 | NUMBER(10) | INT |
| string | STRING | VARCHAR2(n) | STRING |
| decimal(10,2) | NUMERIC | NUMBER(10,2) | DECIMAL(10,2) |
| date | DATE | DATE (also has time!) | DATE |

## See the schema with SQL
| Tool | Command |
|---|---|
| BigQuery | `SELECT column_name, data_type FROM shield.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'employees'` |
| Oracle | `SELECT column_name, data_type FROM user_tab_columns WHERE table_name = 'EMPLOYEES'` (name in CAPITALS) |
| Spark | `t["employees"].printSchema()` or `%%sql DESCRIBE employees` |
