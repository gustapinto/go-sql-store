# go-sql-store

A golang SQL-Like data store

## Supported Operations

### DDL

- `CREATE DATABASE <database name>;`
- `DROP DATABASE <database name>;`
- `CREATE TABLE <database name>.<table name> ( <column name> <column type> [column constraint] );`
  - Supported Types
    - `TEXT`
    - `FLOAT`
    - `INTEGER`
    - `TIMESTAMP`
  - Supported Constraints
    - `PRIMARY KEY`
    - `UNIQUE`
- `DROP TABLE <database name>.<table name>;`

### DML

- `INSERT INTO <database name>.<table name> (<column name>) VALUES (<column value>);`
- `UPDATE <database name>.<table name> SET <column name> = <column value> [WHERE <another column name> = <another column value>]`
- `DELETE FROM <database name>.<table name> [WHERE <column name> = <column value>]`

### DQL

- `SELECT [<column nane>|*] FROM <database name>.<table name> [WHERE <column name> = <column value>]`
