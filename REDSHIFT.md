
To Create DB,

```
CREATE DATABASE newdb;
```

Use new connect to db..



```
CREATE SCHEMA gk;

CREATE TABLE gk.invoices (inv_id int, amount int);

INSERT INTO gk.invoices VALUES (1, 100);

SELECT * from gk.invoices;


select count(*) from sales; -- public 
```
