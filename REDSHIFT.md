
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


https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum-create-role.html

## REdshift spectrum 
```
create external schema rs_gk_external
from data catalog 
database 'rs_gk_external_db' iam_role 'arn:aws:iam::879957431711:role/TRAINING_REDSHIFT_ROLE'
create external database if not exists;

-- check in glue catalog rs_gk_external_db

create external table rs_gk_external.sales(
 id integer,
 qty integer,
 amount float)
row format delimited
 fields terminated by ','
 stored as textfile
 location 's3://gksworkshop/sales/'
 table properties ('numRows'='172000');
 
```
```
 select * from  rs_gk_external.sales
 ```
 
 ```
DROP SCHEMA rs_gk_external;
  ```
