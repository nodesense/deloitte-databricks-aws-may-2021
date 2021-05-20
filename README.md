For RDS , first sql script to create db.



```
create database gk_pg_db;
```

```

drop table if exists proucts;
create table products(product_id int, name text, amount int, brand_id int);


create table brands(brand_id int, name text);


create table orders(order_no int,amount int,cust_id int,country text);



```
