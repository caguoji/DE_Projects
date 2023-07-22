USE WAREHOUSE compute_wh;
SHOW WAREHOUSES;

--Create schema for staging
CREATE SCHEMA IF NOT EXISTS sakila_stg;

--Define Fact table structure
CREATE OR REPLACE TABLE sakila_stg.bi_fct
(
	rent_date date
	,staff_id varchar(2)
	,rental_id integer
	,cust_id integer
	,film_id integer
	,store_id integer
	,sales integer
);

--Define calendar dimension table structure
CREATE OR REPLACE TABLE sakila_stg.bi_calendar_dim
(
	date date
	,day_of_wk_num integer
	,day_of_wk_desc varchar(10)
	,yr_num integer
	,wk_num integer
	,yr_wk_num integer
	,mnth_num integer
	,yr_mnth_num integer
);


--Define staffing dimension table structure
CREATE OR REPLACE TABLE sakila_stg.bi_staff_dim
(	
	Staff_id integer
	,first_name varchar(20)
	,last_name varchar(20)
	,address_id integer
	,email varchar(50)
	,store_id integer
	,active boolean
	,username varchar(20)
	,password varchar(30)
	,last_update date
);

--Define store dimension table structure
CREATE OR REPLACE TABLE sakila_stg.bi_store_dim
(	
	store_id integer
	,manager_staff_id integer
	,address_id integer
	,last_update date
);



--Define Film dimension table structure
CREATE OR REPLACE TABLE sakila_stg.bi_film_dim
(
	film_id integer
	,title varchar(50)
	,release_year integer
	,language_id integer
	,rental_duration integer
	,rental_rate numeric(4,2)
	,LENGTH integer
	,replacement_cost numeric(5,2)
	,rating varchar(5)
	,special_features varchar(100)
);


--Define rental dimension table structure
CREATE OR REPLACE TABLE sakila_stg.bi_rntl_dim
(
	rental_id integer
	,rental_date date
	,inventory_id integer
	,customer_id integer
	,return_date date
	,staff_id integer
	,last_update date
);


--Define customer dimension table structure
CREATE OR REPLACE TABLE sakila_stg.bi_cust_dim
(
	customer_id integer
	,store_id integer
	,first_name varchar(15)
	,last_name varchar(15)
	,email varchar(50)
	,address_id integer
	,active boolean
	,create_date date
	,last_update date
);