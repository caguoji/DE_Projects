--Populate Fact table
USE SCHEMA wcd_lab.sakila_stg;
TRUNCATE TABLE SAKILA_STG.BI_FCT;

INSERT INTO SAKILA_STG.BI_FCT
(
		rent_date
		,STAFF_ID
		,RENTAL_ID
		,CUST_ID
		,FILM_ID
		,store_ID
		,sales
)
SELECT  nvl(r.rental_date,NULL) AS rent_date
	   ,p.staff_id
	   ,p.rental_id
	   ,p.customer_id
	   ,i.film_id
	   ,s.store_id
	   ,nvl(p.amount,0) AS sales	
FROM sakila.Rental r 
LEFT JOIN sakila.Payment p
ON r.rental_ID = p.rental_ID
LEFT JOIN sakila.Inventory I 
ON r.inventory_id = i.inventory_id
LEFT JOIN sakila.Staff S
ON s.staff_id =  p.staff_id
;

SELECT *
FROM SAKILA_STG.BI_FCT
LIMIT 10;
--Populate calendar table
USE SCHEMA wcd_lab.sakila_stg;
TRUNCATE TABLE SAKILA_STG.BI_CALENDAR_DIM;

INSERT INTO SAKILA_STG.BI_CALENDAR_DIM
(
	date
	,DAY_OF_WK_NUM 
	,DAY_OF_WK_DESC
	,YR_NUM
	,WK_NUM 
	,YR_WK_NUM
	,YR_MNTH_NUM 
	,MNTH_NUM
)
SELECT
	cal_dt AS date
	,DAY_OF_WK_NUM AS DAY_OF_WK_NUM  
	,DAY_OF_WK_DESC AS DAY_OF_WK_DESC 
	,YR_NUM AS YR_NUM
	,WK_NUM AS WK_NUM
	,YR_WK_NUM AS YR_WK_NUM 
	,YR_MNTH_NUM AS YR_MNTH_NUM 
	,MNTH_NUM AS MNTH_NUM 
FROM SAKILA.CALENDAR_DIM
;
	
--Populate customer table
USE SCHEMA wcd_lab.sakila_stg;
TRUNCATE TABLE SAKILA_STG.BI_CUST_DIM;

INSERT INTO SAKILA_STG.BI_CUST_DIM
(
	customer_id
	,STORE_ID
	,FIRST_NAME
	,LAST_NAME
	,EMAIL
	,ADDRESS_ID
	,ACTIVE
	,CREATE_DATE
	,LAST_UPDATE 	
)
SELECT
	CUSTOMER_ID AS CUSTOMER_ID
	,STORE_ID AS STORE_ID  
	,FIRST_NAME AS FIRST_NAME  
	,LAST_NAME AS LAST_NAME 
	,EMAIL AS EMAIL 
	,ADDRESS_ID AS ADDRESS_ID  
	,ACTIVE AS ACTIVE 
	,CREATE_DATE AS CREATE_DATE 
	,LAST_UPDATE AS LAST_UPDATE 
FROM SAKILA.CUSTOMER
;

--Populate staff table

USE SCHEMA wcd_lab.sakila_stg;
TRUNCATE TABLE SAKILA_STG.BI_STAFF_DIM;

INSERT INTO SAKILA_STG.BI_STAFF_DIM
(
	STAFF_ID
	,FIRST_NAME
	,LAST_NAME
	,ADDRESS_ID
	,EMAIL
	,STORE_ID 
	,ACTIVE
	,USERNAME
	,PASSWORD 
	,LAST_UPDATE 	
)
SELECT
	staff_ID AS staff_ID  
	,FIRST_NAME AS FIRST_NAME  
	,LAST_NAME AS LAST_NAME
	,ADDRESS_ID AS ADDRESS_ID
	,EMAIL AS EMAIL 
	,STORE_ID AS STORE_ID
	,ACTIVE AS ACTIVE 
	,USERNAME AS USERNAME
	,PASSWORD AS PASSWORD 
	,LAST_UPDATE AS LAST_UPDATE 
FROM SAKILA.Staff
;

--Populate rental table
USE SCHEMA wcd_lab.sakila_stg;
TRUNCATE TABLE SAKILA_STG.BI_Rntl_DIM;

INSERT INTO SAKILA_STG.BI_Rntl_DIM
(
	rental_ID
	,rental_date
	,inventory_id
	,customer_id
	,return_date
	,staff_id  
	,LAST_UPDATE 	
)
SELECT
	rental_ID AS rental_ID  
	,rental_date AS rental_date  
	,inventory_id AS inventory_id
	,customer_ID AS customer_ID
	,return_date AS return_date 
	,staff_ID AS staff_ID 
	,LAST_UPDATE AS LAST_UPDATE 
FROM SAKILA.Rental
;


--Populate store table
USE SCHEMA wcd_lab.sakila_stg;
TRUNCATE TABLE SAKILA_STG.BI_Store_DIM;

INSERT INTO SAKILA_STG.BI_Store_DIM
(
	Store_ID
	,MANAGER_STAFF_ID 
	,ADDRESS_ID 
	,LAST_UPDATE 	
)
SELECT
	Store_ID AS Store_ID  
	,MANAGER_STAFF_ID AS MANAGER_STAFF_ID  
	,ADDRESS_ID AS ADDRESS_ID
	,LAST_UPDATE AS LAST_UPDATE 
FROM SAKILA.STORE
;

--Populate film table
USE SCHEMA wcd_lab.sakila_stg;
TRUNCATE TABLE SAKILA_STG.BI_FILM_DIM;

INSERT INTO SAKILA_STG.BI_FILM_DIM
(
	FILM_ID
	,TITLE
	,RELEASE_YEAR
	,LANGUAGE_ID
	,RENTAL_DURATION
	,RENTAL_RATE
	,LENGTH
	,REPLACEMENT_COST
	,RATING
	,SPECIAL_FEATURES

)
SELECT
	FILM_ID AS FILM_ID  
	,TITLE AS TITLE  
	,RELEASE_YEAR AS RELEASE_YEAR
	,LANGUAGE_ID AS lANGUAGE_ID
	,RENTAL_DURATION AS RENTAL_DURATION
	,RENTAL_RATE AS RENTAL_RATE
	,LENGTH AS LENGTH
	,REPLACEMENT_COST AS REPLACEMENT_COST
	,RATING AS RATING
	,SPECIAL_FEATURES AS SPECIAL_FEATURES
FROM SAKILA.FILM
;