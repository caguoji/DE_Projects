## Creating a Data Model for A Data Warehouse

### Steps
1. Use DBeaver to load original data: You will use this script to load the dataset into snowflake data warehouse.
2. Analyze the Business Requirements and translate the requirements into the technical requirements (rough formulas and sql queries).
3. Consider the grain of the data model based on your analysis of the above Requirements. Assess the atomic row of a fact table.
4. Decide what dimension tables you will use in the data model, the columns and their source tables.
5. Define your fact table, its columns and their source tables.
6. Use a different schema to create the dimension and fact tables in the Snowflake data warehouse. 
7. Write ETL script to transform data from the production tables to the target tables in the data model. 

### Output
1. ETL script to transform the data from the original tables to the data model you create.


### Issues
1. Logging into Snowflake test website
2. Use of DBeaver

### Business Requirements
1. List the total revenue of each store everyday.
2. List the total revenue of totally everyday.
3. List the top store according to their weekly revenue every week.
4. List top sales clerk who have the most sales each day/week/month.
5. Queries need to be able to easily answer the following using data model:
    - Which film is the top film each week/month in each store/totally?
    - Who are our top 10 customers each month/year?
    - Is there any store the sales is in a decline trend (within the recent 4 weeks the avg sales of each week is declining)
