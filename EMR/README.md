## Create an end to end pipeline, using Snowflake, Airflow, Databricks and PySpark and Amazon Web Services to support
## a dashboard build in PowerBI

### Steps
1. Review the data architecture diagram and develop scripts to support its implementation.
2. Leverage Snowflake as 'Production database', write cron job to pull data from Snowflake
and dump in AWS S3 buckets.
3. Connect AWS to Databricks to write and test PySpark scripts needed for ETL jobs as
per requirements below.
4. Create AWS EC2 and EMR instance to run PySpark, use Airflow to orchestrate jobflow.
5. Use lambda function to start EC2 automatically and trigger Airflow job.
6. Save output files in AWS S3 bucket in parquet file formt.
7. Connect AWS S3 Output bucket to AWS Athena using AWS Glue.
8. Connect AWS Athena to PowerBI using JDBC connector.
9. Create Dashboard in PowerBI to visualize the data.

### Output
1. PowerBI Dashboard summarizing metrics from output dataframe

### Issues
1. Testing EMR connector in Airflow

### Business Requirements
1. Output dataframe should allow business to answer the following questions, the table should
be grouped by each week, each store, each product to calculate the following metrics:

    - total sales quantity of a product : Sum(sales_qty)
    - total sales amount of a product : Sum(sales_amt)
    - average sales Price: Sum(sales_amt)/Sum(sales_qty)
    - stock level by then end of the week : stock_on_hand_qty by the end of the week (only the stock level at the end 
     day of the week)
    - store on Order level by then end of the week: ordered_stock_qty by the end of the week (only the ordered stock 
     quantity at the end day of the week)
    - total cost of the week: Sum(cost_amt)
    - the percentage of Store In-Stock: (how many times of out_of_stock in a week) / days of a week (7 days)
    - total Low Stock Impact: sum (out_of+stock_flg + Low_Stock_flg)
    - potential Low Stock Impact: if Low_Stock_Flg =TRUE then SUM(sales_amt - stock_on_hand_amt)
    - no Stock Impact: if out_of_stock_flg=true, then sum(sales_amt)
    - low Stock Instances: Calculate how many times of Low_Stock_Flg* in a week
    - no Stock Instances: Calculate then how many times of out_of_Stock_Flg in a week
    - how many weeks the on hand stock can supply: (stock_on_hand_qty at the end of the week) / sum(sales_qty)
    *(Low Stock_flg = if today's stock_on_hand_qty<sales_qty , then low_stock_flg=1, otherwise =0) *


![Architecture](https://github.com/caguoji/DE_Projects/assets/11283694/0ea384bb-e0f6-41ce-989c-a69a9ef552e7)
