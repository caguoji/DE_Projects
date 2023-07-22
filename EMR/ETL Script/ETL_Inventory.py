from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

aws_bucket_name = "s3://wcd-midterm-output-chichi"

date = sys.argv[1]
sales_data = sys.argv[2]
cal_data = sys.argv[3]
inv_data = sys.argv[4]
prod_data = sys.argv[5]
store_data = sys.argv[6]

spark = SparkSession.builder \
        .master("yarn") \
        .appName("WCD_Midterm") \
        .getOrCreate()

    

# sales_data = "sales_"+date+".csv"
# inv_data = "inventory_"+date+".csv"
# cldr_data = "calendar_"+date+".csv"
# prod_data = "product_"+date+".csv"
# store_data = "store_"+date+".csv"

#read sales data
sales = spark.read \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("delimiter",',') \
    .csv(f"{sales_data}")


#read inv data
inv = spark.read \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("delimiter",',') \
    .csv(f"{inv_data}")

#read calendar data
cldr = spark.read \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("delimiter",',') \
    .csv(f"{cal_data}")

#read store data
store = spark.read \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("delimiter",',') \
    .csv(f"{store_data}")

#read product data
prod = spark.read \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("delimiter",',') \
    .csv(f"{prod_data}")


#join sales data with calendar data
sales_jn_wk = sales.join(cldr["CAL_DT","WK_NUM","YR_NUM"], sales["TRANS_DT"] == cldr["CAL_DT"], how="inner")
sales_jn_wk.show()



#calc wkly sales amount, wkly sales qty and wkly sales cost ques #1 and #2 and #6
sales_by_wk = sales_jn_wk.groupBy('YR_NUM','WK_NUM','STORE_KEY','PROD_KEY') \
    .agg(sum('SALES_AMT') \
    .alias('WKLY_SALES_AMT'),sum('SALES_QTY') \
    .alias('WKLY_SALES_QTY'),sum('SALES_COST')
    .alias('WKLY_SALES_COST')) \
    .orderBy(sales_jn_wk['WK_NUM'] \
    .desc())



#calc wkly avg sales price #3
sales_by_wk = sales_by_wk.withColumn('WKLY_AVG_SALES_PRICE',round(sales_by_wk['WKLY_SALES_AMT']/sales_by_wk['WKLY_SALES_QTY'],0))

#sales by product and store join with calendar data
sales_by_store = sales.groupby('TRANS_DT','STORE_KEY','PROD_KEY') \
    .agg(sum('SALES_QTY').alias('SALES_BY_STORE')) \
    .join(cldr['CAL_DT','YR_NUM','WK_NUM'],(sales['TRANS_DT']== cldr['CAL_DT']),how="inner") \
    .drop(sales['TRANS_DT']) \
    .drop(cldr['CAL_DT'])



#calc inv on hand and on order at wk's end #4 and 5 
end_of_wk_inv = inv['CAL_DT','STORE_KEY','PROD_KEY','INVENTORY_ON_HAND_QTY','INVENTORY_ON_ORDER_QTY'] \
    .join(cldr.filter(cldr['DAY_OF_WK_DESC'] == 'Friday')['CAL_DT','DAY_OF_WK_DESC','WK_NUM','YR_NUM'], inv['CAL_DT']==cldr['CAL_DT'],how='inner') \
    .drop(inv['CAL_DT']) \
    .drop(cldr['CAL_DT']) \
    .drop(cldr['DAY_OF_WK_DESC'])

#Tested    
end_of_wk_inv=end_of_wk_inv.withColumnRenamed("INVENTORY_ON_HAND_QTY","END_OF_WK_INV_ON_HAND_QTY")
end_of_wk_inv=end_of_wk_inv.withColumnRenamed("INVENTORY_ON_ORDER_QTY","END_OF_WK_INV_ON_ORDER_QTY")

#Tested
#join sales by store and inv at wk's end #13
sales_and_inv_by_wk = sales_by_store.join(end_of_wk_inv,(sales_by_store['YR_NUM'] == end_of_wk_inv['YR_NUM']) & \
    (sales_by_store['WK_NUM']== end_of_wk_inv['WK_NUM']) & \
    (sales_by_store['STORE_KEY'] == end_of_wk_inv['STORE_KEY']) & \
    (sales_by_store['PROD_KEY']== end_of_wk_inv['PROD_KEY']), how="inner") \
    .drop(sales_by_store['WK_NUM'])\
    .drop(sales_by_store['YR_NUM']) \
    .drop(sales_by_store['STORE_KEY']) \
    .drop(sales_by_store['PROD_KEY'])

#Tested
sales_and_inv_by_wk=sales_and_inv_by_wk.withColumn('WKS_INV_ON_HAND',round(sales_and_inv_by_wk['END_OF_WK_INV_ON_HAND_QTY']/sales_and_inv_by_wk['SALES_BY_STORE'],3))

#Tested
#calc pct inv out of stock #7
pct_inv_out_of_stock = inv['CAL_DT','STORE_KEY','PROD_KEY','OUT_OF_STOCK_FLG'] \
    .join(cldr['CAL_DT','WK_NUM','YR_NUM'], inv['CAL_DT']==cldr['CAL_DT'],how='inner') \
    .groupBy('YR_NUM','WK_NUM','STORE_KEY','PROD_KEY') \
    .agg(round((sum('OUT_OF_STOCK_FLG')/7),3) \
    .alias('WKLY_PCT_OUT_OF_STOCK')) \
    .orderBy(inv['STORE_KEY'] \
    .desc(),inv['PROD_KEY'] \
    .desc(),cldr['YR_NUM'] \
    .desc())


#Tested
#calc low stock flag
inv_and_sales = inv['CAL_DT','STORE_KEY','PROD_KEY','INVENTORY_ON_HAND_QTY'] \
    .join(sales['TRANS_DT','STORE_KEY','PROD_KEY','SALES_QTY'],(inv['CAL_DT'] == sales['TRANS_DT']) & (inv['STORE_KEY'] == sales['STORE_KEY']) & (inv['PROD_KEY'] == sales['PROD_KEY']), how="inner") \
    .join(cldr['CAL_DT','WK_NUM','YR_NUM'], (inv['CAL_DT']==cldr['CAL_DT']) & (sales['TRANS_DT']==cldr['CAL_DT']),how="inner") \
    .drop(inv['STORE_KEY']) \
    .drop(inv['PROD_KEY']) \
    .drop(inv['CAL_DT'])


inv_and_sales = inv_and_sales.withColumn('LOW_STOCK_FLG', when(inv_and_sales['INVENTORY_ON_HAND_QTY']<inv_and_sales['SALES_QTY'],1) \
    .otherwise(0))

#check
inv_and_sales.filter(inv_and_sales['LOW_STOCK_FLG']==1).show()


#calc low stock impact #8; 
inv_low_stock = inv['CAL_DT','STORE_KEY','PROD_KEY','OUT_OF_STOCK_FLG','INVENTORY_ON_HAND_QTY'] \
    .join(inv_and_sales['TRANS_DT','WK_NUM','YR_NUM','STORE_KEY','PROD_KEY','LOW_STOCK_FLG','SALES_QTY'],(inv['CAL_DT']== inv_and_sales['TRANS_DT']) & \
    (inv['PROD_KEY'] == inv_and_sales['PROD_KEY']) & (inv['STORE_KEY'] == inv_and_sales['STORE_KEY']), how="inner").drop(inv_and_sales['STORE_KEY']) \
    .drop(inv_and_sales['PROD_KEY'])

inv_low_stock = inv_low_stock.withColumn('TOT_LOW_STOCK_IMPACT',inv_low_stock['LOW_STOCK_FLG']+inv_low_stock['OUT_OF_STOCK_FLG'])



 # calc potential low stock impact #9 
inv_low_stock = inv_low_stock.withColumn('PTNL_LOW_STOCK_IMPACT', when(inv_low_stock['LOW_STOCK_FLG']==1,inv_low_stock['SALES_QTY']- inv_low_stock['INVENTORY_ON_HAND_QTY']) \
    .otherwise(0))
 

#10 No stock impact
inv_low_stock = inv_low_stock.withColumn('NO_STOCK_IMPACT', when(inv_low_stock['OUT_OF_STOCK_FLG']==1,inv_low_stock['SALES_QTY']) \
    .otherwise(0))
 

#calc wkly number of times low stock flag #11 

#inv_low_stock.toPandas().head()
wkly_low_stock_flg = inv_low_stock.groupBy('YR_NUM','WK_NUM','STORE_KEY','PROD_KEY') \
    .agg(sum(inv_low_stock['LOW_STOCK_FLG']) \
    .alias('WKLY_LOW_STOCK_FLG_CNT'))


#calc number of times out of stock flag this week #12 
wkly_no_stock_flg = inv_low_stock.groupBy('YR_NUM','WK_NUM','STORE_KEY','PROD_KEY') \
    .agg(sum(inv_low_stock['OUT_OF_STOCK_FLG']) \
    .alias('WKLY_OUT_OF_STOCK_FLG_CNT'))



#Tested
#roll up daily flags in inv_low_stock df

inv_low_stock_by_wk = inv_low_stock.groupBy('YR_NUM','WK_NUM','STORE_KEY','PROD_KEY') \
    .agg(sum('OUT_OF_STOCK_FLG') \
    .alias('WKLY_OUT_OF_STOCK_FLG'),sum('LOW_STOCK_FLG') \
    .alias('WKLY_LOW_STOCK_FLG'),sum('TOT_LOW_STOCK_IMPACT') \
    .alias('WKLY_TOT_LOW_STOCK_IMPACT'),round(sum('PTNL_LOW_STOCK_IMPACT'),3 ) \
    .alias('WKLY_PTNL_LOW_STOCK_IMPACT'),sum('NO_STOCK_IMPACT') \
    .alias('WKLY_NO_STOCK_IMPACT'))


#join all weekly datasets together

#1st join
_1st_join = end_of_wk_inv.join(sales_by_wk, [(end_of_wk_inv['YR_NUM'] == sales_by_wk['YR_NUM']) \
, (end_of_wk_inv['WK_NUM']==sales_by_wk['WK_NUM']) , (end_of_wk_inv['PROD_KEY']==sales_by_wk['PROD_KEY']) ,(end_of_wk_inv['STORE_KEY']==sales_by_wk['STORE_KEY'])],how="inner").drop(end_of_wk_inv['YR_NUM']).drop(end_of_wk_inv['WK_NUM']).drop(end_of_wk_inv['PROD_KEY']).drop(end_of_wk_inv['STORE_KEY'])


#2nd join
_2nd_join=_1st_join.join(sales_and_inv_by_wk, [(sales_and_inv_by_wk['YR_NUM'] == _1st_join['YR_NUM']) \
, (sales_and_inv_by_wk['WK_NUM']==_1st_join['WK_NUM']) , (sales_and_inv_by_wk['PROD_KEY']==_1st_join['PROD_KEY']) ,(sales_and_inv_by_wk['STORE_KEY']==_1st_join['STORE_KEY'])],how="inner").drop(_1st_join['PROD_KEY']).drop(_1st_join['STORE_KEY']).drop(sales_and_inv_by_wk['YR_NUM']).drop(sales_and_inv_by_wk['WK_NUM']).drop(sales_and_inv_by_wk['END_OF_WK_INV_ON_ORDER_QTY']).drop(sales_and_inv_by_wk['END_OF_WK_INV_ON_HAND_QTY'])


#3rd join
_3rd_join=_2nd_join.join(inv_low_stock_by_wk,[(inv_low_stock_by_wk['YR_NUM'] == _2nd_join['YR_NUM']) \
, (inv_low_stock_by_wk['WK_NUM']==_2nd_join['WK_NUM']) , (inv_low_stock_by_wk['PROD_KEY']==_2nd_join['PROD_KEY']) ,(inv_low_stock_by_wk['STORE_KEY']==_2nd_join['STORE_KEY'])],how="inner").drop(_2nd_join['PROD_KEY']).drop(_2nd_join['STORE_KEY']).drop(_2nd_join['YR_NUM']).drop(_2nd_join['WK_NUM'])


df_out = _3rd_join


#Tested
df_out.repartition(5).write \
    .option("header",True) \
    .option("inferSchema",True) \
    .mode("overwrite") \
    .parquet(f"{aws_bucket_name}/Wkly_Inv_Data/Year/Store/FACT_INV_{date}.parquet")

#Tested
cldr.write \
    .option("header",True) \
    .option("inferSchema",True) \
    .mode("overwrite") \
    .csv(f"{aws_bucket_name}/Wkly_Cal_Data/DIM_CALENDAR_{date}.csv")


prod.write \
    .option("header",True) \
    .option("inferSchema",True) \
    .mode("overwrite") \
    .csv(f"{aws_bucket_name}/Wkly_Prod_Data/DIM_PROD_{date}.csv")


store.write \
    .option("header",True) \
    .option("inferSchema",True) \
    .mode("overwrite") \
    .csv(f"{aws_bucket_name}/Wkly_Store_Data/DIM_STORE_{date}.csv")